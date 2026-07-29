//! `ShellBuilder`：链式配置 + `spawn()`。

use std::future::Future;
use std::sync::Arc;

use anyhow::{ensure, Result};
use tokio::sync::Notify;

use crate::shell::backend::LaunchConfig;
#[cfg(feature = "pty")]
use crate::shell::backend::PtyOptions;
use crate::shell::buffer::OutputBuffer;
use crate::shell::callbacks::{
    AsyncCloseCallback, AsyncErrorCallback, AsyncExitCallback, AsyncOutputCallback,
    AsyncPreSendCallback, CallbackHub, CallbackMode, Callbacks, PreSendHook,
};
use crate::shell::Shell;

const DEFAULT_BUFFER_CAPACITY: usize = 4 * 1024 * 1024;

pub struct ShellBuilder {
    shell_path: String,
    pre_send: Option<AsyncPreSendCallback>,
    callbacks: Callbacks,
    close_notify: Arc<Notify>,
    buffer_capacity: Option<usize>,

    #[cfg(feature = "pty")]
    pty_opts: Option<PtyOptions>,
}

impl ShellBuilder {
    pub fn new(shell: impl Into<String>) -> Self {
        Self {
            shell_path: shell.into(),
            pre_send: None,
            callbacks: Callbacks::default(), // mode = Raw
            close_notify: Arc::new(Notify::new()),
            buffer_capacity: None,

            #[cfg(feature = "pty")]
            pty_opts: None,
        }
    }

    // ── PTY 配置 ──────────────────────────────────────────────────────────

    #[cfg(feature = "pty")]
    fn pty_opts_mut(&mut self) -> &mut PtyOptions {
        self.pty_opts.get_or_insert_with(PtyOptions::default)
    }

    /// 启用后使用 PTY（伪终端）模式而不是管道模式。
    ///
    /// PTY 模式下：
    /// - stdout / stderr 会被**合并**为一路输出（`on_error` / `stderr` 永远为空）；
    /// - 子进程会看到自己连接在一个真实终端上（支持全屏程序、prompt 着色、
    ///   job control）；
    /// - 终端默认开启回显（echo），发送的命令本身也会出现在输出里；
    /// - 支持 `resize()`、`output_snapshot()`（渲染后的屏幕快照）。
    #[cfg(feature = "pty")]
    pub fn enable_pty(mut self) -> Self {
        self.pty_opts_mut();
        self
    }

    /// 设置 PTY 初始窗口尺寸（默认 80x24）。仅在 `enable_pty()` 后生效。
    #[cfg(feature = "pty")]
    pub fn pty_size(mut self, cols: u16, rows: u16) -> Self {
        let opts = self.pty_opts_mut();
        opts.cols = cols;
        opts.rows = rows;
        self
    }

    /// 设置 vt100 屏幕快照的回滚缓冲行数（默认 2000）。
    #[cfg(feature = "pty")]
    pub fn scrollback(mut self, lines: usize) -> Self {
        self.pty_opts_mut().scrollback = lines;
        self
    }

    /// 关闭 vt100 屏幕追踪，节省 CPU/内存。关闭后 `output_snapshot()` /
    /// `screen_clone()` 将返回错误。
    #[cfg(feature = "pty")]
    pub fn disable_snapshot(mut self) -> Self {
        self.pty_opts_mut().track_screen = false;
        self
    }

    // ── 缓冲区 ────────────────────────────────────────────────────────────

    /// 启用输出缓冲，使用默认容量（4 MB）。
    pub fn enable_buffer(mut self) -> Self {
        self.buffer_capacity = Some(DEFAULT_BUFFER_CAPACITY);
        self
    }

    /// 启用输出缓冲，指定容量上限（字节）；超出后丢弃最旧数据。
    pub fn enable_buffer_with_capacity(mut self, max_bytes: usize) -> Self {
        self.buffer_capacity = Some(max_bytes);
        self
    }

    // ── 回调模式 ──────────────────────────────────────────────────────────

    /// 切换为行回调模式：`on_output`/`on_error` 以完整行为单位触发。
    pub fn line_callback(mut self) -> Self {
        self.callbacks.mode = CallbackMode::Line;
        self
    }

    /// 切换为原始块回调模式（默认）：读到多少立即回调，延迟最低。
    pub fn raw_callback(mut self) -> Self {
        self.callbacks.mode = CallbackMode::Raw;
        self
    }

    // ── 回调注册 ──────────────────────────────────────────────────────────

    pub fn on_send<F, Fut>(mut self, mut f: F) -> Self
    where
        F: FnMut(String) -> Fut + Send + 'static,
        Fut: Future<Output = Option<String>> + Send + 'static,
    {
        let cb: AsyncPreSendCallback = Box::new(move |s| Box::pin(f(s)));
        self.pre_send = Some(cb);
        self
    }

    pub fn on_output<F, Fut>(mut self, mut f: F) -> Self
    where
        F: FnMut(String) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cb: AsyncOutputCallback = Box::new(move |s| Box::pin(f(s)));
        self.callbacks.on_output = Some(cb);
        self
    }

    /// 注册 stderr 回调。**注意**：PTY 模式下 stdout/stderr 合并，此回调不会被触发。
    pub fn on_error<F, Fut>(mut self, mut f: F) -> Self
    where
        F: FnMut(String) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cb: AsyncErrorCallback = Box::new(move |s| Box::pin(f(s)));
        self.callbacks.on_error = Some(cb);
        self
    }

    pub fn on_exit<F, Fut>(mut self, mut f: F) -> Self
    where
        F: FnMut(Option<i32>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cb: AsyncExitCallback = Box::new(move |c| Box::pin(f(c)));
        self.callbacks.on_exit = Some(cb);
        self
    }

    pub fn on_close<F, Fut>(mut self, mut f: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cb: AsyncCloseCallback = Box::new(move || Box::pin(f()));
        self.callbacks.on_close = Some(cb);
        self
    }

    // ── spawn ─────────────────────────────────────────────────────────────

    pub async fn spawn(self) -> Result<Shell> {
        let shell_path = self.shell_path.trim().to_string();
        ensure!(!shell_path.is_empty(), "shell path cannot be empty");

        let pre_send = PreSendHook::new(self.pre_send);
        let callbacks = CallbackHub::new(self.callbacks);
        let output_buffer = self
            .buffer_capacity
            .map(|cap| Arc::new(OutputBuffer::new(cap)));

        #[cfg(feature = "pty")]
        let is_pty = self.pty_opts.is_some();
        #[cfg(not(feature = "pty"))]
        let is_pty = false;

        // PTY 模式下 stdout/stderr 合并到 output_buffer，不需要独立的 error_buffer。
        let error_buffer = if is_pty {
            None
        } else {
            self.buffer_capacity
                .map(|cap| Arc::new(OutputBuffer::new(cap)))
        };

        let cfg = LaunchConfig {
            shell_path,
            callbacks,
            output_buffer,
            error_buffer,
            #[cfg(feature = "pty")]
            pty_opts: self.pty_opts,
        };

        Shell::spawn_new(cfg, pre_send, self.close_notify).await
    }
}