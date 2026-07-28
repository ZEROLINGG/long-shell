use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot, Mutex, Notify, OnceCell};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::util::{normalize_shell_name, StreamDecoder};

// ─── 全局单例 ────────────────────────────────────────────────────────────────

#[cfg(unix)]
static BASH: OnceCell<Result<Arc<Mutex<Shell>>>> = OnceCell::const_new();
#[cfg(windows)]
static POWERSHELL: OnceCell<Result<Arc<Mutex<Shell>>>> = OnceCell::const_new();

#[cfg(unix)]
pub async fn bash() -> Result<Arc<Mutex<Shell>>> {
    let result = BASH
        .get_or_init(|| async {
            Shell::new("bash")
                .enable_buffer()
                .spawn()
                .await
                .map(|s| Arc::new(Mutex::new(s)))
        })
        .await;
    match result {
        Ok(s) => Ok(s.clone()),
        Err(e) => Err(anyhow!("{e}")),
    }
}

#[cfg(windows)]
pub async fn powershell() -> Result<Arc<Mutex<Shell>>> {
    let result = POWERSHELL
        .get_or_init(|| async {
            Shell::new("powershell")
                .enable_buffer()
                .spawn()
                .await
                .map(|s| Arc::new(Mutex::new(s)))
        })
        .await;
    match result {
        Ok(s) => Ok(s.clone()),
        Err(e) => Err(anyhow!("{e}")),
    }
}

// ─── 平台相关 ─────────────────────────────────────────────────────────────────

#[cfg(windows)]
use std::os::windows::process::CommandExt;
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x08000000;

// ─── 常量 ─────────────────────────────────────────────────────────────────────

/// 每次 `read()` 的块大小。
const READ_CHUNK_SIZE: usize = 8192;

/// 行模式：输出空闲多久后，即便没有换行符也强制 flush 已缓存内容。
/// 决定了交互式 prompt（如 `>>> `）这类不带换行输出的最大展示延迟。
const LINE_MODE_FLUSH_IDLE: Duration = Duration::from_millis(80);

/// 行模式：单行数据超过该长度时强制切块 flush，防止超长行导致内存无限增长。
const LINE_MODE_FORCE_FLUSH: usize = 64 * 1024;

/// `OutputBuffer` 默认容量上限（字节）。超过后丢弃最旧的数据。
const DEFAULT_BUFFER_CAPACITY: usize = 4 * 1024 * 1024;

// ─── 类型别名 ─────────────────────────────────────────────────────────────────

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

type AsyncPreSendCallback =
Box<dyn FnMut(String) -> BoxFuture<'static, Option<String>> + Send + 'static>;
type AsyncOutputCallback =
Box<dyn FnMut(String) -> BoxFuture<'static, ()> + Send + 'static>;
type AsyncErrorCallback =
Box<dyn FnMut(String) -> BoxFuture<'static, ()> + Send + 'static>;
type AsyncExitCallback =
Box<dyn FnMut(Option<i32>) -> BoxFuture<'static, ()> + Send + 'static>;
type AsyncCloseCallback =
Box<dyn FnMut() -> BoxFuture<'static, ()> + Send + 'static>;

// ─── 回调模式 ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackMode {
    /// 有数据就立即回调，粒度为原始读取块，延迟最低。
    Raw,
    /// 以行为单位回调；无换行时依赖空闲超时强制 flush。
    Line,
}

impl Default for CallbackMode {
    fn default() -> Self {
        Self::Raw
    }
}

// ─── 内部消息 ─────────────────────────────────────────────────────────────────

enum StdinMsg {
    Data(String),
    Close,
    Eof
}

// ─── 回调集合 ─────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Callbacks {
    on_output: Option<AsyncOutputCallback>,
    on_error:  Option<AsyncErrorCallback>,
    on_exit:   Option<AsyncExitCallback>,
    on_close:  Option<AsyncCloseCallback>,
    /// 回调触发粒度，由 `ShellBuilder` 设置，spawn 后不可变。
    mode:      CallbackMode,
}

// ─── 输出缓冲区 ───────────────────────────────────────────────────────────────

struct OutputBufferInner {
    chunks:    VecDeque<Arc<str>>,
    total_len: usize,
}

/// 有界输出缓冲区。
pub struct OutputBuffer {
    inner:               Mutex<OutputBufferInner>,
    pub notify:          Notify,
    max_bytes:           usize,
    pub truncated_bytes: AtomicUsize,
}

impl OutputBuffer {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(OutputBufferInner {
                chunks:    VecDeque::new(),
                total_len: 0,
            }),
            notify:          Notify::new(),
            max_bytes,
            truncated_bytes: AtomicUsize::new(0),
        }
    }

    /// 推入一个数据块；超过容量时自动丢弃最旧的块。
    pub async fn push(&self, chunk: Arc<str>) {
        if chunk.is_empty() {
            return;
        }
        let mut inner = self.inner.lock().await;
        inner.total_len += chunk.len();
        inner.chunks.push_back(chunk);

        while inner.total_len > self.max_bytes {
            match inner.chunks.pop_front() {
                Some(front) => {
                    inner.total_len -= front.len();
                    self.truncated_bytes
                        .fetch_add(front.len(), Ordering::Relaxed);
                }
                None => break,
            }
        }
        drop(inner);
        self.notify.notify_one();
    }

    /// 取出全部数据并清空缓冲区。
    pub async fn take(&self) -> String {
        let mut inner = self.inner.lock().await;
        let mut s = String::with_capacity(inner.total_len);
        for chunk in inner.chunks.drain(..) {
            s.push_str(&chunk);
        }
        inner.total_len = 0;
        s
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.chunks.is_empty()
    }
}

// ─── Builder ──────────────────────────────────────────────────────────────────

pub struct ShellBuilder {
    shell_path:      String,
    pre_send:        Option<AsyncPreSendCallback>,
    callbacks:       Callbacks,
    close_notify:    Arc<Notify>,
    buffer_capacity: Option<usize>,
}

impl ShellBuilder {
    pub fn new(shell: impl Into<String>) -> Self {
        Self {
            shell_path:      shell.into(),
            pre_send:        None,
            callbacks:       Callbacks::default(),   // mode = Raw
            close_notify:    Arc::new(Notify::new()),
            buffer_capacity: None,
        }
    }

    // ── 缓冲区 ────────────────────────────────────────────────────────────────

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

    // ── 回调模式 ──────────────────────────────────────────────────────────────

    /// 切换为行回调模式：`on_output`/`on_error` 以完整行为单位触发。
    ///
    /// 若长时间没有换行符（如交互式 REPL 提示符），会在空闲超时
    /// （[`LINE_MODE_FLUSH_IDLE`]，默认 80 ms）后把已有内容强制输出，
    /// 不会永久阻塞。
    pub fn line_callback(mut self) -> Self {
        self.callbacks.mode = CallbackMode::Line;
        self
    }

    /// 切换为原始块回调模式（默认）：读到多少立即回调，延迟最低。
    pub fn raw_callback(mut self) -> Self {
        self.callbacks.mode = CallbackMode::Raw;
        self
    }

    // ── 回调注册 ──────────────────────────────────────────────────────────────

    pub fn on_send<F, Fut>(mut self, mut f: F) -> Self
    where
        F:   FnMut(String) -> Fut + Send + 'static,
        Fut: Future<Output = Option<String>> + Send + 'static,
    {
        self.pre_send = Some(Box::new(move |s| Box::pin(f(s))));
        self
    }

    pub fn on_output<F, Fut>(mut self, mut f: F) -> Self
    where
        F:   FnMut(String) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_output = Some(Box::new(move |s| Box::pin(f(s))));
        self
    }

    pub fn on_error<F, Fut>(mut self, mut f: F) -> Self
    where
        F:   FnMut(String) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_error = Some(Box::new(move |s| Box::pin(f(s))));
        self
    }

    pub fn on_exit<F, Fut>(mut self, mut f: F) -> Self
    where
        F:   FnMut(Option<i32>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_exit = Some(Box::new(move |c| Box::pin(f(c))));
        self
    }

    pub fn on_close<F, Fut>(mut self, mut f: F) -> Self
    where
        F:   FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.callbacks.on_close = Some(Box::new(move || Box::pin(f())));
        self
    }

    // ── spawn ─────────────────────────────────────────────────────────────────

    pub async fn spawn(self) -> Result<Shell> {
        let shell_path = self.shell_path.trim().to_string();
        ensure!(!shell_path.is_empty(), "shell path cannot be empty");

        let pre_send  = Arc::new(Mutex::new(self.pre_send));
        let callbacks = Arc::new(Mutex::new(self.callbacks));

        let output_buffer =
            self.buffer_capacity.map(|cap| Arc::new(OutputBuffer::new(cap)));
        let error_buffer =
            self.buffer_capacity.map(|cap| Arc::new(OutputBuffer::new(cap)));

        let (tx_stdin, drop_tx, join) = Shell::spawn_process(
            &shell_path,
            callbacks.clone(),
            output_buffer.clone(),
            error_buffer.clone(),
        )
            .await?;

        Ok(Shell {
            shell_path,
            tx_stdin,
            drop_tx: Some(drop_tx),
            pre_send,
            callbacks,
            join: Some(join),
            droped: false,
            close_notify: self.close_notify,
            output_buffer,
            error_buffer,
        })
    }
}

// ─── Shell ────────────────────────────────────────────────────────────────────

pub struct Shell {
    pub shell_path:    String,
    tx_stdin:      mpsc::Sender<StdinMsg>,
    drop_tx:       Option<oneshot::Sender<()>>,
    pre_send:      Arc<Mutex<Option<AsyncPreSendCallback>>>,
    callbacks:     Arc<Mutex<Callbacks>>,
    join:          Option<JoinHandle<()>>,
    droped:        bool,
    close_notify:  Arc<Notify>,
    output_buffer: Option<Arc<OutputBuffer>>,
    error_buffer:  Option<Arc<OutputBuffer>>,
}

impl Shell {
    pub fn new(shell: impl Into<String>) -> ShellBuilder {
        ShellBuilder::new(shell)
    }

    // ── 输出读取 ──────────────────────────────────────────────────────────────

    /// 等待直到输出空闲超过 `idle_time`（默认 200 ms），然后返回并清空缓冲。
    pub async fn output(&mut self, idle_time: Option<Duration>) -> String {
        if self.output_buffer.is_none() {
            return String::new();
        }
        let timeout = idle_time.unwrap_or(Duration::from_millis(200));
        let ob = self.output_buffer.as_ref().unwrap();

        loop {
            tokio::select! {
                _ = self.close_notify.notified() => break,
                res = tokio::time::timeout(timeout, ob.notify.notified()) => {
                    match res {
                        Ok(_)  => continue,
                        Err(_) => break,
                    }
                }
            }
        }
        ob.take().await
    }

    /// 同 `output`，针对 stderr。
    pub async fn output_error(&mut self, idle_time: Option<Duration>) -> String {
        if self.error_buffer.is_none() {
            return String::new();
        }
        let timeout = idle_time.unwrap_or(Duration::from_millis(200));
        let ob = self.error_buffer.as_ref().unwrap();

        loop {
            tokio::select! {
                _ = self.close_notify.notified() => break,
                res = tokio::time::timeout(timeout, ob.notify.notified()) => {
                    match res {
                        Ok(_)  => continue,
                        Err(_) => break,
                    }
                }
            }
        }
        ob.take().await
    }

    /// 返回 stdout 缓冲区因超出容量而丢弃的累计字节数。
    pub fn output_truncated_bytes(&self) -> usize {
        self.output_buffer
            .as_ref()
            .map(|ob| ob.truncated_bytes.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// 返回 stderr 缓冲区因超出容量而丢弃的累计字节数。
    pub fn error_truncated_bytes(&self) -> usize {
        self.error_buffer
            .as_ref()
            .map(|ob| ob.truncated_bytes.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    // ── 发送 ──────────────────────────────────────────────────────────────────

    pub async fn send(&mut self, cmd: &str) -> Result<()> {
        ensure!(!self.droped, "shell is closed");

        // 识别控制字符简写：^C / ^D / ^R
        if cmd.len() < 5 {
            let trimmed = cmd.trim();
            if trimmed.chars().count() == 2 {
                let mut chars = trimmed.chars();
                if let (Some('^'), Some(c)) = (chars.next(), chars.next()) {
                    let upper = c.to_ascii_uppercase();
                    if "CDR".contains(upper) {
                        return self.send_control_char(upper).await;
                    }
                }
            }
        }

        if let Some(s) = self.preprocess_send(cmd.to_string()).await {
            self.tx_stdin
                .send(StdinMsg::Data(s))
                .await
                .map_err(|_| anyhow!("send failed: stdin channel closed"))?;
        }
        Ok(())
    }

    pub async fn send_line(&mut self, cmd: &str) -> Result<()> {
        self.send(&format!("{cmd}\n")).await
    }
    async fn send_eof(&mut self) -> Result<()> {
        self.tx_stdin
            .send(StdinMsg::Eof)
            .await
            .map_err(|_| anyhow!("send EOF failed"))
    }

    pub async fn send_control_char(&mut self, ctrl: char) -> Result<()> {
        match ctrl {
            'R' => self.reset().await, // 特别占用^R为shell重置语义
            'D' => self.send_eof().await,
            _ => Ok(()),
        }
    }

    // ── 生命周期 ──────────────────────────────────────────────────────────────

    pub async fn join(&mut self) -> Result<()> {
        if !self.droped {
            self.close_notify.notified().await;
        }
        if let Some(handle) = self.join.take() {
            let _ = handle.await;
        }
        Ok(())
    }

    pub async fn join_exit(&mut self) -> Result<()> {
        if let Some(handle) = self.join.take() {
            handle
                .await
                .map_err(|e| anyhow!("join_exit failed: {e}"))?;
        }
        Ok(())
    }

    pub async fn reset(&mut self) -> Result<()> {
        ensure!(!self.droped, "shell is closed");
        self.exit().await?;
        if let Some(handle) = self.join.take() {
            let _ = handle.await;
        }

        let (tx_stdin, drop_tx, join) = Self::spawn_process(
            &self.shell_path,
            self.callbacks.clone(),
            self.output_buffer.clone(),
            self.error_buffer.clone(),
        )
            .await?;
        self.tx_stdin = tx_stdin;
        self.drop_tx  = Some(drop_tx);
        self.join     = Some(join);
        self.droped   = false;
        Ok(())
    }

    /// 关闭当前会话（可通过 `reset()` 恢复）。
    pub async fn exit(&mut self) -> Result<()> {
        ensure!(!self.droped, "shell is closed");

        let exit_cmd = Self::exit_command(&self.shell_path);
        let _ = self.tx_stdin.send(StdinMsg::Data(exit_cmd)).await;
        let _ = self.tx_stdin.send(StdinMsg::Close).await;

        const EXIT_TIMEOUT: Duration = Duration::from_secs(10);
        match tokio::time::timeout(EXIT_TIMEOUT, self.join_exit()).await {
            Ok(result) => {

                if !self.droped {
                    self.droped = true;
                    let _ = self.drop_tx.take();
                    self.close_notify.notify_waiters();
                }
                result
            }
            Err(_) => {

                self.close()
            }
        }
    }

    /// 立即关闭 Shell 实例（不可恢复，同步）。
    pub fn close(&mut self) -> Result<()> {
        if self.droped {
            return Ok(());
        }
        self.droped = true;
        let _ = self.drop_tx.take();
        let _ = self.tx_stdin.try_send(StdinMsg::Close);
        self.close_notify.notify_waiters();
        Ok(())
    }

    // ── 私有辅助 ──────────────────────────────────────────────────────────────

    async fn preprocess_send(&self, raw: String) -> Option<String> {
        let mut guard = self.pre_send.lock().await;
        if let Some(f) = guard.as_mut() {
            f(raw).await
        } else {
            Some(raw)
        }
    }

    async fn spawn_process(
        shell: &str,
        callbacks: Arc<Mutex<Callbacks>>,
        output_buffer: Option<Arc<OutputBuffer>>,
        error_buffer:  Option<Arc<OutputBuffer>>,
    ) -> Result<(mpsc::Sender<StdinMsg>, oneshot::Sender<()>, JoinHandle<()>)> {
        let shell_name = normalize_shell_name(shell)?;
        let mut cmd    = build_command(shell, &shell_name)?;

        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        #[cfg(windows)]
        cmd.creation_flags(CREATE_NO_WINDOW);

        let mut child = cmd.spawn()?;

        let mut stdin = child.stdin.take().unwrap();
        let stdout    = child.stdout.take().unwrap();
        let stderr    = child.stderr.take().unwrap();

        let (tx_stdin, mut rx_stdin) = mpsc::channel::<StdinMsg>(32);
        let (drop_tx, drop_rx)       = oneshot::channel::<()>();

        if let Some(cmd) = init_command(&shell_name) {
            stdin.write_all(cmd.as_bytes()).await?;
            stdin.flush().await?;
        }

        // 读取 mode：spawn 后 mode 不再变化，直接拷贝出来避免锁开销
        let mode = callbacks.lock().await.mode;

        let cb_stdout   = callbacks.clone();
        let ob_stdout   = output_buffer.clone();
        let stdout_task = tokio::spawn(read_stream(stdout, ob_stdout, cb_stdout, false, mode));

        let cb_stderr   = callbacks.clone();
        let ob_stderr   = error_buffer.clone();
        let stderr_task = tokio::spawn(read_stream(stderr, ob_stderr, cb_stderr, true, mode));

        let stdin_task = tokio::spawn(async move {
            let mut stdin_opt = Some(stdin);
            while let Some(msg) = rx_stdin.recv().await {
                match msg {
                    StdinMsg::Close => break,
                    StdinMsg::Eof => {
                        if let Some(mut s) = stdin_opt.take() {
                            let _ = s.flush().await;
                            drop(s);
                        }
                    }
                    StdinMsg::Data(data) => {
                        if let Some(mut stdin) = stdin_opt.take() {
                            const STDIN_TIMEOUT: Duration = Duration::from_secs(30);

                            let r = tokio::time::timeout(
                                STDIN_TIMEOUT,
                                stdin.write_all(data.as_bytes()),
                            )
                                .await;
                            if r.ok().and_then(|r| r.ok()).is_none() {
                                break; // write failed — genuinely broken pipe, ok to drop
                            }

                            let r = tokio::time::timeout(STDIN_TIMEOUT, stdin.flush()).await;
                            if r.ok().and_then(|r| r.ok()).is_none() {
                                break;
                            }

                            stdin_opt = Some(stdin);
                        }
                    }
                }
            }
            drop(stdin_opt);
        });

        let cb_main = callbacks.clone();
        let join = tokio::spawn(async move {
            let _code: Option<i32> = tokio::select! {
                status = child.wait() => {
                    let code = status.ok().and_then(|s| s.code());
                    let fut_opt = {
                        let mut cb = cb_main.lock().await;
                        cb.on_exit.as_mut().map(|f| f(code))
                    };
                    if let Some(fut) = fut_opt { fut.await; }
                    code
                }
                _ = drop_rx => {
                    if let Err(e) = child.kill().await {
                        eprintln!("kill failed (process may have already exited): {e}");
                    }
                    child.wait().await.ok().and_then(|s| s.code())
                }
            };

            let _ = tokio::join!(stdout_task, stderr_task, stdin_task);

            let fut_opt = {
                let mut cb = cb_main.lock().await;
                cb.on_close.as_mut().map(|f| f())
            };
            if let Some(fut) = fut_opt { fut.await; }
        });

        Ok((tx_stdin, drop_tx, join))
    }

    fn exit_command(shell_path: &str) -> String {
        let name = normalize_shell_name(shell_path).unwrap_or_default();
        match name.as_str() {
            "python"              => "quit()\n".into(),
            "node"                => ".exit\n".into(),
            _                     => "exit\n".into(),
        }
    }
}

impl Drop for Shell {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

// ─── 流读取（入口，按 mode 分派）─────────────────────────────────────────────

/// 统一入口：根据 `mode` 分派到对应的读取实现。
async fn read_stream<R>(
    reader:    R,
    ob:        Option<Arc<OutputBuffer>>,
    callbacks: Arc<Mutex<Callbacks>>,
    is_stderr: bool,
    mode:      CallbackMode,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    match mode {
        CallbackMode::Raw  => read_stream_raw(reader, ob, callbacks, is_stderr).await,
        CallbackMode::Line => read_stream_line(reader, ob, callbacks, is_stderr).await,
    }
}

// ─── Raw 模式：读到多少立即回调 ───────────────────────────────────────────────

/// 读到多少字节就立即解码并推送，延迟最低。
/// 回调收到的字符串是原始块（可能跨多行，也可能是半行）。
async fn read_stream_raw<R>(
    mut reader: R,
    ob:         Option<Arc<OutputBuffer>>,
    callbacks:  Arc<Mutex<Callbacks>>,
    is_stderr:  bool,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    let mut decoder = StreamDecoder::new();
    let mut raw     = vec![0u8; READ_CHUNK_SIZE];
    let mut decoded = String::new();

    loop {
        match reader.read(&mut raw).await {
            Ok(0) => {
                // EOF：flush 解码器残留
                decoder.finish(&mut decoded);
                if !decoded.is_empty() {
                    emit(std::mem::take(&mut decoded), &ob, &callbacks, is_stderr).await;
                }
                break;
            }
            Ok(n) => {
                decoder.feed(&raw[..n], &mut decoded);
                if !decoded.is_empty() {
                    emit(std::mem::take(&mut decoded), &ob, &callbacks, is_stderr).await;
                }
            }
            Err(_) => break,
        }
    }
}

// ─── Line 模式：以行为单位回调 ────────────────────────────────────────────────

/// 以 `\n` 为界切分完整行后回调。
/// 长时间无换行符时依赖空闲定时器强制 flush，避免永久阻塞。
/// 完整行回调时已去除末尾 `\r\n`；强制 flush 的半行保留原样。
async fn read_stream_line<R>(
    mut reader: R,
    ob:         Option<Arc<OutputBuffer>>,
    callbacks:  Arc<Mutex<Callbacks>>,
    is_stderr:  bool,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    let mut decoder = StreamDecoder::new();
    let mut raw     = vec![0u8; READ_CHUNK_SIZE];
    let mut decoded = String::new();
    let mut carry   = String::new();

    let mut ticker = tokio::time::interval(LINE_MODE_FLUSH_IDLE);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // 消耗第一个立即触发的 tick

    loop {
        tokio::select! {
            biased;

            res = reader.read(&mut raw) => {
                match res {
                    Ok(0) => {
                        // EOF
                        decoder.finish(&mut decoded);
                        carry.push_str(&decoded);
                        decoded.clear();

                        drain_lines(&mut carry, &ob, &callbacks, is_stderr).await;
                        if !carry.is_empty() {
                            emit(std::mem::take(&mut carry), &ob, &callbacks, is_stderr).await;
                        }
                        break;
                    }
                    Ok(n) => {
                        decoder.feed(&raw[..n], &mut decoded);
                        carry.push_str(&decoded);
                        decoded.clear();
                        drain_lines(&mut carry, &ob, &callbacks, is_stderr).await;
                    }
                    Err(_) => break,
                }
            }

            _ = ticker.tick() => {
                // 空闲超时：强制把半行 flush 出去
                if !carry.is_empty() {
                    emit(std::mem::take(&mut carry), &ob, &callbacks, is_stderr).await;
                }
            }
        }
    }
}

/// 从 `carry` 中持续提取完整行并推送，直到没有 `\n` 或触发强制切块。
async fn drain_lines(
    carry:     &mut String,
    ob:        &Option<Arc<OutputBuffer>>,
    callbacks: &Arc<Mutex<Callbacks>>,
    is_stderr: bool,
) {
    loop {
        if let Some(pos) = carry.find('\n') {
            // 含 \n 的原始行存入 buffer（保留原始格式）；
            // 回调传去掉末尾空白的版本。
            let raw_line: String = carry.drain(..=pos).collect();
            let trimmed = raw_line
                .trim_end_matches(|c| c == '\r' || c == '\n')
                .to_string();

            if let Some(ob) = ob {
                ob.push(Arc::from(raw_line.as_str())).await;
            }
            invoke_callback(trimmed, callbacks, is_stderr).await;
        } else if carry.len() >= LINE_MODE_FORCE_FLUSH {
            // 单行过长，强制切块防止内存无限增长
            let chunk = std::mem::take(carry);
            if let Some(ob) = ob {
                ob.push(Arc::from(chunk.as_str())).await;
            }
            invoke_callback(chunk, callbacks, is_stderr).await;
        } else {
            break;
        }
    }
}

// ─── 公共推送原语 ─────────────────────────────────────────────────────────────

/// Raw 模式和 Line 模式强制 flush 时使用的统一推送原语：
/// chunk 原样存入 buffer 并触发回调（不做任何裁剪）。
async fn emit(
    chunk:     String,
    ob:        &Option<Arc<OutputBuffer>>,
    callbacks: &Arc<Mutex<Callbacks>>,
    is_stderr: bool,
) {
    if chunk.is_empty() {
        return;
    }
    if let Some(ob) = ob {
        ob.push(Arc::from(chunk.as_str())).await;
    }
    invoke_callback(chunk, callbacks, is_stderr).await;
}

/// 触发 `on_output` 或 `on_error` 回调。
async fn invoke_callback(
    text:      String,
    callbacks: &Arc<Mutex<Callbacks>>,
    is_stderr: bool,
) {
    let fut_opt = {
        let mut cb = callbacks.lock().await;
        if is_stderr {
            cb.on_error.as_mut().map(|f| f(text))
        } else {
            cb.on_output.as_mut().map(|f| f(text))
        }
    };
    if let Some(fut) = fut_opt {
        fut.await;
    }
}

// ─── 进程构建辅助 ─────────────────────────────────────────────────────────────

fn build_command(shell: &str, shell_name: &str) -> Result<Command> {
    let mut cmd = Command::new(shell);
    // #[cfg(unix)]
    // cmd.process_group(0);
    match shell_name {
        "bash"                    => { cmd.args(["--norc", "--noprofile", "-s"]); }
        "zsh"                     => { cmd.args(["-f", "-s"]); }
        "sh"                      => { cmd.args(["-s"]); }
        "fish"                    => { cmd.args(["--no-config", "-i"]); }
        "cmd"                     => { cmd.args(["/Q", "/K", "prompt $G"]); }
        "powershell" | "pwsh"     => {
            cmd.args([
                "-ExecutionPolicy", "Bypass",
                "-NoExit",
                "-NoProfile",
            ]);
        }
        "python"                  => { cmd.args(["-u", "-i"]); }
        "node"                    => { cmd.arg("-i"); }
        _                         => anyhow::bail!("unsupported shell: {shell_name}"),
    }
    Ok(cmd)
}

fn init_command(shell_name: &str) -> Option<String> {
    match shell_name {
        "cmd" => Some("chcp 65001 >nul 2>&1\r\n".into()),
        "powershell" | "pwsh" => Some(
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; \
             [Console]::InputEncoding  = [System.Text.Encoding]::UTF8; \
             $OutputEncoding           = [System.Text.Encoding]::UTF8\n"
                .into(),
        ),
        _ => None,
    }
}

// ─── 测试 ─────────────────────────────────────────────────────────────────────


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, timeout, Duration};

    // ── 1. OutputBuffer 单元测试 ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_output_buffer_basic() {
        let buffer = OutputBuffer::new(1024);
        assert!(buffer.is_empty().await);

        buffer.push(Arc::from("hello ")).await;
        buffer.push(Arc::from("world")).await;

        assert!(!buffer.is_empty().await);
        assert_eq!(buffer.take().await, "hello world");
        assert!(buffer.is_empty().await);
    }

    #[tokio::test]
    async fn test_output_buffer_capacity_truncation() {
        // 容量上限设置为 10 字节
        let buffer = OutputBuffer::new(10);

        // 推入 6 字节 ("hello ")
        buffer.push(Arc::from("hello ")).await;
        assert_eq!(buffer.truncated_bytes.load(Ordering::Relaxed), 0);

        // 推入 6 字节 ("world!")，总共 12 字节 > 10 字节上限
        // 最旧的 chunk ("hello ") 会被抛弃
        buffer.push(Arc::from("world!")).await;

        assert_eq!(buffer.truncated_bytes.load(Ordering::Relaxed), 6);
        assert_eq!(buffer.take().await, "world!");
    }

    #[tokio::test]
    async fn test_output_buffer_notify() {
        let buffer = Arc::new(OutputBuffer::new(1024));
        let buffer_clone = buffer.clone();

        let handle = tokio::spawn(async move {
            buffer_clone.notify.notified().await;
            buffer_clone.take().await
        });

        sleep(Duration::from_millis(50)).await;
        buffer.push(Arc::from("data received")).await;

        let result = timeout(Duration::from_secs(1), handle)
            .await
            .expect("notify timeout")
            .expect("task failed");

        assert_eq!(result, "data received");
    }

    // ── 2. Shell 基础执行与 Output 读取测试 ──────────────────────────────────

    #[tokio::test]
    #[cfg(unix)]
    async fn test_shell_basic_output_unix() {
        let mut shell = Shell::new("sh")
            .enable_buffer()
            .on_output(|s| async move { print!("{}", s) })
            .spawn()
            .await
            .expect("Failed to spawn sh");

        shell.send_line("echo 'hello_tokio'").await.unwrap();

        let out = shell.output(Some(Duration::from_millis(300))).await;
        assert!(out.contains("hello_tokio"));


        shell.exit().await.unwrap();
    }

    #[tokio::test]
    #[cfg(windows)]
    async fn test_shell_basic_output_windows() {
        let mut shell = Shell::new("cmd")
            .enable_buffer()
            .spawn()
            .await
            .expect("Failed to spawn cmd");

        shell.send_line("echo hello_tokio").await.unwrap();

        let out = shell.output(Some(Duration::from_millis(300))).await;
        assert!(out.contains("hello_tokio"));

        shell.exit().await.unwrap();
    }

    // ── 3. 回调机制测试 (Line 模式与 Raw 模式) ─────────────────────────────────

    #[tokio::test]
    #[cfg(unix)]
    async fn test_shell_line_callback() {
        let (tx, mut rx) = mpsc::channel::<String>(10);

        let mut shell = Shell::new("sh")
            .line_callback()
            .on_output(move |line| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(line).await;
                }
            })
            .spawn()
            .await
            .unwrap();

        shell.send_line("echo line1").await.unwrap();
        shell.send_line("echo line2").await.unwrap();

        let line1 = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let line2 = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(line1, "line1");
        assert_eq!(line2, "line2");


        shell.exit().await.unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_shell_stderr_callback() {
        let (tx, mut rx) = mpsc::channel::<String>(10);

        let mut shell = Shell::new("sh")
            .raw_callback()
            .on_error(move |err_chunk| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(err_chunk).await;
                }
            })
            .spawn()
            .await
            .unwrap();

        // 写入 stderr
        shell.send_line("echo 'error_msg' >&2").await.unwrap();

        let err_out = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(err_out.contains("error_msg"));

        shell.exit().await.unwrap();
    }

    // ── 4. 命令拦截过滤测试 (on_send / pre_send) ────────────────────────────

    #[tokio::test]
    #[cfg(unix)]
    async fn test_shell_pre_send_filter() {
        let mut shell = Shell::new("sh")
            .enable_buffer()
            .on_send(|cmd| async move {
                if cmd.contains("BLOCK") {
                    None // 拦截并丢弃该命令
                } else {
                    Some(cmd.replace("FOO", "BAR"))
                }
            })
            .spawn()
            .await
            .unwrap();

        // 该命令包含 BLOCK，应被拦截
        shell.send_line("echo BLOCK_ME").await.unwrap();
        let out_blocked = shell.output(Some(Duration::from_millis(200))).await;
        assert!(!out_blocked.contains("BLOCK_ME"));

        // 该命令 FOO 被替换为 BAR
        shell.send_line("echo FOO_TEST").await.unwrap();
        let out_modified = shell.output(Some(Duration::from_millis(200))).await;
        assert!(out_modified.contains("BAR_TEST"));

        shell.exit().await.unwrap();
    }

    // ── 5. 控制字符与重置/生命周期测试 (reset / exit / close) ───────────────

    #[tokio::test]
    #[cfg(unix)]
    async fn test_shell_reset_lifecycle() {
        let mut shell = Shell::new("sh")
            .enable_buffer()
            .spawn()
            .await
            .unwrap();

        // 设置环境变量
        shell.send_line("EXPORTED_VAR=12345").await.unwrap();
        shell.send_line("echo $EXPORTED_VAR").await.unwrap();

        let out1 = shell.output(Some(Duration::from_millis(200))).await;
        assert!(out1.contains("12345"));

        // 执行 Reset，底层进程将被杀掉并重新 spawn
        shell.reset().await.expect("Reset failed");

        // 验证旧环境状态已清理
        shell.send_line("echo $EXPORTED_VAR").await.unwrap();
        let out2 = shell.output(Some(Duration::from_millis(200))).await;
        assert!(!out2.contains("12345"));

        shell.exit().await.unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_shell_on_close_callback() {
        let (tx_close, mut rx_close) = mpsc::channel::<()>(1);

        let mut shell = Shell::new("sh")
            .on_close(move || {
                let tx = tx_close.clone();
                async move {
                    let _ = tx.send(()).await;
                }
            })
            .spawn()
            .await
            .unwrap();

        shell.exit().await.unwrap();

        let closed_triggered = timeout(Duration::from_secs(2), rx_close.recv())
            .await
            .is_ok();

        assert!(closed_triggered, "on_close callback was not invoked");
    }


}