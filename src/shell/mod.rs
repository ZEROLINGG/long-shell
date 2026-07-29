//! `Shell`：对外的核心类型，本文件只负责"胶水"——字段组装与公开方法转发，
//! 具体实现分散在各个子模块中：
//!
//! - `builder`  — 链式配置
//! - `profile`  — 各 shell 的启动参数/初始化/退出命令
//! - `callbacks`— 回调类型与封装
//! - `buffer`   — 有界输出缓冲区
//! - `stream`   — 管道模式读取 + Raw/Line 统一解码分派
//! - `backend`  — PTY 状态收敛 + 统一的会话启动入口

mod backend;
pub(crate) mod callbacks;
pub(crate) mod buffer;
mod builder;
pub(crate) mod profile;
pub(crate) mod stream;

pub use builder::ShellBuilder;
pub use buffer::OutputBuffer;
pub use callbacks::CallbackMode;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, OnceCell};
use tokio::task::JoinHandle;

#[cfg(feature = "pty")]
use rust_pty::PtySignal;

use crate::shell::backend::LaunchConfig;
#[cfg(feature = "pty")]
use crate::shell::backend::PtyState;
use crate::shell::callbacks::{BoxFuture, CallbackHub, PreSendHook};
use crate::shell::profile::ShellProfile;
use crate::shell::stream::StdinMsg;

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

// ─── ShellOutput ───────────────────────────────────────────────────────────

/// 包含标准输出和标准错误的结果结构体。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShellOutput {
    pub stdout: String,
    pub stderr: String,
}

impl ShellOutput {
    pub fn is_empty(&self) -> bool {
        self.stdout.is_empty() && self.stderr.is_empty()
    }
}

// ─── Shell ────────────────────────────────────────────────────────────────

pub struct Shell {
    pub shell_path: String,
    tx_stdin: mpsc::Sender<StdinMsg>,
    drop_tx: Option<oneshot::Sender<()>>,
    pre_send: PreSendHook,
    callbacks: CallbackHub,
    join: Option<JoinHandle<()>>,
    droped: bool,
    close_notify: Arc<Notify>,
    output_buffer: Option<Arc<OutputBuffer>>,
    error_buffer: Option<Arc<OutputBuffer>>,

    /// `None` = 管道模式；`Some` = PTY 模式及其专属状态。
    #[cfg(feature = "pty")]
    pty: Option<PtyState>,
}

impl Shell {
    pub fn new(shell: impl Into<String>) -> ShellBuilder {
        ShellBuilder::new(shell)
    }

    /// 由 `ShellBuilder::spawn()` 调用的唯一构造入口。
    pub(crate) async fn spawn_new(
        cfg: LaunchConfig,
        pre_send: PreSendHook,
        close_notify: Arc<Notify>,
    ) -> Result<Shell> {
        // launch() 会消耗掉 cfg 里的字段，这里先拷贝出后续要长期持有的部分。
        let shell_path = cfg.shell_path.clone();
        let callbacks = cfg.callbacks.clone();
        let output_buffer = cfg.output_buffer.clone();
        let error_buffer = cfg.error_buffer.clone();

        let session = backend::launch(cfg).await?;

        Ok(Shell {
            shell_path,
            tx_stdin: session.tx_stdin,
            drop_tx: Some(session.drop_tx),
            pre_send,
            callbacks,
            join: Some(session.join),
            droped: false,
            close_notify,
            output_buffer,
            error_buffer,
            #[cfg(feature = "pty")]
            pty: session.pty,
        })
    }

    /// 当前会话是否运行在 PTY 模式下。
    #[cfg(feature = "pty")]
    pub fn is_pty(&self) -> bool {
        self.pty.is_some()
    }

    // ── 输出读取 ──────────────────────────────────────────────────────────

    /// 等待直到 stdout/stderr 均空闲超过 `timeout`，但不清空缓冲区。
    async fn wait_idle(&self, timeout: Duration) {
        let ob_out = self.output_buffer.as_ref();
        let ob_err = self.error_buffer.as_ref();

        if ob_out.is_none() && ob_err.is_none() {
            return;
        }

        loop {
            let notify_out = async {
                if let Some(ob) = ob_out {
                    ob.notify.notified().await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            let notify_err = async {
                if let Some(ob) = ob_err {
                    ob.notify.notified().await;
                } else {
                    std::future::pending::<()>().await;
                }
            };

            tokio::select! {
                _ = self.close_notify.notified() => break,
                res = tokio::time::timeout(timeout, async {
                    tokio::select! {
                        _ = notify_out => {}
                        _ = notify_err => {}
                    }
                }) => {
                    match res {
                        Ok(_)  => continue,
                        Err(_) => break,
                    }
                }
            }
        }
    }

    /// 等待直到 stdout 和 stderr 均空闲超过 `idle_time`（默认 200 ms），然后返回并清空缓冲。
    pub async fn output(&mut self, idle_time: Option<Duration>) -> ShellOutput {
        let timeout = idle_time.unwrap_or(Duration::from_millis(200));

        if self.output_buffer.is_none() && self.error_buffer.is_none() {
            return ShellOutput::default();
        }

        self.wait_idle(timeout).await;

        let stdout = if let Some(ob) = &self.output_buffer {
            ob.take().await
        } else {
            String::new()
        };
        let stderr = if let Some(ob) = &self.error_buffer {
            ob.take().await
        } else {
            String::new()
        };

        ShellOutput { stdout, stderr }
    }

    /// 持续等待，直到 stdout 或 stderr 中出现指定的子串 `pattern`，
    /// 或等待超过 `timeout`，然后返回期间累积到的全部输出并清空缓冲区。
    pub async fn output_until(
        &mut self,
        pattern: String,
        timeout: Option<Duration>,
    ) -> ShellOutput {
        let ob_out = self.output_buffer.as_ref();
        let ob_err = self.error_buffer.as_ref();

        if ob_out.is_none() && ob_err.is_none() {
            return ShellOutput::default();
        }

        let mut stdout_acc = String::new();
        let mut stderr_acc = String::new();
        let deadline = timeout.map(|d| tokio::time::Instant::now() + d);

        loop {
            if let Some(ob) = ob_out {
                let chunk = ob.take().await;
                if !chunk.is_empty() {
                    stdout_acc.push_str(&chunk);
                }
            }
            if let Some(ob) = ob_err {
                let chunk = ob.take().await;
                if !chunk.is_empty() {
                    stderr_acc.push_str(&chunk);
                }
            }

            if stdout_acc.contains(&pattern) || stderr_acc.contains(&pattern) {
                break;
            }

            let sleep_fut: BoxFuture<'_, ()> = match deadline {
                Some(inst) => {
                    if tokio::time::Instant::now() >= inst {
                        break;
                    }
                    Box::pin(tokio::time::sleep_until(inst))
                }
                None => Box::pin(std::future::pending()),
            };

            let notify_out = async {
                if let Some(ob) = ob_out {
                    ob.notify.notified().await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            let notify_err = async {
                if let Some(ob) = ob_err {
                    ob.notify.notified().await;
                } else {
                    std::future::pending::<()>().await;
                }
            };

            tokio::select! {
                _ = self.close_notify.notified() => break,
                _ = sleep_fut => break,
                _ = notify_out => {}
                _ = notify_err => {}
            }
        }

        if let Some(ob) = ob_out {
            let chunk = ob.take().await;
            if !chunk.is_empty() {
                stdout_acc.push_str(&chunk);
            }
        }
        if let Some(ob) = ob_err {
            let chunk = ob.take().await;
            if !chunk.is_empty() {
                stderr_acc.push_str(&chunk);
            }
        }

        ShellOutput {
            stdout: stdout_acc,
            stderr: stderr_acc,
        }
    }

    /// 返回渲染后的虚拟终端屏幕快照（仅 PTY 模式，且未 `disable_snapshot()`）。
    #[cfg(feature = "pty")]
    pub async fn output_snapshot(&mut self, wait: Option<Duration>) -> Result<String> {
        ensure!(!self.droped, "shell is closed");

        let parser = self
            .pty
            .as_ref()
            .ok_or_else(|| anyhow!("output_snapshot() requires enable_pty()"))?
            .vt_parser()?
            .clone();

        if let Some(idle) = wait {
            self.wait_idle(idle).await;
        }

        let guard = parser
            .lock()
            .map_err(|_| anyhow!("vt100 parser lock poisoned"))?;
        Ok(guard.screen().contents())
    }

    /// 克隆一份 `vt100::Screen`，用于自定义渲染（拿光标位置、每格颜色等）。
    #[cfg(feature = "pty")]
    pub fn screen_clone(&self) -> Result<vt100::Screen> {
        ensure!(!self.droped, "shell is closed");

        let parser = self
            .pty
            .as_ref()
            .ok_or_else(|| anyhow!("screen_clone() requires enable_pty()"))?
            .vt_parser()?;

        let guard = parser
            .lock()
            .map_err(|_| anyhow!("vt100 parser lock poisoned"))?;
        Ok(guard.screen().clone())
    }

    /// 调整 PTY 窗口尺寸（仅 PTY 模式）。
    #[cfg(feature = "pty")]
    pub async fn resize(&mut self, cols: u16, rows: u16) -> Result<()> {
        ensure!(!self.droped, "shell is closed");
        let state = self
            .pty
            .as_mut()
            .ok_or_else(|| anyhow!("resize() requires enable_pty()"))?;
        state.cols = cols;
        state.rows = rows;

        self.tx_stdin
            .send(StdinMsg::Resize(cols, rows))
            .await
            .map_err(|_| anyhow!("resize failed: stdin channel closed"))
    }

    /// 当前记录的 PTY 窗口尺寸（列, 行）；管道模式下返回 `None`。
    #[cfg(feature = "pty")]
    pub fn pty_window_size(&self) -> Option<(u16, u16)> {
        self.pty.as_ref().map(|p| (p.cols, p.rows))
    }

    /// 向 PTY 子进程转发一个信号（如 `PtySignal::Interrupt`）。仅 PTY 模式可用。
    #[cfg(feature = "pty")]
    pub async fn send_signal(&mut self, sig: PtySignal) -> Result<()> {
        ensure!(!self.droped, "shell is closed");
        let state = self
            .pty
            .as_ref()
            .ok_or_else(|| anyhow!("send_signal() requires enable_pty()"))?;
        state
            .signal_tx
            .send(sig)
            .await
            .map_err(|_| anyhow!("signal channel closed"))
    }

    /// 返回 stdout 缓冲区因超出容量而丢弃的累计字节数。
    pub fn output_truncated_bytes(&self) -> usize {
        self.output_buffer
            .as_ref()
            .map(|ob| ob.truncated_bytes.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// 返回 stderr 缓冲区因超出容量而丢弃的累计字节数。
    /// PTY 模式下 stdout/stderr 合并，恒为 0。
    pub fn error_truncated_bytes(&self) -> usize {
        self.error_buffer
            .as_ref()
            .map(|ob| ob.truncated_bytes.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    // ── 发送 ──────────────────────────────────────────────────────────────

    pub async fn send(&mut self, cmd: &str) -> Result<()> {
        ensure!(!self.droped, "shell is closed");

        if let Some(ctrl) = parse_control_shortcut(cmd) {
            return self.send_control_char(ctrl).await;
        }

        if let Some(s) = self.pre_send.process(cmd.to_string()).await {
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

    pub async fn send_eof(&mut self) -> Result<()> {
        self.tx_stdin
            .send(StdinMsg::Eof)
            .await
            .map_err(|_| anyhow!("send EOF failed"))
    }

    pub async fn send_control_char(&mut self, ctrl: char) -> Result<()> {
        let upper = ctrl.to_ascii_uppercase();

        #[cfg(feature = "pty")]
        if self.is_pty() {
            // PTY 模式下：使用公式计算完整的控制字符并直接发送给终端
            // 标准 ASCII 控制字符对应的可打印字符范围是 '@' (0x40) 到 '_' (0x5F)
            if upper >= '@' && upper <= '_' {
                // 公式：字符的 ASCII 码与 0x40 异或，映射到 0x00 - 0x1F
                let ctrl_byte = upper as u8 ^ 0x40;
                let data = String::from_utf8(vec![ctrl_byte]).unwrap_or_default();

                return self.tx_stdin
                    .send(StdinMsg::Data(data))
                    .await
                    .map_err(|_| anyhow!("send control char failed: stdin channel closed"));
            } else if upper == '?' {
                // 特例：终端标准中，`^?` 通常代表 DEL (0x7F / 127)
                return self.tx_stdin
                    .send(StdinMsg::Data("\x7F".to_string()))
                    .await
                    .map_err(|_| anyhow!("send DEL failed: stdin channel closed"));
            }

            // 如果不是有效范围内的字符，忽略
            return Ok(());
        }

        // 管道模式（非 PTY 模式）下：只保留特殊的硬编码控制语义
        match upper {
            'R' => self.reset().await,    // 自定义语义：彻底重置 Shell 会话
            'D' => self.send_eof().await, // EOF：向底层管道发送结束信号 (关闭 stdin)
            _ => Ok(()),                  // 按需求，忽略管道模式下无意义的其他控制字符
        }
    }


    // ── 生命周期 ──────────────────────────────────────────────────────────

    pub async fn join_close(&mut self) -> Result<()> {
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
            handle.await.map_err(|e| anyhow!("join_exit failed: {e}"))?;
        }
        Ok(())
    }

    /// 重新拉起一个全新的会话（复用相同的 shell_path / 回调 / 缓冲区 / PTY 配置）。
    pub async fn reset(&mut self) -> Result<()> {
        ensure!(!self.droped, "shell is closed");
        self.exit().await?;
        if let Some(handle) = self.join.take() {
            let _ = handle.await;
        }

        let cfg = LaunchConfig {
            shell_path: self.shell_path.clone(),
            callbacks: self.callbacks.clone(),
            output_buffer: self.output_buffer.clone(),
            error_buffer: self.error_buffer.clone(),
            #[cfg(feature = "pty")]
            pty_opts: self.pty.as_ref().map(PtyState::options),
        };

        let session = backend::launch(cfg).await?;

        self.tx_stdin = session.tx_stdin;
        self.drop_tx = Some(session.drop_tx);
        self.join = Some(session.join);
        #[cfg(feature = "pty")]
        {
            self.pty = session.pty;
        }
        self.droped = false;
        Ok(())
    }

    /// 关闭当前会话（可通过 `reset()` 恢复）。
    pub async fn exit(&mut self) -> Result<()> {
        ensure!(!self.droped, "shell is closed");

        let exit_cmd = ShellProfile::detect(&self.shell_path)
            .map(|p| p.exit_command())
            .unwrap_or_else(|_| "exit\n".to_string());

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
            Err(_) => self.close(),
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
}

impl Drop for Shell {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// 识别 `^C` / `^A` / `^?` 这类控制字符简写，返回大写的控制字符。
fn parse_control_shortcut(cmd: &str) -> Option<char> {
    if cmd.len() >= 5 {
        return None;
    }
    let trimmed = cmd.trim();
    let mut chars = trimmed.chars();
    match (chars.next(), chars.next(), chars.next()) {
        (Some('^'), Some(c), None) => {
            let upper = c.to_ascii_uppercase();
            // 允许 '@' 到 '_' (包含了 A-Z) 以及 '?'
            if (upper >= '@' && upper <= '_') || upper == '?' {
                Some(upper)
            } else {
                None
            }
        }
        _ => None,
    }
}

// ─── 测试 ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc as test_mpsc;
    use tokio::time::{timeout, Duration};

    #[test]
    fn control_shortcut_parsing() {
        assert_eq!(parse_control_shortcut("^C"), Some('C'));
        assert_eq!(parse_control_shortcut(" ^d "), Some('D'));
        assert_eq!(parse_control_shortcut("^X"), None);
        assert_eq!(parse_control_shortcut("echo hi"), None);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn basic_output() {
        let mut shell = Shell::new("sh")
            .enable_buffer()
            .spawn()
            .await
            .expect("failed to spawn sh");

        shell.send_line("echo hello_tokio").await.unwrap();
        let out = shell.output(Some(Duration::from_millis(300))).await;
        assert!(out.stdout.contains("hello_tokio"));

        shell.exit().await.unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn line_callback_mode() {
        let (tx, mut rx) = test_mpsc::channel::<String>(10);

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
        let line1 = timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(line1, "line1");

        shell.exit().await.unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn pre_send_filter() {
        let mut shell = Shell::new("sh")
            .enable_buffer()
            .on_send(|cmd| async move {
                if cmd.contains("BLOCK") {
                    None
                } else {
                    Some(cmd.replace("FOO", "BAR"))
                }
            })
            .spawn()
            .await
            .unwrap();

        shell.send_line("echo BLOCK_ME").await.unwrap();
        let blocked = shell.output(Some(Duration::from_millis(200))).await;
        assert!(!blocked.stdout.contains("BLOCK_ME"));

        shell.send_line("echo FOO_TEST").await.unwrap();
        let modified = shell.output(Some(Duration::from_millis(200))).await;
        assert!(modified.stdout.contains("BAR_TEST"));

        shell.exit().await.unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn reset_clears_environment() {
        let mut shell = Shell::new("sh").enable_buffer().spawn().await.unwrap();

        shell.send_line("EXPORTED_VAR=12345").await.unwrap();
        shell.send_line("echo $EXPORTED_VAR").await.unwrap();
        let out1 = shell.output(Some(Duration::from_millis(200))).await;
        assert!(out1.stdout.contains("12345"));

        shell.reset().await.expect("reset failed");

        shell.send_line("echo $EXPORTED_VAR").await.unwrap();
        let out2 = shell.output(Some(Duration::from_millis(200))).await;
        assert!(!out2.stdout.contains("12345"));

        shell.exit().await.unwrap();
    }

    #[tokio::test]
    #[cfg(all(unix, feature = "pty"))]
    async fn pty_basic_output_and_snapshot() {
        let mut shell = Shell::new("sh")
            .enable_pty()
            .enable_buffer()
            .spawn()
            .await
            .expect("failed to spawn pty shell");

        assert!(shell.is_pty());
        assert_eq!(shell.pty_window_size(), Some((80, 24)));

        shell.send_line("printf 'SNAP_MARK\\n'").await.unwrap();
        let snap = shell
            .output_snapshot(Some(Duration::from_millis(300)))
            .await
            .expect("snapshot failed");
        assert!(snap.contains("SNAP_MARK"));

        shell.exit().await.unwrap();
    }
}