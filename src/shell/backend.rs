//! PTY 专属状态收敛（`PtyState`）以及唯一的会话启动入口 `launch()`。
//!
//! `ShellBuilder::spawn()` 与 `Shell::reset()` 都通过 `launch()` 拉起底层
//! 进程，避免两处各自维护一份几乎相同的"判断 pipe/pty -> 构造 Shell 字段"
//! 分支逻辑（这是原实现里最大的重复代码来源）。

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

#[cfg(feature = "pty")]
use rust_pty::PtySignal;
#[cfg(feature = "pty")]
use std::sync::Mutex as StdMutex;

use crate::shell::buffer::OutputBuffer;
use crate::shell::callbacks::CallbackHub;
use crate::shell::profile::ShellProfile;
use crate::shell::stream::StdinMsg;

/// PTY 模式的可配置项（窗口尺寸 / 回滚缓冲 / 是否追踪屏幕）。
#[cfg(feature = "pty")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct PtyOptions {
    pub cols: u16,
    pub rows: u16,
    pub scrollback: usize,
    pub track_screen: bool,
}

#[cfg(feature = "pty")]
impl Default for PtyOptions {
    fn default() -> Self {
        Self {
            cols: 80,
            rows: 24,
            scrollback: 2000,
            track_screen: true,
        }
    }
}

/// PTY 模式下的专属运行时状态。`Shell.pty` 为 `None` 即代表当前是管道模式，
/// 从根本上消除了"管道模式下 pty_cols/pty_rows 等字段毫无意义地悬空存在"的问题。
#[cfg(feature = "pty")]
pub(crate) struct PtyState {
    pub cols: u16,
    pub rows: u16,
    pub scrollback: usize,
    pub track_screen: bool,
    pub vt_parser: Option<Arc<StdMutex<vt100::Parser>>>,
    pub signal_tx: mpsc::Sender<PtySignal>,
}

#[cfg(feature = "pty")]
impl PtyState {
    /// 取出 vt100 解析器；未开启屏幕追踪时返回统一的错误提示。
    pub fn vt_parser(&self) -> Result<&Arc<StdMutex<vt100::Parser>>> {
        self.vt_parser.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "screen tracking is disabled; remove disable_snapshot() to use this feature"
            )
        })
    }

    /// 导出当前配置，供 `reset()` 时复用（保持窗口尺寸/scrollback 不变）。
    pub fn options(&self) -> PtyOptions {
        PtyOptions {
            cols: self.cols,
            rows: self.rows,
            scrollback: self.scrollback,
            track_screen: self.track_screen,
        }
    }
}

/// 启动一个会话所需的全部配置。
pub(crate) struct LaunchConfig {
    pub shell_path: String,
    pub callbacks: CallbackHub,
    pub output_buffer: Option<Arc<OutputBuffer>>,
    pub error_buffer: Option<Arc<OutputBuffer>>,
    #[cfg(feature = "pty")]
    pub pty_opts: Option<PtyOptions>,
}

/// `launch()` 的返回值：驱动一个会话（管道或 PTY）所需的全部句柄。
pub(crate) struct SpawnedSession {
    pub tx_stdin: mpsc::Sender<StdinMsg>,
    pub drop_tx: oneshot::Sender<()>,
    pub join: JoinHandle<()>,
    #[cfg(feature = "pty")]
    pub pty: Option<PtyState>,
}

/// 唯一的会话启动入口。
pub(crate) async fn launch(cfg: LaunchConfig) -> Result<SpawnedSession> {
    let profile = ShellProfile::detect(&cfg.shell_path)?;

    #[cfg(feature = "pty")]
    if let Some(opts) = cfg.pty_opts {
        let result = crate::pty::spawn_pty_process(
            &cfg.shell_path,
            &profile,
            opts.cols,
            opts.rows,
            opts.scrollback,
            opts.track_screen,
            cfg.callbacks,
            cfg.output_buffer,
        )
            .await?;

        return Ok(SpawnedSession {
            tx_stdin: result.tx_stdin,
            drop_tx: result.drop_tx,
            join: result.join,
            pty: Some(PtyState {
                cols: opts.cols,
                rows: opts.rows,
                scrollback: opts.scrollback,
                track_screen: opts.track_screen,
                vt_parser: result.vt_parser,
                signal_tx: result.signal_tx,
            }),
        });
    }

    let (tx_stdin, drop_tx, join) = crate::pipe::spawn_process(
        &cfg.shell_path,
        &profile,
        cfg.callbacks,
        cfg.output_buffer,
        cfg.error_buffer,
    )
        .await?;

    Ok(SpawnedSession {
        tx_stdin,
        drop_tx,
        join,
        #[cfg(feature = "pty")]
        pty: None,
    })
}