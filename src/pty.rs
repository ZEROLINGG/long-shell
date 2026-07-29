use std::sync::{Arc, Mutex as StdMutex};

use anyhow::Result;
use rust_pty::{NativePtySystem, PtyChild, PtyConfig, PtyMaster, PtySignal, PtySystem, WindowSize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::shell::callbacks::{CallbackHub, CallbackMode};
use crate::shell::profile::ShellProfile;
use crate::shell::stream::{OutputPump, StdinMsg, LINE_MODE_FLUSH_IDLE, READ_CHUNK_SIZE};
use crate::shell::OutputBuffer;

/// `spawn_pty_process` 的返回值集合。
pub(crate) struct PtySpawnResult {
    pub tx_stdin: mpsc::Sender<StdinMsg>,
    pub drop_tx: oneshot::Sender<()>,
    pub join: JoinHandle<()>,
    pub vt_parser: Option<Arc<StdMutex<vt100::Parser>>>,
    pub signal_tx: mpsc::Sender<PtySignal>,
}

/// 启动一个 PTY 会话，返回给 `Shell` 用于驱动交互的各种句柄。
pub(crate) async fn spawn_pty_process(
    shell_path: &str,
    profile: &ShellProfile,
    cols: u16,
    rows: u16,
    scrollback: usize,
    track_screen: bool,
    callbacks: CallbackHub,
    output_buffer: Option<Arc<OutputBuffer>>,
) -> Result<PtySpawnResult> {
    let args = profile.args(true)?;
    let config = PtyConfig::builder().window_size(cols, rows).build();

    let (mut master, child) = NativePtySystem::spawn(shell_path, args, &config).await?;

    if let Some(init) = profile.init_command() {
        master.write_all(init.as_bytes()).await?;
        master.flush().await?;
    }

    let vt_parser: Option<Arc<StdMutex<vt100::Parser>>> = if track_screen {
        Some(Arc::new(StdMutex::new(vt100::Parser::new(
            rows, cols, scrollback,
        ))))
    } else {
        None
    };

    let (tx_stdin, rx_stdin) = mpsc::channel::<StdinMsg>(32);
    let (drop_tx, drop_rx) = oneshot::channel::<()>();
    let (signal_tx, signal_rx) = mpsc::channel::<PtySignal>(8);

    let mode = callbacks.mode().await;

    let cb_io = callbacks.clone();
    let ob_io = output_buffer.clone();
    let vt_io = vt_parser.clone();

    let io_task = tokio::spawn(run_pty_io(master, rx_stdin, ob_io, cb_io, vt_io, mode));

    let cb_super = callbacks.clone();
    let join = tokio::spawn(run_supervisor(child, drop_rx, signal_rx, io_task, cb_super));

    Ok(PtySpawnResult {
        tx_stdin,
        drop_tx,
        join,
        vt_parser,
        signal_tx,
    })
}

/// 单一 I/O 任务：持续持有 `master`，交替处理"读输出"、"写输入/resize"、
/// "行模式空闲 flush"。解码/分派逻辑复用 `OutputPump`，与管道模式共享
/// 同一套实现，不再各写一份。
async fn run_pty_io<M>(
    mut master: M,
    mut rx_stdin: mpsc::Receiver<StdinMsg>,
    ob: Option<Arc<OutputBuffer>>,
    callbacks: CallbackHub,
    vt: Option<Arc<StdMutex<vt100::Parser>>>,
    mode: CallbackMode,
) where
    M: PtyMaster,
{
    let mut pump = OutputPump::new(mode);
    let mut raw = vec![0u8; READ_CHUNK_SIZE];

    let mut ticker = tokio::time::interval(LINE_MODE_FLUSH_IDLE);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // 消耗第一个立即触发的 tick

    loop {
        tokio::select! {
            biased;

            res = master.read(&mut raw) => {
                match res {
                    Ok(0) | Err(_) => {
                        let text = pump.decode_eof();
                        if let Some(vt) = &vt {
                            if let Ok(mut p) = vt.lock() {
                                p.process(text.as_bytes());
                            }
                        }
                        pump.finish(&ob, &callbacks, false).await;
                        break;
                    }
                    Ok(n) => {
                        let text = pump.decode(&raw[..n]);
                        if let Some(vt) = &vt {
                            if let Ok(mut p) = vt.lock() {
                                p.process(text.as_bytes());
                            }
                        }
                        pump.dispatch(&ob, &callbacks, false).await;
                    }
                }
            }

            msg = rx_stdin.recv() => {
                match msg {
                    None | Some(StdinMsg::Close) => break,
                    Some(StdinMsg::Eof) => {
                        let _ = master.write_all(&[0x04]).await;
                        let _ = master.flush().await;
                    }
                    Some(StdinMsg::Data(data)) => {
                        if master.write_all(data.as_bytes()).await.is_err() {
                            break;
                        }
                        if master.flush().await.is_err() {
                            break;
                        }
                    }
                    Some(StdinMsg::Resize(cols, rows)) => {
                        let _ = master.resize(WindowSize::new(cols, rows));
                        if let Some(vt) = &vt {
                            if let Ok(mut p) = vt.lock() {
                                // set_size 定义在 Screen 上，需通过 screen_mut() 调用
                                p.screen_mut().set_size(rows, cols);
                            }
                        }
                    }
                }
            }

            _ = ticker.tick(), if pump.is_line_mode() => {
                pump.flush_idle(&ob, &callbacks, false).await;
            }
        }
    }
}

/// 监督任务：等待子进程退出 / 响应外部关闭请求 / 转发信号（如 Ctrl+C）。
/// 与管道模式的 join 任务保持相同的语义：只有"自然退出"才触发 `on_exit`，
/// 被外部强制 kill 时不触发。
async fn run_supervisor<C>(
    mut child: C,
    mut drop_rx: oneshot::Receiver<()>,
    mut signal_rx: mpsc::Receiver<PtySignal>,
    io_task: JoinHandle<()>,
    callbacks: CallbackHub,
) where
    C: PtyChild,
{
    loop {
        tokio::select! {
            status = child.wait() => {
                let code = status.ok().and_then(|s| s.code());
                callbacks.fire_exit(code).await;
                break;
            }
            _ = &mut drop_rx => {
                let _ = child.kill();
                let _ = child.wait().await;
                break;
            }
            sig = signal_rx.recv() => {
                if let Some(sig) = sig {
                    let _ = child.signal(sig);
                }
                // 继续循环，等待真正的退出或关闭信号
            }
        }
    }

    let _ = io_task.await;
    callbacks.fire_close().await;
}