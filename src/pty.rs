use std::sync::{Arc, Mutex as StdMutex};

use anyhow::Result;
use rust_pty::{NativePtySystem, PtyChild, PtyConfig, PtyMaster, PtySignal, PtySystem, WindowSize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::shell::{
    CallbackMode, Callbacks, LINE_MODE_FLUSH_IDLE, OutputBuffer, READ_CHUNK_SIZE, StdinMsg,
    callback_mode, drain_lines, emit, fire_on_close, fire_on_exit, init_command, shell_args,
};
use crate::util::StreamDecoder;

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
    shell_name: &str,
    cols: u16,
    rows: u16,
    scrollback: usize,
    track_screen: bool,
    callbacks: Arc<Mutex<Callbacks>>,
    output_buffer: Option<Arc<OutputBuffer>>,
) -> Result<PtySpawnResult> {
    let args = shell_args(shell_name, true)?;

    let config = PtyConfig::builder().window_size(cols, rows).build();


    let (mut master, child) = NativePtySystem::spawn(shell_path, args, &config).await?;

    if let Some(init) = init_command(shell_name) {
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

    let mode = callback_mode(&callbacks).await;

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
/// "行模式空闲 flush"。
async fn run_pty_io<M>(
    mut master: M,
    mut rx_stdin: mpsc::Receiver<StdinMsg>,
    ob: Option<Arc<OutputBuffer>>,
    callbacks: Arc<Mutex<Callbacks>>,
    vt: Option<Arc<StdMutex<vt100::Parser>>>,
    mode: CallbackMode,
) where
    M: PtyMaster,
{
    let mut decoder = StreamDecoder::new();
    let mut raw = vec![0u8; READ_CHUNK_SIZE];
    let mut decoded = String::new();
    let mut carry = String::new();

    let mut ticker = tokio::time::interval(LINE_MODE_FLUSH_IDLE);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // 消耗第一个立即触发的 tick

    loop {
        tokio::select! {
            biased;

            res = master.read(&mut raw) => {
                match res {
                    Ok(0) | Err(_) => {
                        decoder.finish(&mut decoded);
                        if let Some(vt) = &vt {
                            if let Ok(mut p) = vt.lock() {
                                p.process(decoded.as_bytes());
                            }
                        }
                        flush_final(mode, &mut decoded, &mut carry, &ob, &callbacks).await;
                        break;
                    }
                    Ok(n) => {
                        decoder.feed(&raw[..n], &mut decoded);
                        if let Some(vt) = &vt {
                            if let Ok(mut p) = vt.lock() {
                                p.process(decoded.as_bytes());
                            }
                        }
                        dispatch_chunk(mode, &mut decoded, &mut carry, &ob, &callbacks).await;
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

            _ = ticker.tick(), if mode == CallbackMode::Line => {
                if !carry.is_empty() {
                    emit(std::mem::take(&mut carry), &ob, &callbacks, false).await;
                }
            }
        }
    }
}

/// Raw 模式直接 emit；Line 模式先攒进 `carry` 再按行 drain。
/// `is_stderr` 恒为 `false`：PTY 合并了 stdout/stderr。
async fn dispatch_chunk(
    mode: CallbackMode,
    decoded: &mut String,
    carry: &mut String,
    ob: &Option<Arc<OutputBuffer>>,
    callbacks: &Arc<Mutex<Callbacks>>,
) {
    if decoded.is_empty() {
        return;
    }
    match mode {
        CallbackMode::Raw => {
            emit(std::mem::take(decoded), ob, callbacks, false).await;
        }
        CallbackMode::Line => {
            carry.push_str(decoded);
            decoded.clear();
            drain_lines(carry, ob, callbacks, false).await;
        }
    }
}

/// EOF/读错误时的收尾 flush。
async fn flush_final(
    mode: CallbackMode,
    decoded: &mut String,
    carry: &mut String,
    ob: &Option<Arc<OutputBuffer>>,
    callbacks: &Arc<Mutex<Callbacks>>,
) {
    match mode {
        CallbackMode::Raw => {
            if !decoded.is_empty() {
                emit(std::mem::take(decoded), ob, callbacks, false).await;
            }
        }
        CallbackMode::Line => {
            carry.push_str(decoded);
            decoded.clear();
            drain_lines(carry, ob, callbacks, false).await;
            if !carry.is_empty() {
                emit(std::mem::take(carry), ob, callbacks, false).await;
            }
        }
    }
}

/// 监督任务：等待子进程退出 / 响应外部关闭请求 / 转发信号（如 Ctrl+C）。
/// 与管道模式的 join 任务保持相同的语义：只有"自然退出"才触发 `on_exit`，
/// 被外部强制 kill 时不触发（与原管道模式行为一致）。
async fn run_supervisor<C>(
    mut child: C,
    mut drop_rx: oneshot::Receiver<()>,
    mut signal_rx: mpsc::Receiver<PtySignal>,
    io_task: JoinHandle<()>,
    callbacks: Arc<Mutex<Callbacks>>,
) where
    C: PtyChild,
{
    loop {
        tokio::select! {
            status = child.wait() => {
                let code = status.ok().and_then(|s| s.code());
                fire_on_exit(&callbacks, code).await;
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
    fire_on_close(&callbacks).await;
}
