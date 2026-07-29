//! `OutputPump`：统一封装"解码 + 按模式分派（Raw 立即输出 / Line 按行 flush）"
//! 的状态机。管道模式（`read_stream`）与 PTY 模式（`pty::run_pty_io`）
//! 共用同一套实现，避免出现两份容易行为漂移的读取逻辑。

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::time::MissedTickBehavior;

use crate::shell::buffer::OutputBuffer;
use crate::shell::callbacks::{CallbackHub, CallbackMode};
use crate::util::StreamDecoder;

/// 每次 `read()` 的块大小。
pub(crate) const READ_CHUNK_SIZE: usize = 8192;

/// 行模式：输出空闲多久后，即便没有换行符也强制 flush 已缓存内容。
pub(crate) const LINE_MODE_FLUSH_IDLE: Duration = Duration::from_millis(80);

/// 行模式：单行数据超过该长度时强制切块 flush，防止超长行导致内存无限增长。
const LINE_MODE_FORCE_FLUSH: usize = 64 * 1024;

/// 管道 / PTY 输入端的统一消息类型。
pub(crate) enum StdinMsg {
    Data(String),
    Close,
    Eof,
    /// 调整 PTY 窗口尺寸（列, 行）。管道模式下会被忽略。
    #[cfg(feature = "pty")]
    Resize(u16, u16),
}

/// 解码 + 分派状态机。
///
/// - `decode()` / `decode_eof()`：只做解码，返回本次新解码出来的文本引用，
///   供调用方在真正分派前先做一些旁路处理（例如 PTY 模式下喂给 vt100 解析器）。
/// - `dispatch()` / `finish()`：把已解码内容按 `CallbackMode` 分派出去
///   （Raw 立即 emit；Line 攒进 `carry` 按行 drain）。
/// - `feed()`：`decode()` + `dispatch()` 的便捷组合，管道模式直接用它即可。
pub(crate) struct OutputPump {
    decoder: StreamDecoder,
    decoded: String,
    carry: String,
    mode: CallbackMode,
    eof_decoded: bool,
}

impl OutputPump {
    pub fn new(mode: CallbackMode) -> Self {
        Self {
            decoder: StreamDecoder::new(),
            decoded: String::new(),
            carry: String::new(),
            mode,
            eof_decoded: false,
        }
    }

    /// 是否处于行模式（供调用方决定是否需要驱动 idle ticker）。
    pub fn is_line_mode(&self) -> bool {
        self.mode == CallbackMode::Line
    }

    /// 解码一段新读到的字节，返回累计到 `decoded` 缓冲区的只读引用。
    pub fn decode(&mut self, bytes: &[u8]) -> &str {
        self.decoder.feed(bytes, &mut self.decoded);
        &self.decoded
    }

    /// EOF 时调用：flush 解码器内部残留字节。可安全重复调用（幂等）。
    pub fn decode_eof(&mut self) -> &str {
        if !self.eof_decoded {
            self.decoder.finish(&mut self.decoded);
            self.eof_decoded = true;
        }
        &self.decoded
    }

    /// 把 `decode()` 产生的内容按模式分派出去（回调 + 写入 buffer）。
    pub async fn dispatch(
        &mut self,
        ob: &Option<Arc<OutputBuffer>>,
        callbacks: &CallbackHub,
        is_stderr: bool,
    ) {
        if self.decoded.is_empty() {
            return;
        }
        match self.mode {
            CallbackMode::Raw => {
                emit(std::mem::take(&mut self.decoded), ob, callbacks, is_stderr).await;
            }
            CallbackMode::Line => {
                self.carry.push_str(&self.decoded);
                self.decoded.clear();
                drain_lines(&mut self.carry, ob, callbacks, is_stderr).await;
            }
        }
    }

    /// `decode()` + `dispatch()` 的便捷组合。
    pub async fn feed(
        &mut self,
        bytes: &[u8],
        ob: &Option<Arc<OutputBuffer>>,
        callbacks: &CallbackHub,
        is_stderr: bool,
    ) {
        self.decode(bytes);
        self.dispatch(ob, callbacks, is_stderr).await;
    }

    /// idle ticker 触发时调用：Line 模式下把半行强制 flush 出去。
    pub async fn flush_idle(
        &mut self,
        ob: &Option<Arc<OutputBuffer>>,
        callbacks: &CallbackHub,
        is_stderr: bool,
    ) {
        if self.mode == CallbackMode::Line && !self.carry.is_empty() {
            emit(std::mem::take(&mut self.carry), ob, callbacks, is_stderr).await;
        }
    }

    /// EOF / 读错误时的收尾：flush 解码器残留 + carry 中的全部内容。
    pub async fn finish(
        &mut self,
        ob: &Option<Arc<OutputBuffer>>,
        callbacks: &CallbackHub,
        is_stderr: bool,
    ) {
        self.decode_eof();
        match self.mode {
            CallbackMode::Raw => {
                if !self.decoded.is_empty() {
                    emit(std::mem::take(&mut self.decoded), ob, callbacks, is_stderr).await;
                }
            }
            CallbackMode::Line => {
                self.carry.push_str(&self.decoded);
                self.decoded.clear();
                drain_lines(&mut self.carry, ob, callbacks, is_stderr).await;
                if !self.carry.is_empty() {
                    emit(std::mem::take(&mut self.carry), ob, callbacks, is_stderr).await;
                }
            }
        }
    }
}

/// 从 `carry` 中持续提取完整行并推送，直到没有 `\n` 或触发强制切块。
pub(crate) async fn drain_lines(
    carry: &mut String,
    ob: &Option<Arc<OutputBuffer>>,
    callbacks: &CallbackHub,
    is_stderr: bool,
) {
    loop {
        if let Some(pos) = carry.find('\n') {
            // 含 \n 的原始行存入 buffer（保留原始格式）；
            // 回调传去掉末尾空白的版本。
            let raw_line: String = carry.drain(..=pos).collect();
            let trimmed = raw_line.trim_end_matches(['\r', '\n']).to_string();

            if let Some(ob) = ob {
                ob.push(Arc::from(raw_line.as_str())).await;
            }
            callbacks.fire_output(trimmed, is_stderr).await;
        } else if carry.len() >= LINE_MODE_FORCE_FLUSH {
            // 单行过长，强制切块防止内存无限增长
            let chunk = std::mem::take(carry);
            if let Some(ob) = ob {
                ob.push(Arc::from(chunk.as_str())).await;
            }
            callbacks.fire_output(chunk, is_stderr).await;
        } else {
            break;
        }
    }
}

/// 统一推送原语：chunk 原样存入 buffer 并触发回调（不做任何裁剪）。
pub(crate) async fn emit(
    chunk: String,
    ob: &Option<Arc<OutputBuffer>>,
    callbacks: &CallbackHub,
    is_stderr: bool,
) {
    if chunk.is_empty() {
        return;
    }
    if let Some(ob) = ob {
        ob.push(Arc::from(chunk.as_str())).await;
    }
    callbacks.fire_output(chunk, is_stderr).await;
}

/// 管道模式下读取单个流（stdout 或 stderr）并驱动 `OutputPump`。
/// PTY 模式复用同一个 `OutputPump`，但驱动方式不同（见 `pty::run_pty_io`），
/// 因为 PTY 还需要在同一个 `select!` 循环里处理 stdin/resize。
pub(crate) async fn read_stream<R>(
    mut reader: R,
    ob: Option<Arc<OutputBuffer>>,
    callbacks: CallbackHub,
    is_stderr: bool,
    mode: CallbackMode,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    let mut pump = OutputPump::new(mode);
    let mut raw = vec![0u8; READ_CHUNK_SIZE];

    if !pump.is_line_mode() {
        // Raw 模式：没有 idle flush 的必要，线性读取即可，延迟最低。
        loop {
            match reader.read(&mut raw).await {
                Ok(0) => {
                    pump.finish(&ob, &callbacks, is_stderr).await;
                    break;
                }
                Ok(n) => pump.feed(&raw[..n], &ob, &callbacks, is_stderr).await,
                Err(_) => break,
            }
        }
        return;
    }

    // Line 模式：需要空闲定时器强制 flush 半行（如无换行的交互式 prompt）。
    let mut ticker = tokio::time::interval(LINE_MODE_FLUSH_IDLE);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // 消耗第一个立即触发的 tick

    loop {
        tokio::select! {
            biased;

            res = reader.read(&mut raw) => {
                match res {
                    Ok(0) => {
                        pump.finish(&ob, &callbacks, is_stderr).await;
                        break;
                    }
                    Ok(n) => pump.feed(&raw[..n], &ob, &callbacks, is_stderr).await,
                    Err(_) => break,
                }
            }

            _ = ticker.tick() => {
                pump.flush_idle(&ob, &callbacks, is_stderr).await;
            }
        }
    }
}