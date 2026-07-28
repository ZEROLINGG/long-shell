use std::path::Path;
use anyhow::{anyhow, Result};
use encoding_rs::{CoderResult, Decoder, Encoding, UTF_8};

pub fn normalize_shell_name(shell: &str) -> Result<String> {
    let mut name = Path::new(shell)
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("{} {}", "invalid shell path:", shell))?
        .to_lowercase();
    if let Some(idx) = name.find(|c: char| c.is_ascii_digit() || c == '.') {
        name.truncate(idx);
    }

    Ok(name)
}

/// 检测当前应使用的字符编码：
/// - Windows：读取控制台输出代码页 (`GetConsoleOutputCP`)，映射到对应的 `Encoding`；
/// - 其他平台：默认 UTF-8。
pub fn detect_encoding() -> &'static Encoding {
    #[cfg(windows)]
    {
        detect_encoding_windows()
    }
    #[cfg(not(windows))]
    {
        UTF_8
    }
}

#[cfg(windows)]
fn detect_encoding_windows() -> &'static Encoding {
    unsafe extern "system" {
        fn GetConsoleOutputCP() -> u32;
    }
    let cp = unsafe { GetConsoleOutputCP() };
    codepage::to_encoding(cp as u16).unwrap_or(UTF_8)
}

/// 一次性解码整个字节缓冲区。
///
/// 注意：这是**无状态**解码，只适合"一次性拿到完整数据"的场景
/// （比如一次性读取完子进程全部输出后再解码）。
/// 如果数据是分块到达的（例如从管道逐次 `read()` 出来），
/// 请使用 [`StreamDecoder`]，否则多字节字符可能会在块边界处被
/// 错误替换为 `�`。
pub fn decode_bytes(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    let (cow, _enc, _had_errors) = detect_encoding().decode(bytes);
    cow.into_owned()
}

/// 支持跨多次 `read()` 增量解码的解码器。
///
/// 内部使用 `encoding_rs::Decoder`：如果一次 `feed` 传入的字节里
/// 包含不完整的多字节序列（例如一个 UTF-8 字符被从中间截断），
/// 解码器会在内部缓存这部分字节，等待下一次 `feed` 补齐后再解码，
/// 因此不会在块边界处产生乱码。只有在 `finish()`（真正 EOF）时，
/// 如果仍有残留的非法/不完整字节，才会被替换为 `�`。
pub struct StreamDecoder {
    decoder: Decoder,
}

impl StreamDecoder {
    pub fn new() -> Self {
        Self::with_encoding(detect_encoding())
    }

    pub fn with_encoding(enc: &'static Encoding) -> Self {
        Self {
            decoder: enc.new_decoder(),
        }
    }

    /// 增量喂入新读取到的字节，解码结果追加到 `out` 末尾。
    pub fn feed(&mut self, mut src: &[u8], out: &mut String) {
        loop {
            if out.capacity() - out.len() < 4096 {
                out.reserve(4096);
            }
            let (result, read, _had_errors) = self.decoder.decode_to_string(src, out, false);
            src = &src[read..];
            if let CoderResult::InputEmpty = result {
                break;
            }
        }
    }

    /// 流结束（EOF）时调用，flush 掉解码器内部残留的字节。
    pub fn finish(&mut self, out: &mut String) {
        loop {
            if out.capacity() - out.len() < 4096 {
                out.reserve(4096);
            }
            let (result, _read, _had_errors) = self.decoder.decode_to_string(&[], out, true);
            if let CoderResult::InputEmpty = result {
                break;
            }
        }
    }
}

impl Default for StreamDecoder {
    fn default() -> Self {
        Self::new()
    }
}