//! 有界输出缓冲区：超出容量后自动丢弃最旧的数据块。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

struct OutputBufferInner {
    chunks: VecDeque<Arc<str>>,
    total_len: usize,
}

/// 有界输出缓冲区。
pub struct OutputBuffer {
    inner: Mutex<OutputBufferInner>,
    pub notify: Notify,
    max_bytes: usize,
    pub truncated_bytes: AtomicUsize,
}

impl OutputBuffer {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(OutputBufferInner {
                chunks: VecDeque::new(),
                total_len: 0,
            }),
            notify: Notify::new(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, timeout, Duration};

    #[tokio::test]
    async fn basic_push_take() {
        let buffer = OutputBuffer::new(1024);
        assert!(buffer.is_empty().await);

        buffer.push(Arc::from("hello ")).await;
        buffer.push(Arc::from("world")).await;

        assert!(!buffer.is_empty().await);
        assert_eq!(buffer.take().await, "hello world");
        assert!(buffer.is_empty().await);
    }

    #[tokio::test]
    async fn capacity_truncation() {
        let buffer = OutputBuffer::new(10);

        buffer.push(Arc::from("hello ")).await;
        assert_eq!(buffer.truncated_bytes.load(Ordering::Relaxed), 0);

        buffer.push(Arc::from("world!")).await;

        assert_eq!(buffer.truncated_bytes.load(Ordering::Relaxed), 6);
        assert_eq!(buffer.take().await, "world!");
    }

    #[tokio::test]
    async fn notify_wakes_waiter() {
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
}