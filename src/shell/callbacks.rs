//! 回调类型定义与封装。
//!
//! `CallbackHub` 把裸的 `Arc<Mutex<Callbacks>>` 封装成带名字的方法
//! （`fire_exit` / `fire_close` / `fire_output`），避免在 `pty.rs` / `stream.rs`
//! 里到处出现"手动加锁 + 取 Option + 调用"的重复样板代码。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::Mutex;

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(crate) type AsyncPreSendCallback =
Box<dyn FnMut(String) -> BoxFuture<'static, Option<String>> + Send + 'static>;
pub(crate) type AsyncOutputCallback =
Box<dyn FnMut(String) -> BoxFuture<'static, ()> + Send + 'static>;
pub(crate) type AsyncErrorCallback =
Box<dyn FnMut(String) -> BoxFuture<'static, ()> + Send + 'static>;
pub(crate) type AsyncExitCallback =
Box<dyn FnMut(Option<i32>) -> BoxFuture<'static, ()> + Send + 'static>;
pub(crate) type AsyncCloseCallback =
Box<dyn FnMut() -> BoxFuture<'static, ()> + Send + 'static>;

/// 输出回调触发粒度。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CallbackMode {
    /// 有数据就立即回调，粒度为原始读取块，延迟最低。
    #[default]
    Raw,
    /// 以行为单位回调；无换行时依赖空闲超时强制 flush。
    Line,
}

#[derive(Default)]
pub(crate) struct Callbacks {
    pub(crate) on_output: Option<AsyncOutputCallback>,
    pub(crate) on_error: Option<AsyncErrorCallback>,
    pub(crate) on_exit: Option<AsyncExitCallback>,
    pub(crate) on_close: Option<AsyncCloseCallback>,
    pub(crate) mode: CallbackMode,
}

/// `Arc<Mutex<Callbacks>>` 的封装。所有触发回调的地方都通过这里的方法进行，
/// 而不是散落各处的 "lock -> 取 Option -> 调用" 样板代码。
#[derive(Clone)]
pub(crate) struct CallbackHub(Arc<Mutex<Callbacks>>);

impl CallbackHub {
    pub fn new(callbacks: Callbacks) -> Self {
        Self(Arc::new(Mutex::new(callbacks)))
    }

    /// 读取当前回调模式（供 pipe/pty 后端在 spawn 时取一次快照）。
    pub async fn mode(&self) -> CallbackMode {
        self.0.lock().await.mode
    }

    pub async fn fire_exit(&self, code: Option<i32>) {
        let fut = { self.0.lock().await.on_exit.as_mut().map(|f| f(code)) };
        if let Some(fut) = fut {
            fut.await;
        }
    }

    pub async fn fire_close(&self) {
        let fut = { self.0.lock().await.on_close.as_mut().map(|f| f()) };
        if let Some(fut) = fut {
            fut.await;
        }
    }

    /// 触发 `on_output` 或 `on_error`。
    pub async fn fire_output(&self, text: String, is_stderr: bool) {
        let fut = {
            let mut cb = self.0.lock().await;
            if is_stderr {
                cb.on_error.as_mut().map(|f| f(text))
            } else {
                cb.on_output.as_mut().map(|f| f(text))
            }
        };
        if let Some(fut) = fut {
            fut.await;
        }
    }
}

/// `on_send` 命令拦截钩子的封装。
#[derive(Clone)]
pub(crate) struct PreSendHook(Arc<Mutex<Option<AsyncPreSendCallback>>>);

impl PreSendHook {
    pub fn new(f: Option<AsyncPreSendCallback>) -> Self {
        Self(Arc::new(Mutex::new(f)))
    }

    /// 过一遍拦截器；返回 `None` 表示该命令被拦截，不应发送。
    pub async fn process(&self, raw: String) -> Option<String> {
        let mut guard = self.0.lock().await;
        if let Some(f) = guard.as_mut() {
            f(raw).await
        } else {
            Some(raw)
        }
    }
}