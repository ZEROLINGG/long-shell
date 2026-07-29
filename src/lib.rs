

pub mod exec;
pub mod shell;
pub mod util;

#[cfg(feature = "pty")]
mod pty;
mod pipe;

pub use shell::{CallbackMode, OutputBuffer, Shell, ShellBuilder, ShellOutput};

#[cfg(unix)]
pub use shell::bash;
#[cfg(windows)]
pub use shell::powershell;

// 方便使用者在自己代码里直接引用 vt100::Screen / rust_pty::PtySignal 等类型，
// 而不必自行添加依赖并手动对齐版本号。
#[cfg(feature = "pty")]
pub use vt100;
#[cfg(feature = "pty")]
pub use rust_pty::{PtySignal, WindowSize};