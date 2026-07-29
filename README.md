# shell-engine

A persistent, long-lived shell session manager for async Rust. Spawn a shell once, then interact with it across multiple commands — state, environment variables, and working directory persist.

```rust
use shell_engine::Shell;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut sh = Shell::new("bash")
        .enable_buffer()
        .spawn()
        .await?;

    sh.send_line("export FOO=bar").await?;
    sh.send_line("echo $FOO").await?;
    let out = sh.output(None, None).await;
    assert_eq!(out.stdout.trim(), "bar");
    sh.exit().await?;
    Ok(())
}
```

## Features

- **Persistent process** — One spawn, many commands; env, cwd, and shell state survive between calls
- **Two backends** — Pipe mode (default, clean I/O separation) and PTY mode (optional, for full terminal apps, job control, colored output)
- **Two callback modes** — Raw (lowest latency, per-chunk) or Line (buffered per-line with 80 ms idle flush for interactive prompts)
- **Bounded buffering** — Configurable-capacity `OutputBuffer` with overflow truncation tracking
- **Async callbacks** — Hooks for stdout, stderr, exit, close, and pre-send filtering
- **Lifecycle control** — `send`, `send_line`, `send_control_char`, `send_keys`, `send_eof`, `reset`, `exit`, `close`, `join_close`, `join_exit`, and auto-close on drop
- **Control characters** — `send("^C")` auto-detects `^@`–`^_` and `^?` syntax; PTY mode supports full `^A`–`^_` and `^?`, pipe mode supports `^R` (reset) and `^D` (EOF)
- **Screen snapshot** — PTY mode captures rendered terminal content via `vt100` parser (`output_snapshot`)
- **Cross-platform** — Unix (bash, zsh, sh, fish, python, node) and Windows (cmd, powershell, pwsh, python, node)
- **Encoding support** — Stateful incremental decoder; auto-detects Windows code page
- **One-shot execution** — `exec()` for fire-and-forget commands with optional timeout

## Supported Shells

| Shell | Unix | Windows |
|-------|------|---------|
| bash | ✓ | |
| zsh | ✓ | |
| sh | ✓ | |
| fish | ✓ | |
| cmd | | ✓ |
| powershell / pwsh | | ✓ |
| python | ✓ | ✓ |
| node | ✓ | ✓ |

## Installation

```toml
[dependencies]
shell-engine = "0.2.0"
```

PTY support (via `rust-pty` + `vt100`) is enabled by default. To disable it:

```toml
[dependencies]
shell-engine = { version = "0.2.0", default-features = false }
```

## Usage

### Persistent Shell

```rust
use shell_engine::Shell;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut shell = Shell::new("bash")
        .enable_buffer()              // Buffer stdout/stderr (default 4 MiB)
        .line_callback()              // Complete lines; partial flushed after 80 ms idle
        .on_output(|line| async move {
            println!("[stdout] {line}");
        })
        .on_error(|line| async move {
            eprintln!("[stderr] {line}");
        })
        .on_exit(|code| async move {
            println!("Shell exited with code: {code:?}");
        })
        .on_close(|| async move {
            println!("Shell closed");
        })
        .on_send(|cmd| async move {
            // Filter or transform commands before they reach the shell
            if cmd.contains("rm -rf") {
                return None; // Block dangerous commands
            }
            Some(cmd)
        })
        .spawn()
        .await?;

    shell.send_line("ls -la").await?;
    let output = shell.output(None, None).await;
    println!("stdout: {}", output.stdout);

    shell.send_line("echo done").await?;
    let output = shell.output(Some(Duration::from_millis(500)), None).await;
    println!("stdout: {}", output.stdout);

    // Reset: kill the process and spawn a fresh one (buffers + callbacks preserved)
    shell.reset().await?;

    shell.exit().await?;
    Ok(())
}
```

### One-Shot Execution

For commands where persistent state isn't needed:

```rust
use shell_engine::exec::{exec, ExecResult};
use std::time::Duration;

let result: ExecResult = exec("echo hello", "bash", Some(Duration::from_secs(5))).await?;
assert_eq!(result.stdout.trim(), "hello");
assert!(result.success());

// Unwrap stdout on success, or get stderr on failure
let output: anyhow::Result<String> = result.ok();
```

### PTY Mode

For full terminal applications (editors, `htop`, `screen`, etc.), enable the pseudo-terminal backend.
**Note:** In PTY mode stdout and stderr are merged into a single stream — `on_error` callbacks will never fire and `error_truncated_bytes` is always 0.

```rust
use shell_engine::Shell;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut shell = Shell::new("bash")
        .enable_pty()                     // Use PTY instead of pipes
        .enable_buffer()
        .spawn()
        .await?;

    shell.send_line("vim").await?;

    // Wait for output and capture a rendered screen snapshot
    let snap = shell.output_snapshot(Some(Duration::from_millis(500)), None).await?;
    println!("Screen: {snap}");

    // Send control characters (fully supported in PTY mode)
    shell.send_control_char('C').await?;  // ^C interrupt

    // Adjust terminal window size
    shell.resize(120, 40).await?;

    shell.exit().await?;
    Ok(())
}
```

### Global Singletons

```rust
// Unix: shared global bash instance
#[cfg(unix)]
{
    let bash = shell_engine::bash().await?;
    let mut sh = bash.lock().await;
    sh.send_line("echo hello").await?;
}

// Windows: shared global powershell instance
#[cfg(windows)]
{
    let ps = shell_engine::powershell().await?;
    let mut sh = ps.lock().await;
    sh.send_line("Write-Output hello").await?;
}
```

### Output Buffer

```rust
use shell_engine::OutputBuffer;

let buf = OutputBuffer::new(1024 * 1024); // 1 MiB
buf.push("line 1\n".into()).await;

// Wait for new data (note: ensure notified() is called before push to avoid race)
buf.notify.notified().await;
let content = buf.take().await;

// Track overflow
let lost = buf.truncated_bytes.load(std::sync::atomic::Ordering::Relaxed);
```

## API Overview

All key types are re-exported at the crate root: `use shell_engine::{Shell, ShellBuilder, ShellOutput, OutputBuffer, CallbackMode};`

### `shell` module

| Type | Description |
|------|-------------|
| `Shell` | Live handle to a persistent shell process |
| `ShellBuilder` | Fluent builder for configuring and spawning a `Shell` |
| `ShellOutput` | Result struct containing `stdout` and `stderr` fields; provides `is_empty()` |
| `OutputBuffer` | Bounded, async-concurrent output accumulator |
| `CallbackMode` | `Raw` (per-chunk) or `Line` (per-line) callback mode |

**`Shell` lifecycle methods:** `send` (auto-detects `"^C"` syntax; routes to `send_control_char`), `send_line`, `send_keys` (send `Key::SpecialKey`/`Key::Char` sequences, e.g. direction keys, F1–F12), `send_control_char` (PTY: full `^A`–`^_` + `^?`; pipe: `^R` reset, `^D` EOF), `send_eof`, `reset`, `exit`, `close`, `join_close`, `join_exit`

**`Shell` output methods:** `output(idle, max_wait)`, `output_until(pattern, timeout)`

**`Shell` stats:** `output_truncated_bytes`, `error_truncated_bytes` (always 0 in PTY mode)

**`Shell` PTY methods (requires `pty` feature):** `output_snapshot`, `screen_clone`, `resize`, `is_pty`, `pty_window_size`, `send_signal`, `cursor_position`, `move_cursor_to`

**`ShellBuilder` config:** `enable_buffer`, `enable_buffer_with_capacity`, `line_callback`, `raw_callback`

**`ShellBuilder` PTY config (requires `pty` feature):** `enable_pty`, `pty_size`, `scrollback`, `disable_snapshot` (saves CPU/memory; `output_snapshot`/`screen_clone`/`cursor_position` will error)

**`ShellBuilder` hooks:** `on_output`, `on_error`, `on_exit`, `on_close`, `on_send`

**`OutputBuffer` methods:** `new`, `push`, `take`, `is_empty`

**`OutputBuffer` public fields:** `notify` (waker for new data), `truncated_bytes` (overflow counter)

### Crate root re-exports

| Re-export | Feature gate | Description |
|-----------|-------------|-------------|
| `shell_engine::vt100` | `pty` | Re-exported so you can use `shell_engine::vt100::Screen` without adding your own dep |
| `shell_engine::PtySignal` | `pty` | Signal type for `send_signal()` |
| `shell_engine::WindowSize` | `pty` | PTY window size struct |
| `shell_engine::bash()` | unix | Global singleton bash `Arc<Mutex<Shell>>` |
| `shell_engine::powershell()` | windows | Global singleton powershell `Arc<Mutex<Shell>>` |

### `util` module

Low-level helpers: `StreamDecoder` (incremental text decoder), `decode_bytes`, `detect_encoding`.

### `exec` module

| Type | Description |
|------|-------------|
| `exec()` | Run a one-shot command with optional timeout |
| `ExecResult` | Result with `stdout`, `stderr`, `exit_code`, `ok()`, `success()`, `failed()` |

## License

MIT © 2026
