# long-shell

A persistent, long-lived shell session manager for async Rust. Spawn a shell once, then interact with it across multiple commands — state, environment variables, and working directory persist.

```rust
use long_shell::shell::Shell;

let mut sh = Shell::new("bash")
    .enable_buffer()
    .line_callback()
    .spawn()
    .await?;

sh.send_line("export FOO=bar").await?;
sh.send_line("echo $FOO").await?;
let out = sh.output(None).await;
assert_eq!(out.trim(), "bar");
```

## Features

- **Persistent process** — One spawn, many commands; env, cwd, and shell state survive between calls
- **Two output modes** — Raw (lowest latency, per-chunk) or Line (buffered per-line with idle flush)
- **Bounded buffering** — Configurable-capacity `OutputBuffer` with overflow truncation tracking
- **Async callbacks** — Hooks for stdout, stderr, exit, close, and pre-send filtering
- **Lifecycle control** — `send`, `reset`, `exit` (graceful), `close` (immediate), `join`, and auto-close on drop
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
long-shell = "0.1"
```

## Usage

### Persistent Shell

```rust
use long_shell::shell::{Shell, CallbackMode};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut shell = Shell::new("bash")
        .enable_buffer()              // Buffer stdout/stderr (default 4 MiB)
        .line_callback()              // Callbacks receive complete lines
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
    let output = shell.output(None).await;
    println!("Output: {output}");

    shell.send_line("echo done").await?;
    let output = shell.output(Some(std::time::Duration::from_millis(500))).await;
    println!("Output: {output}");

    // Reset: kill the process and spawn a fresh one (buffers + callbacks preserved)
    shell.reset().await?;

    shell.exit().await?;
    Ok(())
}
```

### One-Shot Execution

For commands where persistent state isn't needed:

```rust
use long_shell::exec::{exec, ExecResult};
use std::time::Duration;

let result: ExecResult = exec("echo hello", "bash", Some(Duration::from_secs(5))).await?;
assert_eq!(result.stdout.trim(), "hello");
assert!(result.success());

// Unwrap stdout on success, or get stderr on failure
let output: anyhow::Result<String> = result.ok();
```

### Global Singletons

```rust
// Unix: shared global bash instance
#[cfg(unix)]
{
    let bash = long_shell::shell::bash().await?;
    let mut sh = bash.lock().await;
    sh.send_line("echo hello").await?;
}

// Windows: shared global powershell instance
#[cfg(windows)]
{
    let ps = long_shell::shell::powershell().await?;
    let mut sh = ps.lock().await;
    sh.send_line("Write-Output hello").await?;
}
```

### Output Buffer

```rust
use long_shell::shell::OutputBuffer;

let buf = OutputBuffer::new(1024 * 1024); // 1 MiB
buf.push("line 1\n".into()).await;

// Wait for new data
buf.notify.notified().await;
let content = buf.take().await;

// Track overflow
let lost = buf.truncated_bytes.load(std::sync::atomic::Ordering::Relaxed);
```

## API Overview

### `shell` module

| Type | Description |
|------|-------------|
| `Shell` | Live handle to a persistent shell process |
| `ShellBuilder` | Fluent builder for configuring and spawning a `Shell` |
| `OutputBuffer` | Bounded, async-concurrent output accumulator |
| `CallbackMode` | `Raw` (per-chunk) or `Line` (per-line) callback mode |

**`Shell` lifecycle methods:** `send`, `send_line`, `send_control_char`, `send_eof`, `reset`, `exit`, `close`, `join`, `join_exit`

**`Shell` output methods:** `output`, `output_error`, `output_truncated_bytes`, `error_truncated_bytes`

**`ShellBuilder` hooks:** `on_output`, `on_error`, `on_exit`, `on_close`, `on_send`

**`OutputBuffer` methods:** `new`, `push`, `take`, `is_empty`

### `exec` module

| Type | Description |
|------|-------------|
| `exec()` | Run a one-shot command with optional timeout |
| `ExecResult` | Result with `stdout`, `stderr`, `exit_code`, `ok()`, `success()`, `failed()` |

## License

MIT © 2026