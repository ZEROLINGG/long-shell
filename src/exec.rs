use std::borrow::Cow;
use std::time::Duration;
use std::process::Stdio;
use anyhow::{anyhow, bail, ensure, Result};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use crate::util::{decode_bytes, normalize_shell_name};

#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x08000000;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

impl ExecResult {
    pub fn ok(self) -> Result<String> {
        if self.exit_code == 0 { return Ok(self.stdout) }
        bail!(self.stderr)
    }

    pub fn success(&self) -> bool { self.exit_code == 0 }

    pub fn failed(&self) -> bool { !self.success() }
}

pub async fn exec<'a>(
    input: impl Into<Cow<'a, str>>,
    shell: impl Into<Cow<'a, str>>,
    timeout_dur: Option<Duration>,
) -> Result<ExecResult> {
    let shell = shell.into();
    let shell = shell.trim();
    ensure!(!shell.is_empty(), "shell path cannot be empty");

    let shell_name = normalize_shell_name(&shell)?;
    let input_ref = input.into();

    let mut cmd = build_exec_command(&shell, &shell_name, input_ref.as_ref())?;

    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    #[cfg(target_os = "windows")]
    {
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    let output = if let Some(dur) = timeout_dur {
        tokio::time::timeout(dur, cmd.spawn()?.wait_with_output())
            .await
            .map_err(|_| anyhow!("command timed out after {:?} ", dur))??
    } else {
        cmd.spawn()?.wait_with_output().await?
    };

    Ok(to_exec_result(output))
}

fn build_exec_command(shell: &str, shell_name: &str, input: &str) -> Result<Command> {
    let mut cmd = Command::new(shell);

    match shell_name {
        "sh" | "zsh" | "bash" | "fish" => {
            cmd.args(["-c", input]);
        }
        "node" => {
            cmd.args(["-e", input]);
        }
        "python" => {
            #[cfg(target_os = "windows")]
            cmd.env("PYTHONUTF8", "1");

            cmd.args(["-c", input]);
        }
        "cmd" => {
            #[cfg(not(target_os = "windows"))]
            cmd.args(["/C", input]);

            #[cfg(target_os = "windows")]
            {
                let wrapped = format!("{} {}", "chcp 65001 >nul 2>&1 & ", input);
                cmd.args(["/C", wrapped.as_str()]);
            }
        }
        "powershell" | "pwsh" => {
            #[cfg(not(target_os = "windows"))]
            cmd.args(["-ep", "Bypass", "-nop", "-c", input]);

            #[cfg(target_os = "windows")]
            {
                let wrapped = format!(
                    "{}{}",
                    "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8;
                 [Console]::InputEncoding  = [System.Text.Encoding]::UTF8;
                 $OutputEncoding           = [System.Text.Encoding]::UTF8;
                 ",
                    input
                );
                cmd.args(["-ep", "Bypass", "-nop", "-c", wrapped.as_str()]);
            }
        }
        _ => {
            bail!("unsupported shell: {}", shell_name);
        }
    }

    Ok(cmd)
}

fn to_exec_result(output: std::process::Output) -> ExecResult {
    ExecResult {
        stdout: decode_bytes(&output.stdout),
        stderr: decode_bytes(&output.stderr),
        exit_code: output.status.code().unwrap_or(-1),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::*;

    #[cfg(unix)]
    async fn test_shell_exec(shell: &str) {
        let out = exec("ls", shell, None).await.unwrap().stdout;
        assert!(out.contains("Cargo.toml"));
        println!("{}", out);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_exec() {
        let shells = vec!["zsh", "bash", "sh"];
        for shell in shells {
            test_shell_exec(shell).await;
        }
    }
}
