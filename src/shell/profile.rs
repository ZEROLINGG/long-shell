//! `ShellProfile`：把各个具体 shell（bash/zsh/cmd/...）的"特性"
//! （启动参数 / 初始化命令 / 退出命令）集中到一处，避免这三件事
//! 分散在三个不同的 `match` 语句里，新增一种 shell 支持时容易漏改。

use anyhow::Result;

use crate::util::normalize_shell_name;

#[derive(Debug, Clone)]
pub(crate) struct ShellProfile {
    name: String,
}

impl ShellProfile {
    pub fn detect(shell_path: &str) -> Result<Self> {
        Ok(Self {
            name: normalize_shell_name(shell_path)?,
        })
    }

    #[allow(unused)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 各 shell 的启动参数。`pty_mode` 用于区分：
    /// - 管道模式下 bash/zsh/sh 使用 `-s`（从 stdin 读脚本，非交互，干净无 prompt）；
    /// - PTY 模式下改用 `-i`（强制交互），以获得 job control、彩色 prompt 等
    ///   完整终端体验——这正是使用 PTY 的意义所在。
    pub fn args(&self, pty_mode: bool) -> Result<Vec<&'static str>> {
        Ok(match self.name.as_str() {
            "bash" => {
                if pty_mode {
                    vec!["--norc", "--noprofile", "-i"]
                } else {
                    vec!["--norc", "--noprofile", "-s"]
                }
            }
            "zsh" => {
                if pty_mode {
                    vec!["-f", "-i"]
                } else {
                    vec!["-f", "-s"]
                }
            }
            "sh" => vec![if pty_mode { "-i" } else { "-s" }],
            "fish" => vec!["--no-config", "-i"],
            "cmd" => vec!["/Q", "/K", "prompt $G"],
            "powershell" | "pwsh" => {
                vec!["-ExecutionPolicy", "Bypass", "-NoExit", "-NoProfile"]
            }
            "python" => vec!["-u", "-i"],
            "node" => vec!["-i"],
            other => anyhow::bail!("unsupported shell: {other}"),
        })
    }

    pub fn init_command(&self) -> Option<String> {
        match self.name.as_str() {
            "cmd" => Some("chcp 65001 >nul 2>&1\r\n".into()),
            "powershell" | "pwsh" => Some(
                "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; \
                 [Console]::InputEncoding  = [System.Text.Encoding]::UTF8; \
                 $OutputEncoding           = [System.Text.Encoding]::UTF8\n"
                    .into(),
            ),
            _ => None,
        }
    }

    pub fn exit_command(&self) -> String {
        match self.name.as_str() {
            "python" => "quit()\n".into(),
            "node" => ".exit\n".into(),
            _ => "exit\n".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bash_args_differ_between_pipe_and_pty() {
        let p = ShellProfile::detect("/bin/bash").unwrap();
        assert_eq!(p.args(false).unwrap(), vec!["--norc", "--noprofile", "-s"]);
        assert_eq!(p.args(true).unwrap(), vec!["--norc", "--noprofile", "-i"]);
    }

    #[test]
    fn unsupported_shell_errors() {
        let p = ShellProfile::detect("/bin/nu").unwrap();
        assert!(p.args(false).is_err());
    }
}