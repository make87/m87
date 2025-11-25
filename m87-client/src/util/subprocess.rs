//! Subprocess execution with proper signal handling using tokio.
//!
//! Uses tokio::process for async child management. The child shares
//! the terminal's process group, so it receives SIGINT directly from
//! the terminal when user presses Ctrl+C.

use anyhow::{Context, Result};
use std::process::Stdio;
use tokio::process::Command;

/// Builder for running external commands.
pub struct SubprocessBuilder {
    program: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
}

impl SubprocessBuilder {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
        }
    }

    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Run the command and wait for it to complete.
    /// The child process receives terminal signals (SIGINT, etc.) directly.
    /// This function does NOT return on success - it calls std::process::exit().
    pub async fn exec(self) -> Result<()> {
        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(false); // Let child handle its own signals

        for (k, v) in &self.env {
            cmd.env(k, v);
        }

        let mut child = cmd
            .spawn()
            .with_context(|| format!("Failed to spawn {}", self.program))?;

        // Simply wait for child to exit.
        // Child receives SIGINT directly from terminal (same process group).
        // No signal forwarding needed
        let status = child.wait().await?;

        std::process::exit(status.code().unwrap_or(1));
    }
}
