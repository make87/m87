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

    #[cfg(test)]
    pub(crate) fn get_program(&self) -> &str {
        &self.program
    }

    #[cfg(test)]
    pub(crate) fn get_args(&self) -> &[String] {
        &self.args
    }

    #[cfg(test)]
    pub(crate) fn get_env(&self) -> &[(String, String)] {
        &self.env
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_new() {
        let builder = SubprocessBuilder::new("my-program");
        assert_eq!(builder.get_program(), "my-program");
        assert!(builder.get_args().is_empty());
        assert!(builder.get_env().is_empty());
    }

    #[test]
    fn test_builder_new_from_string() {
        let builder = SubprocessBuilder::new(String::from("another-program"));
        assert_eq!(builder.get_program(), "another-program");
    }

    #[test]
    fn test_builder_args() {
        let builder = SubprocessBuilder::new("cmd").args(["--flag", "-v", "value"]);
        assert_eq!(builder.get_args(), &["--flag", "-v", "value"]);
    }

    #[test]
    fn test_builder_args_from_vec() {
        let args = vec!["arg1".to_string(), "arg2".to_string()];
        let builder = SubprocessBuilder::new("cmd").args(args);
        assert_eq!(builder.get_args(), &["arg1", "arg2"]);
    }

    #[test]
    fn test_builder_env_single() {
        let builder = SubprocessBuilder::new("cmd").env("KEY", "VALUE");
        assert_eq!(builder.get_env(), &[("KEY".to_string(), "VALUE".to_string())]);
    }

    #[test]
    fn test_builder_env_multiple() {
        let builder = SubprocessBuilder::new("cmd")
            .env("KEY1", "VALUE1")
            .env("KEY2", "VALUE2");
        assert_eq!(builder.get_env().len(), 2);
        assert_eq!(builder.get_env()[0], ("KEY1".to_string(), "VALUE1".to_string()));
        assert_eq!(builder.get_env()[1], ("KEY2".to_string(), "VALUE2".to_string()));
    }

    #[test]
    fn test_builder_fluent_chain() {
        let builder = SubprocessBuilder::new("docker")
            .args(["run", "-it", "ubuntu"])
            .env("DOCKER_HOST", "tcp://localhost:2375")
            .env("DEBUG", "1");

        assert_eq!(builder.get_program(), "docker");
        assert_eq!(builder.get_args(), &["run", "-it", "ubuntu"]);
        assert_eq!(builder.get_env().len(), 2);
    }
}
