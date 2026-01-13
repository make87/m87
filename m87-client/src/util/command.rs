use anyhow::{anyhow, Context, Result};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::{process::Output, time::Duration};
use tokio::process::Command;
use tokio::time::{timeout, Duration as TokioDuration};

/// Get the canonicalized path to the current executable.
///
/// This resolves symlinks and returns the absolute path, useful for
/// situations where we need to reference ourselves (e.g., systemd services,
/// SSH ProxyCommand).
pub fn current_exe_path() -> Result<PathBuf> {
    std::env::current_exe()
        .context("Failed to get current executable path")?
        .canonicalize()
        .context("Failed to canonicalize executable path")
}

pub async fn safe_run_command(mut cmd: Command, timeout_duration: Duration) -> Result<Output> {
    let timeout_duration = TokioDuration::from_secs(timeout_duration.as_secs());
    let result = timeout(timeout_duration, cmd.output()).await;

    match result {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(err)) => Err(anyhow!("I/O error while running command: {}", err)),
        Err(_) => {
            // Timeout occurred
            Err(anyhow!("Command timed out"))
        }
    }
}

pub fn binary_exists(name: &str) -> bool {
    if name.contains('/') {
        return fs::metadata(name).is_ok();
    }

    if let Ok(path) = env::var("PATH") {
        for dir in path.split(':') {
            let mut p = PathBuf::from(dir);
            p.push(name);
            if fs::metadata(&p).is_ok() {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_exists_absolute_path() {
        // /bin/sh should exist on all Unix systems
        assert!(binary_exists("/bin/sh"));
    }

    #[test]
    fn test_binary_exists_in_path() {
        // "sh" should be found via PATH search
        assert!(binary_exists("sh"));
    }

    #[test]
    fn test_binary_exists_not_found() {
        assert!(!binary_exists("nonexistent_binary_xyz_123"));
    }

    #[test]
    fn test_binary_exists_absolute_not_found() {
        assert!(!binary_exists("/nonexistent/path/to/binary"));
    }
}
