use anyhow::{Context, Result, anyhow};
use m87_shared::deploy_spec::CommandSpec;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::{collections::BTreeMap, fmt};
use std::{process::Output, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Child;
use tokio::process::Command;
use tokio::time::{Duration as TokioDuration, timeout};

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

pub fn build_command(cmd: &CommandSpec) -> Result<Command> {
    match cmd {
        CommandSpec::Argv(argv) => {
            let (p, args) = argv.split_first().ok_or_else(|| anyhow!("empty argv"))?;
            let mut c = Command::new(p);
            c.args(args);
            Ok(c)
        }
        CommandSpec::Sh(script) => {
            // Linux-only: /bin/sh -lc
            let sh = if Path::new("/bin/sh").exists() {
                "/bin/sh"
            } else if Path::new("/usr/bin/sh").exists() {
                "/usr/bin/sh"
            } else {
                return Err(anyhow!("no sh found at /bin/sh or /usr/bin/sh"));
            };
            let mut c = Command::new(sh);
            c.arg("-lc").arg(script);
            Ok(c)
        }
    }
}

#[derive(Debug)]
pub struct CommandFailed {
    pub run_id: String,
    pub exit_code: Option<i32>, // None => killed by signal on unix or unknown
    pub timed_out: bool,
    pub stdout_tail: String,
    pub stderr_tail: String,
    pub combined_tail: String,
    pub error: Option<String>,
}

impl fmt::Display for CommandFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = self
            .exit_code
            .map(|c| c.to_string())
            .unwrap_or_else(|| "unknown".into());
        if self.timed_out {
            write!(f, "unit {}/command timed out (exit={})", self.run_id, code)?;
        } else {
            write!(f, "unit {}/command failed (exit={})", self.run_id, code)?;
        }

        // Keep this short-ish; the full tails are still accessible via Debug.
        if !self.combined_tail.is_empty() {
            write!(f, "\n--- tail ---\n{}", self.combined_tail)?;
        }
        Ok(())
    }
}

impl std::error::Error for CommandFailed {}

#[derive(Debug)]
pub enum RunCommandError {
    Failed(CommandFailed),
    Other(anyhow::Error),
    Io(std::io::Error),
}

impl std::fmt::Display for RunCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunCommandError::Failed(e) => write!(f, "{e}"),
            RunCommandError::Io(e) => write!(f, "io error: {e}"),
            RunCommandError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RunCommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RunCommandError::Failed(e) => Some(e),
            RunCommandError::Io(e) => Some(e),
            RunCommandError::Other(e) => Some(e.as_ref()),
        }
    }
}

/// Keep only last `limit_bytes` of whatever we read.
fn push_bounded(buf: &mut Vec<u8>, chunk: &[u8], limit_bytes: usize) {
    if limit_bytes == 0 {
        return;
    }
    buf.extend_from_slice(chunk);
    if buf.len() > limit_bytes {
        let overflow = buf.len() - limit_bytes;
        buf.drain(0..overflow);
    }
}

async fn read_to_tail<R: AsyncRead + Unpin>(
    mut r: R,
    limit_bytes: usize,
) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(limit_bytes.min(64 * 1024));
    let mut tmp = [0u8; 8192];

    loop {
        let n = r.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        push_bounded(&mut out, &tmp[..n], limit_bytes);
    }
    Ok(out)
}

pub async fn run_command(
    run_id: &str,
    wd: &Path,
    env: &BTreeMap<String, String>,
    cmd: &CommandSpec,
    timeout_dur: Option<Duration>,
    tail_bytes: usize, // keep last X bytes of stdout and stderr
) -> Result<String, RunCommandError> {
    let mut c: Command = build_command(cmd).map_err(RunCommandError::Other)?;
    c.current_dir(wd);
    for (k, v) in env {
        c.env(k, v);
    }

    c.stdout(Stdio::piped());
    c.stderr(Stdio::piped());

    let mut child: Child = c
        .spawn()
        .with_context(|| format!("spawn failed for unit {run_id}"))
        .map_err(RunCommandError::Other)?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| RunCommandError::Other(anyhow!("stdout missing")))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| RunCommandError::Other(anyhow!("stderr missing")))?;

    // Read both streams concurrently while the process runs.
    let stdout_task = tokio::spawn(async move { read_to_tail(stdout, tail_bytes).await });
    let stderr_task = tokio::spawn(async move { read_to_tail(stderr, tail_bytes).await });

    let mut timed_out = false;

    let wait_fut = async {
        child
            .wait()
            .await
            .with_context(|| format!("wait failed for unit {run_id}"))
    };

    // On timeout we kill + reap and treat as failure with exit_code = None.
    let status_opt = if let Some(t) = timeout_dur {
        match tokio::time::timeout(t, wait_fut).await {
            Ok(res) => Some(res.map_err(RunCommandError::Other)?),
            Err(_) => {
                timed_out = true;
                let _ = child.kill().await;
                let _ = child.wait().await;
                None
            }
        }
    } else {
        Some(wait_fut.await.map_err(RunCommandError::Other)?)
    };

    // Join readers (they should finish once pipes close after process exits/killed).
    let stdout_tail = stdout_task
        .await
        .context("join stdout reader")
        .map_err(RunCommandError::Other)?
        .map_err(RunCommandError::Io)?;

    let stderr_tail = stderr_task
        .await
        .context("join stderr reader")
        .map_err(RunCommandError::Other)?
        .map_err(RunCommandError::Io)?;

    let stdout_tail_s = String::from_utf8_lossy(&stdout_tail).to_string();
    let stderr_tail_s = String::from_utf8_lossy(&stderr_tail).to_string();

    if !timed_out {
        if let Some(status) = status_opt.as_ref() {
            if status.success() {
                let mut combined = String::new();
                if !stdout_tail_s.is_empty() {
                    combined.push_str(&stdout_tail_s);
                }
                if !stderr_tail_s.is_empty() {
                    if !combined.is_empty() && !combined.ends_with('\n') {
                        combined.push('\n');
                    }
                    combined.push_str(&stderr_tail_s);
                }
                return Ok(combined);
            }
        }
    }

    let exit_code = if timed_out {
        None
    } else {
        status_opt.as_ref().and_then(|s| s.code())
    };

    // Combined tail for display (bounded).
    let mut combined_bytes = Vec::new();
    let combined_limit = tail_bytes.saturating_mul(2);

    push_bounded(
        &mut combined_bytes,
        stdout_tail_s.as_bytes(),
        combined_limit,
    );
    if !combined_bytes.is_empty() && !combined_bytes.ends_with(b"\n") {
        push_bounded(&mut combined_bytes, b"\n", combined_limit);
    }
    push_bounded(
        &mut combined_bytes,
        stderr_tail_s.as_bytes(),
        combined_limit,
    );

    let combined_tail = String::from_utf8_lossy(&combined_bytes).to_string();
    let error = if timed_out {
        Some(format!("Command timed out"))
    } else {
        Some(format!(
            "Command failed with exit code {}",
            exit_code.unwrap_or(-1)
        ))
    };

    Err(RunCommandError::Failed(CommandFailed {
        run_id: run_id.to_string(),
        exit_code,
        timed_out,
        stdout_tail: stdout_tail_s,
        stderr_tail: stderr_tail_s,
        combined_tail,
        error,
    }))
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
