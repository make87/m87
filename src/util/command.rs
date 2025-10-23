use anyhow::{anyhow, Result};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::{process::Output, time::Duration};
use tokio::process::Command;
use tokio::time::{timeout, Duration as TokioDuration};

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
