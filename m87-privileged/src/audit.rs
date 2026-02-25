use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

pub struct AuditLogger {
    file: Mutex<tokio::fs::File>,
}

impl AuditLogger {
    pub async fn new(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir)
                .await
                .with_context(|| format!("failed to create audit log directory: {}", dir.display()))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .with_context(|| format!("failed to open audit log: {}", path.display()))?;

        Ok(Self {
            file: Mutex::new(file),
        })
    }

    async fn write_line(&self, event: &str, request_id: &str, details: &str) {
        let line = format!("{} {} {} {}\n", Utc::now().to_rfc3339(), event, request_id, details);
        let mut f = self.file.lock().await;
        if let Err(e) = f.write_all(line.as_bytes()).await {
            tracing::error!("failed to write audit log: {e}");
        }
    }

    pub async fn log_request(&self, request_id: &str, argv: &[String]) {
        self.write_line("REQUEST", request_id, &argv.join(" ")).await;
    }

    pub async fn log_policy_hit(&self, request_id: &str, pattern: &str) {
        self.write_line("POLICY_HIT", request_id, pattern).await;
    }

    pub async fn log_policy_miss(&self, request_id: &str) {
        self.write_line("POLICY_MISS", request_id, "").await;
    }

    pub async fn log_approval(&self, request_id: &str, decision: &str) {
        self.write_line("APPROVAL", request_id, decision).await;
    }

    pub async fn log_exec(&self, request_id: &str, exit_code: i32) {
        self.write_line("EXEC", request_id, &format!("exit_code={exit_code}"))
            .await;
    }

    pub async fn log_denied(&self, request_id: &str, reason: &str) {
        self.write_line("DENIED", request_id, reason).await;
    }
}
