//! Runtime run fixture for E2E tests

use crate::e2e_containers::containers::E2EInfra;
use crate::e2e_containers::helpers::{
    exec_background, log_contains, read_log, wait_for, E2EError, WaitConfig,
};

/// Builder for starting runtime run with explicit steps
pub struct RuntimeRunner<'a> {
    infra: &'a E2EInfra,
    args: Vec<String>,
}

impl<'a> RuntimeRunner<'a> {
    /// Create a new runtime runner builder
    pub fn new(infra: &'a E2EInfra) -> Self {
        Self {
            infra,
            args: vec![],
        }
    }

    /// Add extra arguments to the runtime run command
    pub fn with_args(mut self, args: &[&str]) -> Self {
        self.args = args.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Step 1: Start runtime run process (runs in background)
    pub async fn start_run(&self) -> Result<(), E2EError> {
        let args_str = if self.args.is_empty() {
            String::new()
        } else {
            format!(" {}", self.args.join(" "))
        };
        let cmd = format!("m87 runtime run{}", args_str);
        tracing::info!("Starting runtime run with command: {}", cmd);
        exec_background(&self.infra.runtime, &cmd, "/tmp/runtime-run.log").await
    }

    /// Step 2: Wait for control tunnel to establish
    pub async fn wait_for_control_tunnel(&self) -> Result<(), E2EError> {
        tracing::info!("Waiting for control tunnel to establish...");

        let runtime = &self.infra.runtime;

        wait_for(
            WaitConfig::with_description("control tunnel")
                .max_attempts(30)
                .interval(std::time::Duration::from_secs(2)),
            || async {
                // Check if "Starting control tunnel" appears in log
                let has_started = log_contains(runtime, "/tmp/runtime-run.log", "Starting control tunnel")
                    .await
                    .unwrap_or(false);

                if !has_started {
                    return false;
                }

                // Make sure it hasn't crashed
                let has_crashed = log_contains(runtime, "/tmp/runtime-run.log", "Control tunnel crashed")
                    .await
                    .unwrap_or(false);

                !has_crashed
            },
        )
        .await?;

        // Double-check for crash after the wait
        let log = self.get_log().await?;
        if log.contains("Control tunnel crashed") {
            return Err(E2EError::RuntimeCrashed(log));
        }

        tracing::info!("Control tunnel established");
        Ok(())
    }

    /// Get runtime run log
    pub async fn get_log(&self) -> Result<String, E2EError> {
        read_log(&self.infra.runtime, "/tmp/runtime-run.log").await
    }

    /// Convenience: Start and wait for control tunnel
    pub async fn start_with_tunnel(&self) -> Result<(), E2EError> {
        self.start_run().await?;
        self.wait_for_control_tunnel().await
    }
}
