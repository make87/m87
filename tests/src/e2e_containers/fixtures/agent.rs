//! Agent run fixture for E2E tests

use crate::e2e_containers::containers::E2EInfra;
use crate::e2e_containers::helpers::{
    exec_background, log_contains, read_log, wait_for, E2EError, WaitConfig,
};

/// Builder for starting agent run with explicit steps
pub struct AgentRunner<'a> {
    infra: &'a E2EInfra,
    args: Vec<String>,
}

impl<'a> AgentRunner<'a> {
    /// Create a new agent runner builder
    pub fn new(infra: &'a E2EInfra) -> Self {
        Self {
            infra,
            args: vec![],
        }
    }

    /// Add extra arguments to the agent run command
    pub fn with_args(mut self, args: &[&str]) -> Self {
        self.args = args.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Step 1: Start agent run process (runs in background)
    pub async fn start_run(&self) -> Result<(), E2EError> {
        let args_str = if self.args.is_empty() {
            String::new()
        } else {
            format!(" {}", self.args.join(" "))
        };
        let cmd = format!("m87 agent run{}", args_str);
        tracing::info!("Starting agent run with command: {}", cmd);
        exec_background(&self.infra.agent, &cmd, "/tmp/agent-run.log").await
    }

    /// Step 2: Wait for control tunnel to establish
    pub async fn wait_for_control_tunnel(&self) -> Result<(), E2EError> {
        tracing::info!("Waiting for control tunnel to establish...");

        let agent = &self.infra.agent;

        wait_for(
            WaitConfig::with_description("control tunnel")
                .max_attempts(30)
                .interval(std::time::Duration::from_secs(2)),
            || async {
                // Check if "Starting control tunnel" appears in log
                let has_started = log_contains(agent, "/tmp/agent-run.log", "Starting control tunnel")
                    .await
                    .unwrap_or(false);

                if !has_started {
                    return false;
                }

                // Make sure it hasn't crashed
                let has_crashed = log_contains(agent, "/tmp/agent-run.log", "Control tunnel crashed")
                    .await
                    .unwrap_or(false);

                !has_crashed
            },
        )
        .await?;

        // Double-check for crash after the wait
        let log = self.get_log().await?;
        if log.contains("Control tunnel crashed") {
            return Err(E2EError::AgentCrashed(log));
        }

        tracing::info!("Control tunnel established");
        Ok(())
    }

    /// Get agent run log
    pub async fn get_log(&self) -> Result<String, E2EError> {
        read_log(&self.infra.agent, "/tmp/agent-run.log").await
    }

    /// Convenience: Start and wait for control tunnel
    pub async fn start_with_tunnel(&self) -> Result<(), E2EError> {
        self.start_run().await?;
        self.wait_for_control_tunnel().await
    }
}
