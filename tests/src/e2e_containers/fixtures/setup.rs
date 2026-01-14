//! Test setup fixture for E2E tests
//!
//! Provides a convenient way to set up the full test environment:
//! - Initialize infrastructure (MongoDB, server, agent, CLI containers)
//! - Register a device
//! - Setup SNI/tunneling
//! - Start agent with control tunnel

use crate::e2e_containers::containers::E2EInfra;
use crate::e2e_containers::helpers::{exec_shell, E2EError, SniSetup};

use super::{RuntimeRunner, DeviceRegistration, RegisteredDevice};

/// Standard test setup with registered device and running agent
///
/// This encapsulates the common setup pattern used by most device tests:
/// 1. Initialize E2E infrastructure
/// 2. Register device
/// 3. Setup SNI tunneling
/// 4. Start agent with control tunnel
/// 5. Wait for SSH/SFTP readiness
pub struct TestSetup {
    pub infra: E2EInfra,
    pub device: RegisteredDevice,
    pub sni: SniSetup,
}

impl TestSetup {
    /// Initialize full test environment with device and agent running
    pub async fn init() -> Result<Self, E2EError> {
        // 1. Initialize infrastructure
        let infra = E2EInfra::init().await?;

        // 2. Register device
        tracing::info!("Registering device...");
        let device = DeviceRegistration::new(&infra).register_full().await?;
        tracing::info!(
            "Device registered: {} ({})",
            device.name,
            device.short_id
        );

        // 3. Setup SNI tunneling
        tracing::info!("Setting up SNI...");
        let sni = SniSetup::from_cli(&infra.cli).await?;
        sni.setup_both(&infra.runtime, &infra.cli, &device.short_id)
            .await?;

        // 4. Start agent with control tunnel
        tracing::info!("Starting agent run...");
        let runtime = RuntimeRunner::new(&infra);
        runtime.start_with_tunnel().await?;

        // 5. Wait for SSH/SFTP to be ready
        tracing::info!("Waiting for agent SSH server to be ready...");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        Ok(Self { infra, device, sni })
    }

    /// Execute CLI command targeting the registered device
    ///
    /// Example: `setup.device_cmd("exec -- echo hello")`
    /// becomes: `m87 <device_name> exec -- echo hello`
    pub async fn device_cmd(&self, cmd: &str) -> Result<String, E2EError> {
        exec_shell(
            &self.infra.cli,
            &format!("m87 {} {} 2>&1", self.device.name, cmd),
        )
        .await
    }

    /// Execute raw m87 command (not targeting a device)
    ///
    /// Example: `setup.m87_cmd("ls device:path")`
    pub async fn m87_cmd(&self, cmd: &str) -> Result<String, E2EError> {
        exec_shell(&self.infra.cli, &format!("m87 {} 2>&1", cmd)).await
    }

    /// Create a file on the agent with given content
    pub async fn create_agent_file(&self, path: &str, content: &str) -> Result<(), E2EError> {
        exec_shell(
            &self.infra.runtime,
            &format!("echo '{}' > {}", content, path),
        )
        .await?;
        Ok(())
    }

    /// Read a file from the agent
    pub async fn read_agent_file(&self, path: &str) -> Result<String, E2EError> {
        exec_shell(&self.infra.runtime, &format!("cat {}", path)).await
    }

    /// Check if a file exists on the agent
    pub async fn agent_file_exists(&self, path: &str) -> Result<bool, E2EError> {
        let result = exec_shell(
            &self.infra.runtime,
            &format!("test -f {} && echo exists || echo missing", path),
        )
        .await?;
        Ok(result.trim() == "exists")
    }

    /// Create a directory on the agent
    pub async fn create_agent_dir(&self, path: &str) -> Result<(), E2EError> {
        exec_shell(&self.infra.runtime, &format!("mkdir -p {}", path)).await?;
        Ok(())
    }

    /// List files in an agent directory
    pub async fn list_agent_dir(&self, path: &str) -> Result<String, E2EError> {
        exec_shell(&self.infra.runtime, &format!("ls -la {}", path)).await
    }

    /// Get agent run log
    pub async fn get_agent_log(&self) -> Result<String, E2EError> {
        exec_shell(&self.infra.runtime, "cat /tmp/agent-run.log 2>/dev/null || echo 'No log file'").await
    }
}
