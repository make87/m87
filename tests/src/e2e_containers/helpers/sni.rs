//! SNI and /etc/hosts management for E2E tests

use testcontainers::{ContainerAsync, GenericImage};

use super::{exec_shell, E2EError};

/// SNI setup information for connecting to the server
#[derive(Debug, Clone)]
pub struct SniSetup {
    pub server_name: String,
    pub server_ip: String,
}

impl SniSetup {
    /// Resolve server name and IP from CLI container's config
    pub async fn from_cli(cli: &ContainerAsync<GenericImage>) -> Result<Self, E2EError> {
        // Get server name from config.json
        let server_name = exec_shell(
            cli,
            "cat /root/.config/m87/config.json | grep api_url | head -1 | sed 's/.*https:\\/\\/\\([^:]*\\).*/\\1/'",
        )
        .await?;

        if server_name.is_empty() {
            return Err(E2EError::Parse("Could not extract server name from config".to_string()));
        }

        // Get server IP
        let server_ip = exec_shell(
            cli,
            &format!("getent hosts {} | awk '{{print $1}}'", server_name),
        )
        .await?;

        if server_ip.is_empty() {
            return Err(E2EError::Parse(format!(
                "Could not resolve IP for server: {}",
                server_name
            )));
        }

        tracing::info!("SNI setup: {} -> {}", server_name, server_ip);

        Ok(Self {
            server_name,
            server_ip,
        })
    }

    /// Add /etc/hosts entry for control tunnel (agent)
    ///
    /// The agent connects to `control-{device_short_id}.{server_name}`
    pub async fn setup_agent_control_tunnel(
        &self,
        agent: &ContainerAsync<GenericImage>,
        device_short_id: &str,
    ) -> Result<(), E2EError> {
        let host = format!("control-{}.{}", device_short_id, self.server_name);
        exec_shell(
            agent,
            &format!("echo '{} {}' >> /etc/hosts", self.server_ip, host),
        )
        .await?;
        tracing::info!("Added agent /etc/hosts: {} -> {}", host, self.server_ip);
        Ok(())
    }

    /// Add /etc/hosts entry for CLI tunnel
    ///
    /// The CLI connects to `{device_short_id}.{server_name}`
    pub async fn setup_cli_tunnel(
        &self,
        cli: &ContainerAsync<GenericImage>,
        device_short_id: &str,
    ) -> Result<(), E2EError> {
        let host = format!("{}.{}", device_short_id, self.server_name);
        exec_shell(
            cli,
            &format!("echo '{} {}' >> /etc/hosts", self.server_ip, host),
        )
        .await?;
        tracing::info!("Added CLI /etc/hosts: {} -> {}", host, self.server_ip);
        Ok(())
    }

    /// Setup both agent and CLI tunnel hosts entries
    pub async fn setup_both(
        &self,
        agent: &ContainerAsync<GenericImage>,
        cli: &ContainerAsync<GenericImage>,
        device_short_id: &str,
    ) -> Result<(), E2EError> {
        self.setup_agent_control_tunnel(agent, device_short_id)
            .await?;
        self.setup_cli_tunnel(cli, device_short_id).await?;
        Ok(())
    }

    /// Get the control tunnel hostname for a device
    pub fn control_tunnel_host(&self, device_short_id: &str) -> String {
        format!("control-{}.{}", device_short_id, self.server_name)
    }

    /// Get the CLI tunnel hostname for a device
    pub fn cli_tunnel_host(&self, device_short_id: &str) -> String {
        format!("{}.{}", device_short_id, self.server_name)
    }
}
