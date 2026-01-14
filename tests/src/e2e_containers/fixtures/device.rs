//! Device registration fixture for E2E tests

use crate::e2e_containers::containers::E2EInfra;
use crate::e2e_containers::helpers::{
    extract_auth_requests, parse_devices_list, wait_for_result, E2EError, WaitConfig,
};

/// Information about a registered device
#[derive(Debug, Clone)]
pub struct RegisteredDevice {
    pub name: String,
    pub short_id: String,
}

/// Builder for device registration with explicit steps
pub struct DeviceRegistration<'a> {
    infra: &'a E2EInfra,
}

impl<'a> DeviceRegistration<'a> {
    /// Create a new device registration builder
    pub fn new(infra: &'a E2EInfra) -> Self {
        Self { infra }
    }

    /// Step 1: Start runtime login process (runs in background)
    pub async fn start_login(&self) -> Result<(), E2EError> {
        self.infra
            .start_runtime_login()
            .await
            .map_err(|e| E2EError::Exec(e.to_string()))
    }

    /// Step 2: Wait for auth request to appear and return its ID
    pub async fn wait_for_auth_request(&self) -> Result<String, E2EError> {
        wait_for_result(
            WaitConfig::with_description("auth request"),
            || async {
                let output = self
                    .infra
                    .cli_exec(&["devices", "list"])
                    .await
                    .map_err(|e| E2EError::Exec(e.to_string()))?;
                Ok(extract_auth_requests(&output).first().cloned())
            },
        )
        .await
    }

    /// Step 3: Approve the auth request
    pub async fn approve(&self, auth_id: &str) -> Result<(), E2EError> {
        self.infra
            .cli_exec(&["devices", "approve", auth_id])
            .await
            .map_err(|e| E2EError::Exec(e.to_string()))?;
        tracing::info!("Approved auth request: {}", auth_id);
        Ok(())
    }

    /// Step 4: Wait for device to be registered and return device info
    pub async fn wait_for_registered(&self) -> Result<RegisteredDevice, E2EError> {
        wait_for_result(
            WaitConfig::with_description("device registration"),
            || async {
                let output = self
                    .infra
                    .cli_exec(&["devices", "list"])
                    .await
                    .map_err(|e| E2EError::Exec(e.to_string()))?;
                tracing::debug!("devices list output: {:?}", output);
                let devices = parse_devices_list(&output);
                tracing::debug!("parsed devices: {:?}", devices);
                // Device status is "online" when registered, not "registered"
                Ok(devices
                    .into_iter()
                    .find(|d| d.status == "online" || d.status == "registered")
                    .map(|d| RegisteredDevice {
                        name: d.name,
                        short_id: d.short_id,
                    }))
            },
        )
        .await
    }

    /// Convenience: Run full registration flow
    ///
    /// This combines all steps: start_login -> wait_for_auth_request -> approve -> wait_for_registered
    pub async fn register_full(&self) -> Result<RegisteredDevice, E2EError> {
        tracing::info!("Starting device registration flow...");

        self.start_login().await?;
        tracing::info!("Agent login started");

        let auth_id = self.wait_for_auth_request().await?;
        tracing::info!("Auth request received: {}", auth_id);

        self.approve(&auth_id).await?;

        let device = self.wait_for_registered().await?;
        tracing::info!(
            "Device registered: {} ({})",
            device.name,
            device.short_id
        );

        Ok(device)
    }
}
