//! Device registration fixture for E2E tests

use crate::e2e_containers::containers::E2EInfra;
use crate::e2e_containers::helpers::{exec_shell, wait_for_result, E2EError, WaitConfig};
use regex::Regex;

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
    ///
    /// We extract the auth-request UUID from whichever of the two runtime
    /// log files contains it: `runtime login` writes to
    /// `/tmp/runtime-login.log`, `runtime run` (the daemon) writes to
    /// `/tmp/runtime-run.log`. Both emit
    /// `tracing::info!("Posted auth request. To approve, check request id
    /// <UUID>")` when triggering registration. We can't rely on the CLI's
    /// `devices list` table because it truncates the REQUEST column to
    /// ~12 chars.
    pub async fn wait_for_auth_request(&self) -> Result<String, E2EError> {
        let uuid_re = Regex::new(
            r"check request id ([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
        )
        .expect("uuid regex compiles");

        wait_for_result(
            WaitConfig::with_description("auth request"),
            || async {
                // Concatenate both candidate log files. A single `cat`
                // gracefully ignores missing files so we don't need two
                // separate exec round-trips.
                let combined = crate::e2e_containers::helpers::exec_shell(
                    &self.infra.runtime,
                    "cat /tmp/runtime-login.log /tmp/runtime-run.log 2>/dev/null || true",
                )
                .await?;
                Ok(uuid_re
                    .captures(&combined)
                    .map(|cap| cap[1].to_string()))
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
    ///
    /// Reads structured `PublicDevice` records via `m87 devices list --json`.
    /// We intentionally do NOT use `cli_exec` here: it appends `--verbose`,
    /// which raises the tracing log level to `info` and — because
    /// `tracing_subscriber::fmt::layer()` writes to stdout by default —
    /// floods stdout with log lines that drown the JSON output. Running the
    /// command directly via `exec_shell` keeps the log level at `warn`, so
    /// stdout is clean JSON.
    pub async fn wait_for_registered(&self) -> Result<RegisteredDevice, E2EError> {
        wait_for_result(
            WaitConfig::with_description("device registration"),
            || async {
                // `RUST_LOG=error` keeps the m87-client tracing subscriber
                // from writing log lines to stdout (the default writer for
                // `tracing_subscriber::fmt::layer()`), which would otherwise
                // mix with the JSON payload. The container's baseline
                // RUST_LOG is "info,m87_client=debug" — too noisy.
                let output =
                    exec_shell(&self.infra.cli, "RUST_LOG=error m87 devices list --json").await?;
                let v: serde_json::Value = serde_json::from_str(output.trim())
                    .map_err(|e| E2EError::Parse(format!("devices list --json: {e}")))?;
                let devices = v
                    .get("devices")
                    .and_then(|d| d.as_array())
                    .cloned()
                    .unwrap_or_default();
                Ok(devices.into_iter().find_map(|d| {
                    let name = d.get("name")?.as_str()?.to_string();
                    let short_id = d.get("short_id")?.as_str()?.to_string();
                    Some(RegisteredDevice { name, short_id })
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
