use std::time::Duration;
use tokio::time::sleep;

use super::containers::E2EInfra;
use super::setup::{ensure_images_built, ensure_network_created};

/// Register a device through the full flow:
/// 1. Agent starts login process
/// 2. Auth request appears in pending devices
/// 3. Admin approves the device
/// 4. Agent completes registration
///
/// Returns the device name (not the short_id - tunnel command needs the name)
pub async fn register_device(infra: &E2EInfra) -> Result<String, Box<dyn std::error::Error>> {
    // Start agent login in background
    tracing::info!("Starting agent login...");
    infra.start_agent_login().await?;

    // Wait for auth request to appear
    tracing::info!("Waiting for auth request...");
    let mut request_id: Option<String> = None;

    for attempt in 1..=30 {
        sleep(Duration::from_secs(2)).await;

        let output = infra.cli_exec(&["devices", "list"]).await.unwrap_or_default();

        tracing::debug!("Devices list output: {}", output);

        // Look for UUID pattern (auth request ID)
        let uuid_pattern =
            regex::Regex::new(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}")
                .unwrap();

        if let Some(m) = uuid_pattern.find(&output) {
            request_id = Some(m.as_str().to_string());
            tracing::info!("Found auth request: {}", request_id.as_ref().unwrap());
            break;
        }

        if attempt % 5 == 0 {
            tracing::info!("Still waiting for auth request... (attempt {})", attempt);

            // Check agent login log for debugging
            if let Ok(log) = infra.get_agent_login_log().await {
                if !log.is_empty() {
                    tracing::debug!("Agent login log: {}", log);
                }
            }
        }
    }

    let request_id = request_id.ok_or("No auth request appeared after 30 attempts")?;

    // Approve the device
    tracing::info!("Approving device: {}", request_id);
    let approve_output = infra.cli_exec(&["devices", "approve", &request_id]).await?;
    tracing::debug!("Approve output: {}", approve_output);

    // Wait for device to complete registration
    tracing::info!("Waiting for device to complete registration...");
    let mut device_name: Option<String> = None;

    for attempt in 1..=15 {
        sleep(Duration::from_secs(2)).await;

        let output = infra.cli_exec(&["devices", "list"]).await.unwrap_or_default();

        tracing::debug!("Devices list after approval: {}", output);

        // Parse device list output
        // Format: short_id   name   status   arch   os   ip   last_seen   pending   version
        // Registered devices show 6-char short ID, pending show full UUID
        for line in output.lines() {
            // Skip header lines and pending devices
            if line.contains("pending") || line.starts_with("ID") || line.is_empty() {
                continue;
            }

            // Split line into columns by whitespace
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let short_id = parts[0];
                let name = parts[1];

                // Registered devices have 6-char hex short_id
                if short_id.len() == 6 && short_id.chars().all(|c| c.is_ascii_hexdigit()) {
                    device_name = Some(name.to_string());
                    tracing::info!(
                        "Device registered: short_id={}, name={}",
                        short_id,
                        name
                    );
                    break;
                }
            }
        }

        if device_name.is_some() {
            break;
        }

        if attempt % 5 == 0 {
            tracing::info!(
                "Still waiting for device registration... (attempt {})",
                attempt
            );
        }
    }

    device_name.ok_or_else(|| "Device did not complete registration".into())
}

/// Test the complete device registration flow
#[tokio::test]
async fn test_device_registration_flow() {
    // Initialize tracing for better test output
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();

    // Build images and create network
    ensure_images_built().expect("Failed to build Docker images");
    ensure_network_created().expect("Failed to create Docker network");

    // Start infrastructure
    let infra = E2EInfra::start()
        .await
        .expect("Failed to start E2E infrastructure");

    // Register device using the helper
    let device_name = register_device(&infra)
        .await
        .expect("Failed to register device");

    tracing::info!("Device registration test passed! Device name: {}", device_name);
}

/// Test that devices can be listed (basic API connectivity test)
#[tokio::test]
async fn test_devices_list() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();

    ensure_images_built().expect("Failed to build Docker images");
    ensure_network_created().expect("Failed to create Docker network");

    let infra = E2EInfra::start()
        .await
        .expect("Failed to start E2E infrastructure");

    // Simply verify we can list devices (should return empty or header)
    let output = infra
        .cli_exec(&["devices", "list"])
        .await
        .expect("Failed to list devices");

    tracing::info!("Devices list output: {}", output);

    // Test passes if we get any response (not an error)
    // The list might be empty or have headers
    assert!(
        !output.contains("error") && !output.contains("Error"),
        "Unexpected error in devices list: {}",
        output
    );
}
