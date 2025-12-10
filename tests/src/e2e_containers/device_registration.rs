use std::time::Duration;
use tokio::time::sleep;

use super::containers::E2EInfra;
use super::setup::{ensure_images_built, ensure_network_created};

/// Test the complete device registration flow:
/// 1. Agent starts login process
/// 2. Auth request appears in pending devices
/// 3. Admin approves the device
/// 4. Agent completes registration
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

    // Start agent login in background
    tracing::info!("Starting agent login...");
    infra
        .start_agent_login()
        .await
        .expect("Failed to start agent login");

    // Wait for auth request to appear
    tracing::info!("Waiting for auth request...");
    let mut request_id: Option<String> = None;

    for attempt in 1..=30 {
        sleep(Duration::from_secs(2)).await;

        let output = infra
            .cli_exec(&["devices", "list"])
            .await
            .unwrap_or_default();

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

    let request_id = request_id.expect("No auth request appeared after 30 attempts");

    // Approve the device
    tracing::info!("Approving device: {}", request_id);
    let approve_output = infra
        .cli_exec(&["devices", "approve", &request_id])
        .await
        .expect("Failed to approve device");
    tracing::debug!("Approve output: {}", approve_output);

    // Wait for device to complete registration
    tracing::info!("Waiting for device to complete registration...");
    let mut device_id: Option<String> = None;

    for attempt in 1..=15 {
        sleep(Duration::from_secs(2)).await;

        let output = infra
            .cli_exec(&["devices", "list"])
            .await
            .unwrap_or_default();

        tracing::debug!("Devices list after approval: {}", output);

        // Look for 6-char device ID (registered device, not pending)
        // Registered devices show short ID, pending show full UUID
        let short_id_pattern = regex::Regex::new(r"^[a-f0-9]{6}").unwrap();

        for line in output.lines() {
            if !line.contains("pending") {
                if let Some(m) = short_id_pattern.find(line) {
                    device_id = Some(m.as_str().to_string());
                    tracing::info!("Device registered with ID: {}", device_id.as_ref().unwrap());
                    break;
                }
            }
        }

        if device_id.is_some() {
            break;
        }

        if attempt % 5 == 0 {
            tracing::info!(
                "Still waiting for device registration... (attempt {})",
                attempt
            );
        }
    }

    let device_id = device_id.expect("Device did not complete registration");
    tracing::info!("Device registration test passed! Device ID: {}", device_id);
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
