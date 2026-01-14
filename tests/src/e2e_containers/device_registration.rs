//! Device registration tests

use super::containers::E2EInfra;
use super::fixtures::{DeviceRegistration, RegisteredDevice};
use super::helpers::E2EError;

/// Register a device through the full flow:
/// 1. Agent starts login process
/// 2. Auth request appears in pending devices
/// 3. Admin approves the device
/// 4. Agent completes registration
///
/// Returns the device name (not the short_id - tunnel command needs the name)
pub async fn register_device(infra: &E2EInfra) -> Result<String, Box<dyn std::error::Error>> {
    let device = DeviceRegistration::new(infra)
        .register_full()
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
    Ok(device.name)
}

/// Register a device and return full device info
pub async fn register_device_full(infra: &E2EInfra) -> Result<RegisteredDevice, E2EError> {
    DeviceRegistration::new(infra).register_full().await
}

/// Test the complete device registration flow
#[tokio::test]
async fn test_device_registration_flow() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Register device using the new fixture
    let device = DeviceRegistration::new(&infra).register_full().await?;

    tracing::info!(
        "Device registration test passed! Device: {} ({})",
        device.name,
        device.short_id
    );

    Ok(())
}

/// Test that devices can be listed (basic API connectivity test)
#[tokio::test]
async fn test_devices_list() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Simply verify we can list devices (should return empty or header)
    let output = infra
        .cli_exec(&["devices", "list"])
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;

    tracing::info!("Devices list output: {}", output);

    // Test passes if we get any response (not an error)
    assert!(
        !output.contains("error") && !output.contains("Error"),
        "Unexpected error in devices list: {}",
        output
    );

    Ok(())
}

/// Test that `m87 devices reject` works for pending devices
#[tokio::test]
async fn test_devices_reject() -> Result<(), E2EError> {
    use super::helpers::extract_auth_requests;

    let infra = E2EInfra::init().await?;

    // Step 1: Start agent login to create a pending auth request
    infra
        .start_runtime_login()
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;
    tracing::info!("Agent login started");

    // Step 2: Wait for auth request to appear
    let auth_id = super::helpers::wait_for_result(
        super::helpers::WaitConfig::with_description("auth request for reject test"),
        || async {
            let output = infra
                .cli_exec(&["devices", "list"])
                .await
                .map_err(|e| E2EError::Exec(e.to_string()))?;
            Ok(extract_auth_requests(&output).first().cloned())
        },
    )
    .await?;
    tracing::info!("Auth request received: {}", auth_id);

    // Step 3: Reject the auth request
    let reject_output = infra
        .cli_exec(&["devices", "reject", &auth_id])
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;
    tracing::info!("Reject output: {}", reject_output);

    // Should indicate success
    assert!(
        reject_output.to_lowercase().contains("rejected")
            || reject_output.to_lowercase().contains("success"),
        "Expected rejection confirmation, got: {}",
        reject_output
    );

    // Step 4: Verify device is no longer pending
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let list_output = infra
        .cli_exec(&["devices", "list"])
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;

    // The auth request ID should no longer appear
    assert!(
        !list_output.contains(&auth_id),
        "Rejected device should not appear in list: {}",
        list_output
    );

    tracing::info!("devices reject test passed!");
    Ok(())
}
