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
