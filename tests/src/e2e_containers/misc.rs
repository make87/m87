//! Miscellaneous CLI command tests
//!
//! Tests for simple CLI commands that don't require device setup:
//! - version
//! - ssh enable/disable

use super::containers::E2EInfra;
use super::helpers::E2EError;

/// Test that `m87 version` returns version information
#[tokio::test]
async fn test_version() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    let output = infra
        .cli_exec(&["version"])
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;

    tracing::info!("Version output: {}", output);

    // Should contain version string
    assert!(
        output.contains("Version:"),
        "Expected 'Version:' in output, got: {}",
        output
    );

    // Should contain semver-like pattern (e.g., "0.1.0" or "0.0.0-dev0")
    assert!(
        output.contains('.'),
        "Expected version number with dots, got: {}",
        output
    );

    // Should contain build info
    assert!(
        output.contains("Build:") || output.contains("Platform:"),
        "Expected build info in output, got: {}",
        output
    );

    tracing::info!("version test passed!");
    Ok(())
}

/// Test that `m87 ssh enable` works
#[tokio::test]
async fn test_ssh_enable() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    let output = infra
        .cli_exec(&["ssh", "enable"])
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;

    tracing::info!("SSH enable output: {}", output);

    // Should indicate success
    assert!(
        output.to_lowercase().contains("enabled")
            || output.to_lowercase().contains("success")
            || output.contains("SSH"),
        "Expected success message for ssh enable, got: {}",
        output
    );

    tracing::info!("ssh enable test passed!");
    Ok(())
}

/// Test that `m87 ssh disable` works
#[tokio::test]
async fn test_ssh_disable() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    let output = infra
        .cli_exec(&["ssh", "disable"])
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;

    tracing::info!("SSH disable output: {}", output);

    // Should indicate success
    assert!(
        output.to_lowercase().contains("disabled")
            || output.to_lowercase().contains("success")
            || output.contains("SSH"),
        "Expected success message for ssh disable, got: {}",
        output
    );

    tracing::info!("ssh disable test passed!");
    Ok(())
}
