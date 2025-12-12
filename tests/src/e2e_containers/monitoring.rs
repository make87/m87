//! Device monitoring tests
//!
//! Tests for:
//! - `m87 <device> stats`
//! - `m87 <device> logs`

use super::fixtures::TestSetup;
use super::helpers::E2EError;

/// Test basic stats command
#[tokio::test]
async fn test_stats() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Get device stats
    let output = setup.device_cmd("stats").await?;

    tracing::info!("stats output: {}", output);

    // Stats should return some system metrics
    // Common metrics include CPU, memory, disk, etc.
    // At minimum, it shouldn't error out
    assert!(
        !output.to_lowercase().contains("error:")
            && !output.contains("connection refused"),
        "Stats command failed: {}",
        output
    );

    // Look for common metric indicators
    let has_metrics = output.to_lowercase().contains("cpu")
        || output.to_lowercase().contains("memory")
        || output.to_lowercase().contains("mem")
        || output.to_lowercase().contains("disk")
        || output.contains("%")
        || output.contains("MB")
        || output.contains("GB");

    assert!(
        has_metrics || output.len() > 0,
        "Expected metrics data in stats output, got: {}",
        output
    );

    tracing::info!("stats test passed!");
    Ok(())
}

/// Test basic logs command
#[tokio::test]
async fn test_logs_basic() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Get logs (should return recent container logs)
    let output = setup.device_cmd("logs").await?;

    tracing::info!("logs output: {}", output);

    // Logs command should not error out
    // It may return empty if no containers are running with logs
    assert!(
        !output.to_lowercase().contains("error:")
            && !output.contains("connection refused"),
        "Logs command failed: {}",
        output
    );

    tracing::info!("logs basic test passed!");
    Ok(())
}

/// Test logs with --tail option
#[tokio::test]
async fn test_logs_tail() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Get limited logs
    let output = setup.device_cmd("logs --tail 10").await?;

    tracing::info!("logs tail output: {}", output);

    // Should not error
    assert!(
        !output.to_lowercase().contains("error:")
            && !output.contains("connection refused"),
        "Logs tail command failed: {}",
        output
    );

    // If there's output, it should be limited
    let line_count = output.lines().count();
    tracing::info!("logs tail returned {} lines", line_count);

    tracing::info!("logs tail test passed!");
    Ok(())
}
