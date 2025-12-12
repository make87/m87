//! Docker integration tests
//!
//! Tests for `m87 <device> docker <args>`
//!
//! These tests use Docker-in-Docker via socket mounting. The e2e agent container
//! has Docker CLI installed and mounts /var/run/docker.sock from the host.

use super::fixtures::TestSetup;
use super::helpers::E2EError;

/// Test docker ps command
#[tokio::test]
async fn test_docker_ps() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Run docker ps
    let output = setup.device_cmd("docker ps").await?;

    tracing::info!("docker ps output: {}", output);

    // Docker ps should return container list with header
    // Should see CONTAINER ID header (Docker is now available via socket mount)
    assert!(
        output.contains("CONTAINER ID") || output.contains("CONTAINER"),
        "Expected Docker ps output with CONTAINER header, got: {}",
        output
    );

    tracing::info!("docker ps test passed!");
    Ok(())
}

/// Test docker images command
#[tokio::test]
async fn test_docker_images() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Run docker images
    let output = setup.device_cmd("docker images").await?;

    tracing::info!("docker images output: {}", output);

    // Docker images should return image list with header
    // Note: Docker CE uses "IMAGE" header, older versions use "REPOSITORY"
    assert!(
        output.contains("REPOSITORY") || output.contains("IMAGE"),
        "Expected Docker images output with IMAGE/REPOSITORY header, got: {}",
        output
    );

    tracing::info!("docker images test passed!");
    Ok(())
}

/// Test docker info command
#[tokio::test]
async fn test_docker_info() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Run docker info
    let output = setup.device_cmd("docker info").await?;

    tracing::info!("docker info output: {}", output);

    // Docker info should return system information
    assert!(
        output.contains("Server") || output.contains("Containers") || output.contains("Images"),
        "Expected Docker info output with system info, got: {}",
        output
    );

    tracing::info!("docker info test passed!");
    Ok(())
}

/// Test docker version command
#[tokio::test]
async fn test_docker_version() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Run docker version
    let output = setup.device_cmd("docker version").await?;

    tracing::info!("docker version output: {}", output);

    // Docker version should return version info
    assert!(
        output.contains("Version") || output.contains("Client") || output.contains("Server"),
        "Expected Docker version output, got: {}",
        output
    );

    tracing::info!("docker version test passed!");
    Ok(())
}
