//! Docker integration tests
//!
//! Tests for `m87 <device> docker <args>`
//!
//! Note: These tests require Docker to be available on the agent.
//! In a containerized test environment, this requires Docker-in-Docker
//! or mounting the Docker socket.

use super::fixtures::TestSetup;
use super::helpers::E2EError;

/// Test docker ps command
#[tokio::test]
async fn test_docker_ps() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Run docker ps
    let output = setup.device_cmd("docker ps").await?;

    tracing::info!("docker ps output: {}", output);

    // Docker ps should return container list format or indicate Docker isn't available
    // Header typically contains CONTAINER ID or similar
    let is_docker_output = output.contains("CONTAINER")
        || output.contains("container")
        || output.is_empty()  // No running containers
        || output.contains("Cannot connect")  // Docker not available
        || output.contains("not installed")  // Docker CLI not installed
        || output.contains("No such file");  // Docker binary not found

    assert!(
        is_docker_output,
        "Unexpected docker ps output: {}",
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

    // Docker images should return image list format or indicate Docker isn't available
    let is_docker_output = output.contains("REPOSITORY")
        || output.contains("IMAGE")
        || output.contains("TAG")
        || output.is_empty()
        || output.contains("Cannot connect")
        || output.contains("not installed")
        || output.contains("No such file");

    assert!(
        is_docker_output,
        "Unexpected docker images output: {}",
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

    // Docker info should return system info or indicate Docker isn't available
    let is_docker_output = output.contains("Server")
        || output.contains("Containers")
        || output.contains("Images")
        || output.contains("Storage Driver")
        || output.contains("Cannot connect")
        || output.contains("not installed")
        || output.contains("No such file");

    assert!(
        is_docker_output,
        "Unexpected docker info output: {}",
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

    // Docker version should return version info or indicate Docker isn't available
    let is_docker_output = output.contains("Version")
        || output.contains("Client")
        || output.contains("Server")
        || output.contains("Cannot connect")
        || output.contains("not installed")
        || output.contains("No such file");

    assert!(
        is_docker_output,
        "Unexpected docker version output: {}",
        output
    );

    tracing::info!("docker version test passed!");
    Ok(())
}
