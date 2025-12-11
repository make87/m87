//! Tunnel E2E tests

use std::time::Duration;

use super::containers::E2EInfra;
use super::device_registration::register_device_full;
use super::fixtures::AgentRunner;
use super::helpers::{
    exec_background, exec_shell, is_port_listening, wait_for, E2EError, SniSetup, WaitConfig,
};

/// Test TCP tunnel from CLI to agent device
/// 1. Register device
/// 2. Start HTTP server on agent (port 80)
/// 3. CLI tunnels 8080:80 (local 8080 → remote 80)
/// 4. CLI curls localhost:8080 to verify tunnel works
#[tokio::test]
async fn test_tunnel_tcp() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Step 1: Register device
    tracing::info!("Registering device...");
    let device = register_device_full(&infra).await?;
    tracing::info!("Device registered: {} ({})", device.name, device.short_id);

    // Step 2: Setup SNI for tunneling
    tracing::info!("Setting up SNI...");
    let sni = SniSetup::from_cli(&infra.cli).await?;
    sni.setup_both(&infra.agent, &infra.cli, &device.short_id)
        .await?;

    // Step 3: Start agent and wait for control tunnel
    tracing::info!("Starting agent run...");
    let agent = AgentRunner::new(&infra);
    agent.start_with_tunnel().await?;

    // Step 4: Start HTTP server on agent using netcat
    // Note: Using printf instead of echo -e for portability (dash doesn't support echo -e)
    tracing::info!("Starting HTTP server on agent...");
    exec_background(
        &infra.agent,
        "sh -c 'while true; do printf \"HTTP/1.1 200 OK\\r\\nContent-Type: text/plain\\r\\nConnection: close\\r\\n\\r\\nHello from tunnel test\" | nc -l -p 80 -q 1; done'",
        "/tmp/http-server.log",
    ).await?;

    // Give HTTP server time to start
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 5: Start tunnel in background on CLI container
    tracing::info!("Starting tunnel {} -> 8080:80...", device.name);
    exec_background(
        &infra.cli,
        &format!("m87 {} tunnel 8080:80", device.name),
        "/tmp/tunnel.log",
    )
    .await?;

    // Step 6: Wait for tunnel to be listening
    tracing::info!("Waiting for tunnel to establish...");
    wait_for(
        WaitConfig::with_description("tunnel listening")
            .max_attempts(20)
            .interval(Duration::from_secs(1)),
        || async { is_port_listening(&infra.cli, 8080).await.unwrap_or(false) },
    )
    .await?;

    // Wait a bit for HTTP server to be ready after nc -z check consumes a connection
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 7: Curl through tunnel from CLI container
    tracing::info!("Testing tunnel connection...");
    let response = exec_shell(
        &infra.cli,
        "curl -v --max-time 10 http://localhost:8080/ 2>&1",
    )
    .await?;
    tracing::info!("Curl response: {}", response);

    // Step 8: Assert response contains expected content
    assert!(
        response.contains("Hello from tunnel test"),
        "Expected 'Hello from tunnel test' in response, got: {}",
        response
    );

    tracing::info!("Tunnel test passed!");
    Ok(())
}
