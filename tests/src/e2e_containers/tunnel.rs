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

/// Test TCP tunnel with port range (same local/remote ports)
/// 1. Register device
/// 2. Start HTTP servers on agent ports 8001, 8002, 8003 with unique responses
/// 3. CLI tunnels 8001-8003 (same port range)
/// 4. Verify each port responds with correct content
#[tokio::test]
async fn test_tunnel_port_range_same() -> Result<(), E2EError> {
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

    // Step 4: Start HTTP servers on multiple ports with unique responses
    tracing::info!("Starting HTTP servers on agent ports 8001-8003...");
    for port in 8001..=8003 {
        exec_background(
            &infra.agent,
            &format!(
                "sh -c 'while true; do printf \"HTTP/1.1 200 OK\\r\\nContent-Type: text/plain\\r\\nConnection: close\\r\\n\\r\\nPort {}\" | nc -l -p {} -q 1; done'",
                port, port
            ),
            &format!("/tmp/http-server-{}.log", port),
        )
        .await?;
    }

    // Give HTTP servers time to start
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 5: Start tunnel with port range
    tracing::info!("Starting tunnel {} -> 8001-8003...", device.name);
    exec_background(
        &infra.cli,
        &format!("m87 {} tunnel 8001-8003", device.name),
        "/tmp/tunnel-range.log",
    )
    .await?;

    // Step 6: Wait for all tunnels to be listening
    tracing::info!("Waiting for tunnels to establish...");
    for port in [8001u16, 8002, 8003] {
        let cli = &infra.cli;
        wait_for(
            WaitConfig::with_description("tunnel listening")
                .max_attempts(20)
                .interval(Duration::from_secs(1)),
            || async move { is_port_listening(cli, port).await.unwrap_or(false) },
        )
        .await?;
        tracing::info!("Port {} is listening", port);
    }

    // Wait a bit for HTTP servers to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 7: Test each port through tunnel
    tracing::info!("Testing tunnel connections...");
    for port in [8001u16, 8002, 8003] {
        let response = exec_shell(
            &infra.cli,
            &format!("curl -v --max-time 10 http://localhost:{}/ 2>&1", port),
        )
        .await?;
        tracing::info!("Port {} response: {}", port, response);

        assert!(
            response.contains(&format!("Port {}", port)),
            "Expected 'Port {}' in response from port {}, got: {}",
            port,
            port,
            response
        );
    }

    tracing::info!("Port range tunnel test passed!");
    Ok(())
}

/// Test TCP tunnel with offset port range mapping
/// 1. Register device
/// 2. Start HTTP servers on agent ports 9001, 9002, 9003 with unique responses
/// 3. CLI tunnels 8001-8003:9001-9003 (local 8001→remote 9001, etc.)
/// 4. Verify each local port connects to correct remote port
#[tokio::test]
async fn test_tunnel_port_range_offset() -> Result<(), E2EError> {
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

    // Step 4: Start HTTP servers on remote ports (9001-9003) with unique responses
    tracing::info!("Starting HTTP servers on agent ports 9001-9003...");
    for port in 9001..=9003 {
        exec_background(
            &infra.agent,
            &format!(
                "sh -c 'while true; do printf \"HTTP/1.1 200 OK\\r\\nContent-Type: text/plain\\r\\nConnection: close\\r\\n\\r\\nRemote {}\" | nc -l -p {} -q 1; done'",
                port, port
            ),
            &format!("/tmp/http-server-{}.log", port),
        )
        .await?;
    }

    // Give HTTP servers time to start
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 5: Start tunnel with offset port range (local 8001-8003 → remote 9001-9003)
    tracing::info!(
        "Starting tunnel {} -> 8001-8003:9001-9003...",
        device.name
    );
    exec_background(
        &infra.cli,
        &format!("m87 {} tunnel 8001-8003:9001-9003", device.name),
        "/tmp/tunnel-offset.log",
    )
    .await?;

    // Step 6: Wait for all tunnels to be listening on local ports
    tracing::info!("Waiting for tunnels to establish...");
    for port in [8001u16, 8002, 8003] {
        let cli = &infra.cli;
        wait_for(
            WaitConfig::with_description("tunnel listening")
                .max_attempts(20)
                .interval(Duration::from_secs(1)),
            || async move { is_port_listening(cli, port).await.unwrap_or(false) },
        )
        .await?;
        tracing::info!("Port {} is listening", port);
    }

    // Wait a bit for HTTP servers to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 7: Test each local port maps to correct remote port
    tracing::info!("Testing tunnel connections with offset mapping...");
    for (local_port, remote_port) in [(8001u16, 9001u16), (8002, 9002), (8003, 9003)] {
        let response = exec_shell(
            &infra.cli,
            &format!(
                "curl -v --max-time 10 http://localhost:{}/ 2>&1",
                local_port
            ),
        )
        .await?;
        tracing::info!(
            "Local port {} (→remote {}) response: {}",
            local_port,
            remote_port,
            response
        );

        assert!(
            response.contains(&format!("Remote {}", remote_port)),
            "Expected 'Remote {}' when connecting to local port {}, got: {}",
            remote_port,
            local_port,
            response
        );
    }

    tracing::info!("Offset port range tunnel test passed!");
    Ok(())
}

/// Test that invalid port range is rejected (mismatched sizes)
#[tokio::test]
async fn test_tunnel_port_range_mismatch_rejected() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Step 1: Register device
    tracing::info!("Registering device...");
    let device = register_device_full(&infra).await?;

    // Step 2: Setup SNI
    let sni = SniSetup::from_cli(&infra.cli).await?;
    sni.setup_both(&infra.agent, &infra.cli, &device.short_id)
        .await?;

    // Step 3: Start agent
    let agent = AgentRunner::new(&infra);
    agent.start_with_tunnel().await?;

    // Step 4: Try to start tunnel with mismatched range sizes
    tracing::info!("Testing mismatched port range rejection...");
    let result = exec_shell(
        &infra.cli,
        &format!(
            "m87 {} tunnel 8001-8003:9001-9005 2>&1 || echo 'TUNNEL_FAILED'",
            device.name
        ),
    )
    .await?;

    tracing::info!("Mismatch test result: {}", result);

    // Should fail with error about range size mismatch
    assert!(
        result.contains("does not match") || result.contains("TUNNEL_FAILED"),
        "Expected error about range size mismatch, got: {}",
        result
    );

    tracing::info!("Port range mismatch rejection test passed!");
    Ok(())
}
