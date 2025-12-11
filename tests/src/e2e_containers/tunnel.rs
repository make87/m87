use std::time::Duration;
use testcontainers::core::ExecCommand;
use tokio::time::sleep;

use super::containers::E2EInfra;
use super::device_registration::register_device;
use super::setup::{ensure_images_built, ensure_network_created};

/// Test TCP tunnel from CLI to agent device
/// 1. Register device
/// 2. Start HTTP server on agent (port 80)
/// 3. CLI tunnels 8080:80 (local 8080 → remote 80)
/// 4. CLI curls localhost:8080 to verify tunnel works
#[tokio::test]
async fn test_tunnel_tcp() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();

    ensure_images_built().expect("Failed to build Docker images");
    ensure_network_created().expect("Failed to create Docker network");

    let infra = E2EInfra::start()
        .await
        .expect("Failed to start E2E infrastructure");

    // 1. Register device
    tracing::info!("Registering device...");
    let device_name = register_device(&infra)
        .await
        .expect("Failed to register device");
    tracing::info!("Device registered: {}", device_name);

    // 2. Get device short_id for DNS setup BEFORE starting agent run
    // Docker DNS doesn't support wildcards, so we need /etc/hosts entries
    let devices_output = infra.cli_exec(&["devices", "list"]).await.unwrap_or_default();
    let mut device_short_id = String::new();
    for line in devices_output.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 && parts[1] == device_name {
            device_short_id = parts[0].to_string();
            break;
        }
    }
    tracing::info!("Device short_id: {}", device_short_id);

    // Get the server container name from config
    let mut server_name_result = infra
        .cli
        .exec(ExecCommand::new(vec![
            "sh",
            "-c",
            "cat /root/.config/m87/config.json | grep api_url | head -1 | sed 's/.*https:\\/\\/\\([^:]*\\).*/\\1/'",
        ]))
        .await
        .expect("Failed to get server name");
    let server_name_bytes = server_name_result.stdout_to_vec().await.unwrap_or_default();
    let server_name = String::from_utf8_lossy(&server_name_bytes)
        .trim()
        .to_string();
    tracing::info!("Server name from config: {}", server_name);

    // Get server IP from CLI container
    let mut server_ip_result = infra
        .cli
        .exec(ExecCommand::new(vec![
            "sh",
            "-c",
            &format!("getent hosts {} | awk '{{print $1}}'", server_name),
        ]))
        .await
        .expect("Failed to get server IP");
    let server_ip = server_ip_result.stdout_to_vec().await.unwrap_or_default();
    let server_ip = String::from_utf8_lossy(&server_ip).trim().to_string();
    tracing::info!("Server IP: {}", server_ip);

    // Add hosts entries to BOTH agent and CLI containers BEFORE starting agent run
    // Agent needs: control-{short_id}.{server} for control tunnel
    // CLI needs: {short_id}.{server} for CLI tunnel
    if !device_short_id.is_empty() && !server_ip.is_empty() {
        // Add control tunnel hostname to agent container
        let control_host = format!("control-{}.{}", device_short_id, server_name);
        infra
            .agent
            .exec(ExecCommand::new(vec![
                "sh",
                "-c",
                &format!("echo '{} {}' >> /etc/hosts", server_ip, control_host),
            ]))
            .await
            .expect("Failed to add agent hosts entry");
        tracing::info!(
            "Added agent /etc/hosts entry: {} -> {}",
            control_host,
            server_ip
        );

        // Add CLI tunnel hostname to CLI container
        let cli_host = format!("{}.{}", device_short_id, server_name);
        infra
            .cli
            .exec(ExecCommand::new(vec![
                "sh",
                "-c",
                &format!("echo '{} {}' >> /etc/hosts", server_ip, cli_host),
            ]))
            .await
            .expect("Failed to add CLI hosts entry");
        tracing::info!("Added CLI /etc/hosts entry: {} -> {}", cli_host, server_ip);
    }

    // 3. Now start agent run (after hosts entries are configured)
    tracing::info!("Starting agent run...");
    infra
        .agent
        .exec(ExecCommand::new(vec![
            "sh",
            "-c",
            "nohup m87 agent run > /tmp/agent-run.log 2>&1 &",
        ]))
        .await
        .expect("Failed to start agent run");

    // Wait for agent control tunnel to be established
    // The agent needs to connect its control tunnel to the server before CLI tunnel can work
    // Look for control tunnel connection success in logs
    tracing::info!("Waiting for agent control tunnel to establish...");
    let mut control_tunnel_ready = false;
    for attempt in 1..=30 {
        sleep(Duration::from_secs(2)).await;

        let mut agent_log_result = infra
            .agent
            .exec(ExecCommand::new(vec![
                "sh",
                "-c",
                "cat /tmp/agent-run.log 2>/dev/null || echo 'No agent log'",
            ]))
            .await
            .expect("Failed to get agent log");
        let agent_log = agent_log_result.stdout_to_vec().await.unwrap_or_default();
        let agent_log_str = String::from_utf8_lossy(&agent_log);

        // Check for signs that control tunnel is established and NOT crashed
        // "Starting control tunnel" appears at start, but we need to ensure it didn't crash
        // Look for successful connection indicators without crash messages
        let has_started = agent_log_str.contains("Starting control tunnel");
        let has_crashed = agent_log_str.contains("Control tunnel crashed");

        if has_started && !has_crashed {
            // Give a bit more time for connection to fully establish
            sleep(Duration::from_secs(2)).await;

            // Re-check log
            let mut agent_log_result2 = infra
                .agent
                .exec(ExecCommand::new(vec![
                    "sh",
                    "-c",
                    "cat /tmp/agent-run.log 2>/dev/null || echo 'No agent log'",
                ]))
                .await
                .expect("Failed to get agent log");
            let agent_log2 = agent_log_result2.stdout_to_vec().await.unwrap_or_default();
            let agent_log_str2 = String::from_utf8_lossy(&agent_log2);

            if !agent_log_str2.contains("Control tunnel crashed") {
                tracing::info!(
                    "Agent control tunnel established (attempt {})\nAgent log: {}",
                    attempt,
                    agent_log_str2
                );
                control_tunnel_ready = true;
                break;
            }
        }

        if attempt % 5 == 0 {
            tracing::info!(
                "Still waiting for agent control tunnel... (attempt {})\nAgent log: {}",
                attempt,
                agent_log_str
            );
        }
    }

    if !control_tunnel_ready {
        // Get final agent log for debugging
        let mut agent_log_result = infra
            .agent
            .exec(ExecCommand::new(vec![
                "sh",
                "-c",
                "cat /tmp/agent-run.log 2>/dev/null || echo 'No agent log'",
            ]))
            .await
            .expect("Failed to get agent log");
        let agent_log = agent_log_result.stdout_to_vec().await.unwrap_or_default();
        panic!(
            "Agent control tunnel did not establish within 60 seconds.\nAgent log: {}",
            String::from_utf8_lossy(&agent_log)
        );
    }

    // 4. Start HTTP server on agent using netcat
    // Using a while loop to handle multiple requests
    // Note: Using printf instead of echo -e for portability (dash doesn't support echo -e)
    tracing::info!("Starting HTTP server on agent...");
    infra
        .agent
        .exec(ExecCommand::new(vec![
            "sh",
            "-c",
            "nohup sh -c 'while true; do printf \"HTTP/1.1 200 OK\\r\\nContent-Type: text/plain\\r\\nConnection: close\\r\\n\\r\\nHello from tunnel test\" | nc -l -p 80 -q 1; done' > /tmp/http-server.log 2>&1 &",
        ]))
        .await
        .expect("Failed to start HTTP server on agent");

    // Give HTTP server time to start
    sleep(Duration::from_secs(2)).await;

    // 5. Start tunnel in background on CLI container
    tracing::info!("Starting tunnel {} -> 8080:80...", device_name);
    infra
        .cli
        .exec(ExecCommand::new(vec![
            "sh",
            "-c",
            &format!(
                "nohup m87 {} tunnel 8080:80 > /tmp/tunnel.log 2>&1 &",
                device_name
            ),
        ]))
        .await
        .expect("Failed to start tunnel");

    // Give tunnel time to establish - poll for tunnel to be listening
    tracing::info!("Waiting for tunnel to establish...");
    let mut tunnel_ready = false;
    for attempt in 1..=20 {
        sleep(Duration::from_secs(1)).await;

        // Check if tunnel is listening on port 8080 using netcat
        // nc -z returns 0 if connection succeeds, non-zero otherwise
        let mut check_result = infra
            .cli
            .exec(ExecCommand::new(vec![
                "sh",
                "-c",
                "nc -z 127.0.0.1 8080 && echo 'listening' || echo 'not listening'",
            ]))
            .await
            .expect("Failed to check tunnel port");
        let check_output = check_result.stdout_to_vec().await.unwrap_or_default();
        let check_str = String::from_utf8_lossy(&check_output);

        if check_str.contains("listening") && !check_str.contains("not listening") {
            tracing::info!("Tunnel is listening on port 8080 (attempt {})", attempt);
            // Wait a bit for HTTP server to be ready after nc -z check consumes a connection
            sleep(Duration::from_secs(2)).await;
            tunnel_ready = true;
            break;
        }

        if attempt % 5 == 0 {
            // Check tunnel log for debugging
            let mut tunnel_log_result = infra
                .cli
                .exec(ExecCommand::new(vec![
                    "sh",
                    "-c",
                    "cat /tmp/tunnel.log 2>/dev/null || echo 'No tunnel log'",
                ]))
                .await
                .expect("Failed to get tunnel log");
            let tunnel_log = tunnel_log_result.stdout_to_vec().await.unwrap_or_default();
            tracing::info!(
                "Still waiting for tunnel... (attempt {})\nTunnel log: {}",
                attempt,
                String::from_utf8_lossy(&tunnel_log)
            );
        }
    }

    // Final tunnel log check
    let mut tunnel_log_result = infra
        .cli
        .exec(ExecCommand::new(vec![
            "sh",
            "-c",
            "cat /tmp/tunnel.log 2>/dev/null || echo 'No tunnel log'",
        ]))
        .await
        .expect("Failed to get tunnel log");
    let tunnel_log = tunnel_log_result.stdout_to_vec().await.unwrap_or_default();
    tracing::info!("Final tunnel log: {}", String::from_utf8_lossy(&tunnel_log));

    // Check agent run log for any errors
    let mut agent_log_result2 = infra
        .agent
        .exec(ExecCommand::new(vec![
            "sh",
            "-c",
            "cat /tmp/agent-run.log 2>/dev/null | tail -30",
        ]))
        .await
        .expect("Failed to get agent log");
    let agent_log2 = agent_log_result2.stdout_to_vec().await.unwrap_or_default();
    tracing::info!("Final agent run log: {}", String::from_utf8_lossy(&agent_log2));

    // Debug: Check what device_id the server registered vs what CLI is using
    tracing::info!(
        "Debug info: device_name={}, device_short_id={}, server_name={}",
        device_name,
        device_short_id,
        server_name
    );

    // Debug: Verify /etc/hosts entries
    let mut hosts_check = infra
        .cli
        .exec(ExecCommand::new(vec!["sh", "-c", "cat /etc/hosts | grep -v '^#'"]))
        .await
        .expect("Failed to check hosts");
    let hosts_output = hosts_check.stdout_to_vec().await.unwrap_or_default();
    tracing::info!(
        "CLI /etc/hosts:\n{}",
        String::from_utf8_lossy(&hosts_output)
    );

    if !tunnel_ready {
        // Get server logs before panicking
        if let Ok(server_logs) = infra.get_server_logs().await {
            tracing::error!("Server logs:\n{}", server_logs);
        }
        panic!("Tunnel did not start listening on port 8080");
    }

    // 4. Curl through tunnel from CLI container
    tracing::info!("Testing tunnel connection...");
    let mut curl_result = infra
        .cli
        .exec(ExecCommand::new(vec![
            "sh",
            "-c",
            "curl -v --max-time 10 http://localhost:8080/ 2>&1",
        ]))
        .await
        .expect("Failed to curl through tunnel");

    let output = curl_result.stdout_to_vec().await.unwrap_or_default();
    let body = String::from_utf8_lossy(&output);
    tracing::info!("Curl response: {}", body);

    // 5. Assert response contains expected content
    assert!(
        body.contains("Hello from tunnel test"),
        "Expected 'Hello from tunnel test' in response, got: {}",
        body
    );

    tracing::info!("Tunnel test passed!");
}
