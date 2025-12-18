//! Tests for agent command `--org-id` and `--email` arguments

use super::containers::E2EInfra;
use super::fixtures::{AgentRunner, DeviceRegistration};
use super::helpers::{
    clear_owner_reference, exec_shell, read_agent_config, set_owner_reference, E2EError,
};

/// Test that `m87 agent run --org-id <id>` saves to config and device registers
#[tokio::test]
async fn test_agent_run_with_org_id() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Clear existing owner_reference first
    clear_owner_reference(&infra.agent).await?;

    // Verify it was cleared
    let config_before = read_agent_config(&infra.agent).await?;
    tracing::info!("Config before: {}", config_before);
    assert!(
        config_before.contains("\"owner_reference\": null")
            || config_before.contains("\"owner_reference\":null"),
        "owner_reference should be null before test"
    );

    // Start agent run with --org-id argument
    let custom_org = "custom-org@test.local";
    AgentRunner::new(&infra)
        .with_args(&["--org-id", custom_org])
        .start_run()
        .await?;

    // Wait a moment for the config to be saved
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify config was updated with the new owner_reference
    let config_after = read_agent_config(&infra.agent).await?;
    tracing::info!("Config after: {}", config_after);
    assert!(
        config_after.contains(&format!("\"owner_reference\": \"{}\"", custom_org))
            || config_after.contains(&format!("\"owner_reference\":\"{}\"", custom_org)),
        "owner_reference should be set to '{}', got: {}",
        custom_org,
        config_after
    );

    // Complete the registration flow
    let registration = DeviceRegistration::new(&infra);
    let auth_id = registration.wait_for_auth_request().await?;
    tracing::info!("Auth request received: {}", auth_id);

    registration.approve(&auth_id).await?;

    let device = registration.wait_for_registered().await?;
    tracing::info!(
        "Device registered: {} ({})",
        device.name,
        device.short_id
    );

    Ok(())
}

/// Test that `m87 agent run --email <email>` saves to config and device registers
#[tokio::test]
async fn test_agent_run_with_email() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Clear existing owner_reference first
    clear_owner_reference(&infra.agent).await?;

    // Verify it was cleared
    let config_before = read_agent_config(&infra.agent).await?;
    assert!(
        config_before.contains("\"owner_reference\": null")
            || config_before.contains("\"owner_reference\":null"),
        "owner_reference should be null before test"
    );

    // Start agent run with --email argument
    let custom_email = "custom-email@test.local";
    AgentRunner::new(&infra)
        .with_args(&["--email", custom_email])
        .start_run()
        .await?;

    // Wait a moment for the config to be saved
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify config was updated with the new owner_reference
    let config_after = read_agent_config(&infra.agent).await?;
    tracing::info!("Config after: {}", config_after);
    assert!(
        config_after.contains(&format!("\"owner_reference\": \"{}\"", custom_email))
            || config_after.contains(&format!("\"owner_reference\":\"{}\"", custom_email)),
        "owner_reference should be set to '{}', got: {}",
        custom_email,
        config_after
    );

    // Complete the registration flow
    let registration = DeviceRegistration::new(&infra);
    let auth_id = registration.wait_for_auth_request().await?;
    tracing::info!("Auth request received: {}", auth_id);

    registration.approve(&auth_id).await?;

    let device = registration.wait_for_registered().await?;
    tracing::info!(
        "Device registered: {} ({})",
        device.name,
        device.short_id
    );

    Ok(())
}

/// Test that `--org-id` argument overrides existing config value
#[tokio::test]
async fn test_agent_run_args_override_existing_config() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Set an initial owner_reference
    let original_owner = "original@test.local";
    set_owner_reference(&infra.agent, original_owner).await?;

    // Verify it was set
    let config_before = read_agent_config(&infra.agent).await?;
    tracing::info!("Config before: {}", config_before);
    assert!(
        config_before.contains(&format!("\"owner_reference\": \"{}\"", original_owner))
            || config_before.contains(&format!("\"owner_reference\":\"{}\"", original_owner)),
        "owner_reference should be '{}' before override",
        original_owner
    );

    // Start agent run with a different --org-id to override
    let override_owner = "override@test.local";
    AgentRunner::new(&infra)
        .with_args(&["--org-id", override_owner])
        .start_run()
        .await?;

    // Wait a moment for the config to be saved
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify config was updated with the new owner_reference
    let config_after = read_agent_config(&infra.agent).await?;
    tracing::info!("Config after: {}", config_after);
    assert!(
        config_after.contains(&format!("\"owner_reference\": \"{}\"", override_owner))
            || config_after.contains(&format!("\"owner_reference\":\"{}\"", override_owner)),
        "owner_reference should be overridden to '{}', got: {}",
        override_owner,
        config_after
    );

    Ok(())
}

/// Test that `m87 agent enable --org-id` saves to config before systemctl fails
/// Note: systemctl is not available in the container, so we just verify config is saved
#[tokio::test]
async fn test_agent_enable_saves_config_before_systemctl() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Clear existing owner_reference first
    clear_owner_reference(&infra.agent).await?;

    // Verify it was cleared
    let config_before = read_agent_config(&infra.agent).await?;
    assert!(
        config_before.contains("\"owner_reference\": null")
            || config_before.contains("\"owner_reference\":null"),
        "owner_reference should be null before test"
    );

    // Run agent enable command (will fail on systemctl but should save config first)
    let enable_org = "enable-org@test.local";
    let _result = exec_shell(
        &infra.agent,
        &format!("m87 agent enable --now --org-id {} 2>&1 || true", enable_org),
    )
    .await?;

    // Verify config was updated with the new owner_reference despite systemctl failure
    let config_after = read_agent_config(&infra.agent).await?;
    tracing::info!("Config after enable: {}", config_after);
    assert!(
        config_after.contains(&format!("\"owner_reference\": \"{}\"", enable_org))
            || config_after.contains(&format!("\"owner_reference\":\"{}\"", enable_org)),
        "owner_reference should be set to '{}' even though systemctl failed, got: {}",
        enable_org,
        config_after
    );

    Ok(())
}

/// Test that `m87 agent start --org-id` saves to config before systemctl fails
#[tokio::test]
async fn test_agent_start_saves_config_before_systemctl() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Clear existing owner_reference first
    clear_owner_reference(&infra.agent).await?;

    // Run agent start command (will fail on systemctl but should save config first)
    let start_org = "start-org@test.local";
    let _result = exec_shell(
        &infra.agent,
        &format!("m87 agent start --org-id {} 2>&1 || true", start_org),
    )
    .await?;

    // Verify config was updated with the new owner_reference despite systemctl failure
    let config_after = read_agent_config(&infra.agent).await?;
    tracing::info!("Config after start: {}", config_after);
    assert!(
        config_after.contains(&format!("\"owner_reference\": \"{}\"", start_org))
            || config_after.contains(&format!("\"owner_reference\":\"{}\"", start_org)),
        "owner_reference should be set to '{}' even though systemctl failed, got: {}",
        start_org,
        config_after
    );

    Ok(())
}

/// Test that `m87 agent restart --org-id` saves to config before systemctl fails
#[tokio::test]
async fn test_agent_restart_saves_config_before_systemctl() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Clear existing owner_reference first
    clear_owner_reference(&infra.agent).await?;

    // Run agent restart command (will fail on systemctl but should save config first)
    let restart_org = "restart-org@test.local";
    let _result = exec_shell(
        &infra.agent,
        &format!("m87 agent restart --org-id {} 2>&1 || true", restart_org),
    )
    .await?;

    // Verify config was updated with the new owner_reference despite systemctl failure
    let config_after = read_agent_config(&infra.agent).await?;
    tracing::info!("Config after restart: {}", config_after);
    assert!(
        config_after.contains(&format!("\"owner_reference\": \"{}\"", restart_org))
            || config_after.contains(&format!("\"owner_reference\":\"{}\"", restart_org)),
        "owner_reference should be set to '{}' even though systemctl failed, got: {}",
        restart_org,
        config_after
    );

    Ok(())
}
