//! Container exec wrappers for E2E tests

use testcontainers::{core::ExecCommand, ContainerAsync, GenericImage};

use super::E2EError;

/// Execute command and return stdout as String
pub async fn exec_cmd(
    container: &ContainerAsync<GenericImage>,
    cmd: &[&str],
) -> Result<String, E2EError> {
    let cmd_vec: Vec<String> = cmd.iter().map(|s| s.to_string()).collect();
    let mut result = container
        .exec(ExecCommand::new(cmd_vec))
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;

    let stdout = result.stdout_to_vec().await.unwrap_or_default();
    Ok(String::from_utf8_lossy(&stdout).trim().to_string())
}

/// Execute shell command (wraps in sh -c)
pub async fn exec_shell(
    container: &ContainerAsync<GenericImage>,
    shell_cmd: &str,
) -> Result<String, E2EError> {
    exec_cmd(container, &["sh", "-c", shell_cmd]).await
}

/// Execute command in background (nohup)
pub async fn exec_background(
    container: &ContainerAsync<GenericImage>,
    cmd: &str,
    log_file: &str,
) -> Result<(), E2EError> {
    exec_shell(
        container,
        &format!("nohup {} > {} 2>&1 &", cmd, log_file),
    )
    .await?;
    Ok(())
}

/// Read log file from container
pub async fn read_log(
    container: &ContainerAsync<GenericImage>,
    log_path: &str,
) -> Result<String, E2EError> {
    exec_shell(
        container,
        &format!("cat {} 2>/dev/null || echo ''", log_path),
    )
    .await
}

/// Check if a string appears in a log file
pub async fn log_contains(
    container: &ContainerAsync<GenericImage>,
    log_path: &str,
    needle: &str,
) -> Result<bool, E2EError> {
    let log = read_log(container, log_path).await?;
    Ok(log.contains(needle))
}

/// Check if a port is listening using netcat
pub async fn is_port_listening(
    container: &ContainerAsync<GenericImage>,
    port: u16,
) -> Result<bool, E2EError> {
    let result = exec_shell(
        container,
        &format!(
            "nc -z 127.0.0.1 {} && echo 'listening' || echo 'not listening'",
            port
        ),
    )
    .await?;
    Ok(result.contains("listening") && !result.contains("not listening"))
}

/// Read config.json from runtime container
pub async fn read_runtime_config(
    container: &ContainerAsync<GenericImage>,
) -> Result<String, E2EError> {
    exec_shell(container, "cat /root/.config/m87/config.json").await
}

/// Update owner_reference in runtime config using jq-like sed replacement
pub async fn set_owner_reference(
    container: &ContainerAsync<GenericImage>,
    owner: &str,
) -> Result<(), E2EError> {
    // Use sed to replace the owner_reference value in the JSON
    let cmd = format!(
        r#"sed -i 's/"owner_reference": *"[^"]*"/"owner_reference": "{}"/' /root/.config/m87/config.json && sed -i 's/"owner_reference": *null/"owner_reference": "{}"/' /root/.config/m87/config.json"#,
        owner, owner
    );
    exec_shell(container, &cmd).await?;
    Ok(())
}

/// Clear owner_reference from runtime config (set to null)
pub async fn clear_owner_reference(
    container: &ContainerAsync<GenericImage>,
) -> Result<(), E2EError> {
    // Use a simpler sed pattern that handles various JSON formatting
    exec_shell(
        container,
        r#"sed -i 's/"owner_reference":[[:space:]]*"[^"]*"/"owner_reference": null/' /root/.config/m87/config.json"#,
    )
    .await?;
    Ok(())
}
