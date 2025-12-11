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
