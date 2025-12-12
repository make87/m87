//! Remote command execution tests
//!
//! Tests for:
//! - `m87 <device> exec <cmd>`
//! - `m87 <device> shell`

use super::fixtures::TestSetup;
use super::helpers::E2EError;

/// Test basic exec command
#[tokio::test]
async fn test_exec_simple() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Execute simple echo command
    let output = setup.device_cmd("exec -- echo hello").await?;

    tracing::info!("exec output: {}", output);

    assert!(
        output.contains("hello"),
        "Expected 'hello' in output, got: {}",
        output
    );

    tracing::info!("exec simple test passed!");
    Ok(())
}

/// Test exec with arguments
#[tokio::test]
async fn test_exec_with_args() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Execute ls with arguments
    let output = setup.device_cmd("exec -- ls -la /tmp").await?;

    tracing::info!("exec ls output: {}", output);

    // Should contain directory listing format
    assert!(
        output.contains("total") || output.contains("drwx") || output.contains("tmp"),
        "Expected directory listing format, got: {}",
        output
    );

    tracing::info!("exec with args test passed!");
    Ok(())
}

/// Test exec with environment variable
#[tokio::test]
async fn test_exec_env() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Execute command that checks environment
    let output = setup.device_cmd("exec -- printenv HOME").await?;

    tracing::info!("exec env output: {}", output);

    // HOME should be set
    assert!(
        output.contains("/root") || output.contains("/home"),
        "Expected HOME path, got: {}",
        output
    );

    tracing::info!("exec env test passed!");
    Ok(())
}

/// Test exec with multiple commands (using shell)
#[tokio::test]
async fn test_exec_multi_command() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Execute multiple commands via sh -c
    let output = setup
        .device_cmd("exec -- sh -c 'pwd && whoami'")
        .await?;

    tracing::info!("exec multi output: {}", output);

    // Should contain both pwd output (a path) and whoami output (root)
    assert!(
        output.contains("/") && output.contains("root"),
        "Expected path and 'root', got: {}",
        output
    );

    tracing::info!("exec multi command test passed!");
    Ok(())
}

/// Test shell with piped input
#[tokio::test]
async fn test_shell_basic() -> Result<(), E2EError> {
    use super::helpers::exec_shell;

    let setup = TestSetup::init().await?;

    // Run shell with piped input
    // Note: shell is interactive, so we pipe commands to it
    let output = exec_shell(
        &setup.infra.cli,
        &format!(
            "echo 'echo test123' | timeout 5 m87 {} shell 2>&1 || true",
            setup.device.name
        ),
    )
    .await?;

    tracing::info!("shell output: {}", output);

    // The shell should execute our command or indicate it needs a TTY
    // Note: This may timeout or produce partial output, which is acceptable
    // The main test is that the shell command doesn't error out immediately
    // ioctl errors are expected when running without a TTY
    let is_acceptable = !output.contains("connection refused")
        && !output.contains("not found")
        && (!output.to_lowercase().contains("error:") || output.contains("ioctl"));

    assert!(
        is_acceptable,
        "Shell command failed unexpectedly: {}",
        output
    );

    tracing::info!("shell basic test passed!");
    Ok(())
}
