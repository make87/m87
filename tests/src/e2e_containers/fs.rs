//! Filesystem E2E tests (cp and sync commands)
//!
//! Path conventions (scp-style):
//! - `device:file.txt` → relative path, resolves to ~/file.txt (home directory)
//! - `device:~/file.txt` → explicit home directory
//! - `device:/absolute/path` → absolute path on the remote filesystem

use super::containers::E2EInfra;
use super::fixtures::{AgentRunner, DeviceRegistration};
use super::helpers::{exec_shell, read_log, E2EError, SniSetup};

/// Test copying a file from local (CLI) to remote (agent)
///
/// 1. Register device
/// 2. Setup SNI for CLI tunnel
/// 3. Start agent with control tunnel
/// 4. Create test file on CLI
/// 5. Run cp command to copy to agent (using scp-style relative path)
/// 6. Verify file exists on agent with correct content
#[tokio::test]
async fn test_cp_local_to_remote() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Step 1: Register device
    tracing::info!("Registering device...");
    let device = DeviceRegistration::new(&infra).register_full().await?;
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

    // Wait a bit for agent's SSH/SFTP server to be fully ready
    tracing::info!("Waiting for agent SSH server to be ready...");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Step 4: Create source file on CLI container
    // Note: CLI reads from local filesystem directly, not through SFTP
    tracing::info!("Creating test file on CLI...");
    exec_shell(
        &infra.cli,
        "echo 'Hello from CLI to Agent' > /tmp/test-source.txt",
    )
    .await?;

    // Verify the source file was created
    let source_content = exec_shell(&infra.cli, "cat /tmp/test-source.txt").await?;
    tracing::info!("Source file content: {}", source_content);

    // Step 5: Run cp command to copy file to agent
    // Using scp-style relative path: "device:test-dest.txt" resolves to ~/test-dest.txt
    // In Docker container running as root, ~ = /root
    tracing::info!("Copying file from CLI to agent...");
    let cp_output = exec_shell(
        &infra.cli,
        &format!(
            "RUST_LOG=debug m87 cp /tmp/test-source.txt {}:test-dest.txt 2>&1; echo \"Exit code: $?\"",
            device.name
        ),
    )
    .await?;
    tracing::info!("cp output: {}", cp_output);

    // Give some time for the file to be written
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Check agent run log for any errors
    let agent_log = read_log(&infra.agent, "/tmp/agent-run.log").await?;
    tracing::info!("Agent run log:\n{}", agent_log);

    // Step 6: Verify file exists on agent with correct content
    // Relative path "test-dest.txt" resolves to ~/test-dest.txt = /root/test-dest.txt
    tracing::info!("Verifying file on agent...");

    // First check if file exists at expected location
    let file_exists = exec_shell(&infra.agent, "ls -la /root/test-dest.txt 2>&1").await?;
    tracing::info!("File listing: {}", file_exists);

    let dest_content = exec_shell(&infra.agent, "cat /root/test-dest.txt 2>&1").await?;
    tracing::info!("Destination file content: {}", dest_content);

    assert!(
        dest_content.contains("Hello from CLI to Agent"),
        "Expected 'Hello from CLI to Agent' in file, got: {}",
        dest_content
    );

    tracing::info!("cp local->remote test passed!");
    Ok(())
}

/// Test copying a file from remote (agent) to local (CLI)
///
/// 1. Register device
/// 2. Setup SNI for CLI tunnel
/// 3. Start agent with control tunnel
/// 4. Create test file on agent
/// 5. Run cp command to copy to CLI (using scp-style relative path)
/// 6. Verify file exists on CLI with correct content
#[tokio::test]
async fn test_cp_remote_to_local() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Step 1: Register device
    tracing::info!("Registering device...");
    let device = DeviceRegistration::new(&infra).register_full().await?;
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

    // Wait a bit for agent's SSH/SFTP server to be fully ready
    tracing::info!("Waiting for agent SSH server to be ready...");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Step 4: Create source file on agent container
    // File at /root/agent-source.txt is accessible via relative path "agent-source.txt"
    // (scp-style: relative paths resolve to home directory)
    tracing::info!("Creating test file on agent...");
    exec_shell(
        &infra.agent,
        "echo 'Hello from Agent to CLI' > /root/agent-source.txt",
    )
    .await?;

    // Verify the source file was created
    let source_content = exec_shell(&infra.agent, "cat /root/agent-source.txt").await?;
    tracing::info!("Source file content: {}", source_content);

    // Step 5: Run cp command to copy file to CLI
    // Using scp-style relative path: "device:agent-source.txt" resolves to ~/agent-source.txt
    tracing::info!("Copying file from agent to CLI...");
    let cp_output = exec_shell(
        &infra.cli,
        &format!(
            "m87 cp {}:agent-source.txt /tmp/local-copy.txt 2>&1",
            device.name
        ),
    )
    .await?;
    tracing::info!("cp output: {}", cp_output);

    // Step 6: Verify file exists on CLI with correct content
    tracing::info!("Verifying file on CLI...");
    let dest_content = exec_shell(&infra.cli, "cat /tmp/local-copy.txt 2>&1").await?;
    tracing::info!("Destination file content: {}", dest_content);

    assert!(
        dest_content.contains("Hello from Agent to CLI"),
        "Expected 'Hello from Agent to CLI' in file, got: {}",
        dest_content
    );

    tracing::info!("cp remote->local test passed!");
    Ok(())
}

/// Test syncing a directory from local (CLI) to remote (agent)
///
/// 1. Register device
/// 2. Setup SNI for CLI tunnel
/// 3. Start agent with control tunnel
/// 4. Create test directory with multiple files on CLI
/// 5. Run sync command (destination auto-created)
/// 6. Verify all files exist on agent with correct content
#[tokio::test]
async fn test_sync_directory() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Step 1: Register device
    tracing::info!("Registering device...");
    let device = DeviceRegistration::new(&infra).register_full().await?;
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

    // Wait a bit for agent's SSH/SFTP server to be fully ready
    tracing::info!("Waiting for agent SSH server to be ready...");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Step 4: Create test directory structure on CLI
    tracing::info!("Creating test directory on CLI...");
    exec_shell(
        &infra.cli,
        "mkdir -p /tmp/sync-source/subdir && \
         echo 'File 1 content' > /tmp/sync-source/file1.txt && \
         echo 'File 2 content' > /tmp/sync-source/file2.txt && \
         echo 'Nested file content' > /tmp/sync-source/subdir/nested.txt",
    )
    .await?;

    // Verify the source directory was created
    let source_files = exec_shell(&infra.cli, "find /tmp/sync-source -type f | sort").await?;
    tracing::info!("Source files: {}", source_files);

    // Step 5: Run sync command
    // Using scp-style relative path: "device:sync-dest/" resolves to ~/sync-dest/
    // The destination directory will be auto-created if it doesn't exist (rsync-style)
    tracing::info!("Syncing directory from CLI to agent...");
    let sync_output = exec_shell(
        &infra.cli,
        &format!(
            "m87 sync /tmp/sync-source/ {}:sync-dest/ 2>&1",
            device.name
        ),
    )
    .await?;
    tracing::info!("sync output: {}", sync_output);

    // Step 6: Verify all files exist on agent with correct content
    // Relative path "sync-dest/" resolves to ~/sync-dest/ = /root/sync-dest/
    tracing::info!("Verifying files on agent...");

    // Check file1.txt
    let file1_content = exec_shell(&infra.agent, "cat /root/sync-dest/file1.txt 2>&1").await?;
    assert!(
        file1_content.contains("File 1 content"),
        "Expected 'File 1 content' in file1.txt, got: {}",
        file1_content
    );

    // Check file2.txt
    let file2_content = exec_shell(&infra.agent, "cat /root/sync-dest/file2.txt 2>&1").await?;
    assert!(
        file2_content.contains("File 2 content"),
        "Expected 'File 2 content' in file2.txt, got: {}",
        file2_content
    );

    // Check nested file
    let nested_content =
        exec_shell(&infra.agent, "cat /root/sync-dest/subdir/nested.txt 2>&1").await?;
    assert!(
        nested_content.contains("Nested file content"),
        "Expected 'Nested file content' in subdir/nested.txt, got: {}",
        nested_content
    );

    // List all synced files
    let dest_files = exec_shell(&infra.agent, "find /root/sync-dest -type f | sort").await?;
    tracing::info!("Destination files: {}", dest_files);

    tracing::info!("sync directory test passed!");
    Ok(())
}

/// Test syncing with --delete flag to remove extra files
///
/// 1. Setup infrastructure and sync initial directory
/// 2. Create an extra file on agent destination
/// 3. Run sync with --delete flag
/// 4. Verify extra file was deleted
#[tokio::test]
async fn test_sync_with_delete() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Step 1: Register device
    tracing::info!("Registering device...");
    let device = DeviceRegistration::new(&infra).register_full().await?;
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

    // Wait a bit for agent's SSH/SFTP server to be fully ready
    tracing::info!("Waiting for agent SSH server to be ready...");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Step 4: Create test directory on CLI
    tracing::info!("Creating test directory on CLI...");
    exec_shell(
        &infra.cli,
        "mkdir -p /tmp/sync-delete-source && \
         echo 'Keep me' > /tmp/sync-delete-source/keep.txt",
    )
    .await?;

    // Step 5: Create destination directory on agent with an extra file
    // Relative path "sync-delete-dest" = ~/sync-delete-dest = /root/sync-delete-dest
    tracing::info!("Creating destination directory on agent with extra file...");
    exec_shell(
        &infra.agent,
        "mkdir -p /root/sync-delete-dest && \
         echo 'Delete me' > /root/sync-delete-dest/extra.txt",
    )
    .await?;

    // Verify extra file exists
    let extra_exists = exec_shell(
        &infra.agent,
        "test -f /root/sync-delete-dest/extra.txt && echo 'exists' || echo 'not found'",
    )
    .await?;
    assert!(
        extra_exists.contains("exists"),
        "Extra file should exist before sync --delete"
    );

    // Step 6: Run sync with --delete flag
    // Using scp-style relative path: "device:sync-delete-dest/" resolves to ~/sync-delete-dest/
    tracing::info!("Syncing with --delete flag...");
    let sync_output = exec_shell(
        &infra.cli,
        &format!(
            "m87 sync --delete /tmp/sync-delete-source/ {}:sync-delete-dest/ 2>&1",
            device.name
        ),
    )
    .await?;
    tracing::info!("sync --delete output: {}", sync_output);

    // Step 7: Verify keep.txt exists
    let keep_content = exec_shell(&infra.agent, "cat /root/sync-delete-dest/keep.txt 2>&1").await?;
    assert!(
        keep_content.contains("Keep me"),
        "Expected 'Keep me' in keep.txt, got: {}",
        keep_content
    );

    // Step 8: Verify extra.txt was deleted
    let extra_after = exec_shell(
        &infra.agent,
        "test -f /root/sync-delete-dest/extra.txt && echo 'exists' || echo 'not found'",
    )
    .await?;
    assert!(
        extra_after.contains("not found"),
        "Extra file should be deleted after sync --delete, but got: {}",
        extra_after
    );

    tracing::info!("sync --delete test passed!");
    Ok(())
}

/// Test sync --dry-run flag (shows what would happen without making changes)
#[tokio::test]
async fn test_sync_dry_run() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Register device
    tracing::info!("Registering device...");
    let device = DeviceRegistration::new(&infra).register_full().await?;

    // Setup SNI
    let sni = SniSetup::from_cli(&infra.cli).await?;
    sni.setup_both(&infra.agent, &infra.cli, &device.short_id)
        .await?;

    // Start agent
    let agent = AgentRunner::new(&infra);
    agent.start_with_tunnel().await?;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Create source directory
    exec_shell(
        &infra.cli,
        "mkdir -p /tmp/dry-run-source && \
         echo 'Test content' > /tmp/dry-run-source/test.txt",
    )
    .await?;

    // Run sync with --dry-run (destination doesn't exist)
    tracing::info!("Running sync with --dry-run...");
    let sync_output = exec_shell(
        &infra.cli,
        &format!(
            "m87 sync --dry-run /tmp/dry-run-source/ {}:dry-run-dest/ 2>&1",
            device.name
        ),
    )
    .await?;
    tracing::info!("sync --dry-run output: {}", sync_output);

    // Verify output mentions dry-run
    assert!(
        sync_output.contains("[dry-run]") || sync_output.contains("would upload"),
        "Expected dry-run output, got: {}",
        sync_output
    );

    // Verify destination was NOT created
    let dest_exists = exec_shell(
        &infra.agent,
        "test -d /root/dry-run-dest && echo 'exists' || echo 'not found'",
    )
    .await?;
    assert!(
        dest_exists.contains("not found"),
        "Destination should not exist after dry-run, got: {}",
        dest_exists
    );

    tracing::info!("sync --dry-run test passed!");
    Ok(())
}

/// Test sync --exclude flag (skip matching files)
#[tokio::test]
async fn test_sync_exclude() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // Register device
    tracing::info!("Registering device...");
    let device = DeviceRegistration::new(&infra).register_full().await?;

    // Setup SNI
    let sni = SniSetup::from_cli(&infra.cli).await?;
    sni.setup_both(&infra.agent, &infra.cli, &device.short_id)
        .await?;

    // Start agent
    let agent = AgentRunner::new(&infra);
    agent.start_with_tunnel().await?;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Create source directory with various files
    exec_shell(
        &infra.cli,
        "mkdir -p /tmp/exclude-source/.git && \
         echo 'Keep this' > /tmp/exclude-source/keep.txt && \
         echo 'Git file' > /tmp/exclude-source/.git/config && \
         echo 'Log file' > /tmp/exclude-source/debug.log",
    )
    .await?;

    // Run sync with --exclude for .git and *.log
    tracing::info!("Running sync with --exclude...");
    let sync_output = exec_shell(
        &infra.cli,
        &format!(
            "m87 sync --exclude .git --exclude '*.log' /tmp/exclude-source/ {}:exclude-dest/ 2>&1",
            device.name
        ),
    )
    .await?;
    tracing::info!("sync --exclude output: {}", sync_output);

    // Verify keep.txt exists
    let keep_exists = exec_shell(
        &infra.agent,
        "test -f /root/exclude-dest/keep.txt && echo 'exists' || echo 'not found'",
    )
    .await?;
    assert!(
        keep_exists.contains("exists"),
        "keep.txt should exist, got: {}",
        keep_exists
    );

    // Verify .git directory was NOT synced
    let git_exists = exec_shell(
        &infra.agent,
        "test -d /root/exclude-dest/.git && echo 'exists' || echo 'not found'",
    )
    .await?;
    assert!(
        git_exists.contains("not found"),
        ".git should be excluded, got: {}",
        git_exists
    );

    // Verify *.log files were NOT synced
    let log_exists = exec_shell(
        &infra.agent,
        "test -f /root/exclude-dest/debug.log && echo 'exists' || echo 'not found'",
    )
    .await?;
    assert!(
        log_exists.contains("not found"),
        "*.log should be excluded, got: {}",
        log_exists
    );

    tracing::info!("sync --exclude test passed!");
    Ok(())
}
