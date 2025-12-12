//! Remote directory listing tests
//!
//! Tests for `m87 ls <device>:<path>`

use super::fixtures::TestSetup;
use super::helpers::E2EError;

/// Test listing home directory with relative path
#[tokio::test]
async fn test_ls_home() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Create a test file in home directory
    setup
        .create_agent_file("/root/ls-test-file.txt", "test content")
        .await?;

    // List using device:path syntax (relative = home dir)
    let output = setup
        .m87_cmd(&format!("ls {}:", setup.device.name))
        .await?;

    tracing::info!("ls home output: {}", output);

    // Should show the test file we created
    assert!(
        output.contains("ls-test-file.txt") || output.contains("ls-test"),
        "Expected test file in listing, got: {}",
        output
    );

    tracing::info!("ls home test passed!");
    Ok(())
}

/// Test listing with absolute path
#[tokio::test]
async fn test_ls_absolute() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // List /tmp (always exists)
    let output = setup
        .m87_cmd(&format!("ls {}:/tmp", setup.device.name))
        .await?;

    tracing::info!("ls /tmp output: {}", output);

    // /tmp should exist and be listable
    // Output should look like a directory listing or be empty
    assert!(
        !output.to_lowercase().contains("error")
            && !output.contains("No such file"),
        "Failed to list /tmp: {}",
        output
    );

    tracing::info!("ls absolute test passed!");
    Ok(())
}

/// Test listing with relative path (resolves to home)
#[tokio::test]
async fn test_ls_relative() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Create a subdirectory in home
    setup.create_agent_dir("/root/test-subdir").await?;
    setup
        .create_agent_file("/root/test-subdir/nested-file.txt", "nested")
        .await?;

    // List using relative path (should resolve to ~/test-subdir)
    let output = setup
        .m87_cmd(&format!("ls {}:test-subdir", setup.device.name))
        .await?;

    tracing::info!("ls relative output: {}", output);

    // Should show the nested file
    assert!(
        output.contains("nested-file.txt") || output.contains("nested"),
        "Expected nested file in listing, got: {}",
        output
    );

    tracing::info!("ls relative test passed!");
    Ok(())
}

/// Test listing non-existent path
#[tokio::test]
async fn test_ls_nonexistent() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Try to list a path that doesn't exist
    let output = setup
        .m87_cmd(&format!(
            "ls {}:/nonexistent-path-12345",
            setup.device.name
        ))
        .await?;

    tracing::info!("ls nonexistent output: {}", output);

    // Should show an error or "no such file"
    assert!(
        output.to_lowercase().contains("error")
            || output.to_lowercase().contains("no such")
            || output.to_lowercase().contains("not found")
            || output.is_empty(),
        "Expected error for nonexistent path, got: {}",
        output
    );

    tracing::info!("ls nonexistent test passed!");
    Ok(())
}

/// Test listing a directory containing a specific file
#[tokio::test]
async fn test_ls_with_file() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // Create a test directory with a file
    setup.create_agent_dir("/root/ls-file-test").await?;
    setup
        .create_agent_file("/root/ls-file-test/test-file.txt", "hello world")
        .await?;

    // List the directory containing the file
    let output = setup
        .m87_cmd(&format!("ls {}:ls-file-test", setup.device.name))
        .await?;

    tracing::info!("ls with file output: {}", output);

    // Should show the file name in the listing
    assert!(
        output.contains("test-file.txt") || output.contains("test-file"),
        "Expected file in listing, got: {}",
        output
    );

    tracing::info!("ls with file test passed!");
    Ok(())
}
