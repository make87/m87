//! E2E tests for install.sh script

use std::process::Stdio;
use std::sync::OnceLock;
use testcontainers::{runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use tokio::process::Command;

use super::helpers::{exec_shell, E2EError};

const INSTALL_TEST_IMAGE: &str = "m87-install-test:e2e";
const TEST_VERSION: &str = "0.0.0-test";

/// Get the target triple for the current architecture
fn get_target_triple() -> &'static str {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    return "x86_64-unknown-linux-musl";

    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    return "aarch64-unknown-linux-musl";

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    return "aarch64-unknown-linux-musl"; // Container will be linux

    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    return "x86_64-unknown-linux-musl"; // Container will be linux

    #[cfg(not(any(
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
    )))]
    return "x86_64-unknown-linux-musl"; // Default fallback
}

// Build install-test image once per test run
static IMAGE_BUILT: OnceLock<Result<(), String>> = OnceLock::new();

/// Build the install-test Docker image
fn ensure_install_test_image() -> Result<(), String> {
    IMAGE_BUILT
        .get_or_init(|| {
            let workspace_root = std::env::current_dir()
                .map(|p| p.parent().map(|p| p.to_path_buf()).unwrap_or(p))
                .unwrap_or_else(|_| std::path::PathBuf::from(".."));

            tracing::info!("Building install-test image...");
            let output = std::process::Command::new("docker")
                .args([
                    "build",
                    "-t",
                    INSTALL_TEST_IMAGE,
                    "-f",
                    "m87-client/Dockerfile.install-test",
                    ".",
                ])
                .current_dir(&workspace_root)
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .output();

            match output {
                Ok(o) if o.status.success() => {
                    tracing::info!("Install-test image built successfully");
                    Ok(())
                }
                Ok(o) => Err(format!(
                    "Failed to build install-test image: exit code {:?}",
                    o.status.code()
                )),
                Err(e) => Err(format!("Failed to run docker build: {}", e)),
            }
        })
        .clone()
}


/// Copy a file from one container to another via a temp file
async fn copy_file_between_containers(
    src_container_id: &str,
    src_path: &str,
    dest_container_id: &str,
    dest_path: &str,
) -> Result<(), E2EError> {
    // Create temp file
    let temp_dir = tempfile::tempdir()
        .map_err(|e| E2EError::Setup(format!("Failed to create temp dir: {}", e)))?;
    let temp_file = temp_dir.path().join("transfer");

    // Copy from source container
    let output = Command::new("docker")
        .args([
            "cp",
            &format!("{}:{}", src_container_id, src_path),
            temp_file.to_str().unwrap(),
        ])
        .output()
        .await
        .map_err(|e| E2EError::Setup(format!("Failed to copy from source: {}", e)))?;

    if !output.status.success() {
        return Err(E2EError::Setup(format!(
            "docker cp from source failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    // Copy to dest container
    let output = Command::new("docker")
        .args([
            "cp",
            temp_file.to_str().unwrap(),
            &format!("{}:{}", dest_container_id, dest_path),
        ])
        .output()
        .await
        .map_err(|e| E2EError::Setup(format!("Failed to copy to dest: {}", e)))?;

    if !output.status.success() {
        return Err(E2EError::Setup(format!(
            "docker cp to dest failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    Ok(())
}

/// Infrastructure for install script tests
struct InstallTestInfra {
    container: ContainerAsync<GenericImage>,
    /// Container to extract the binary from (m87-client:latest)
    binary_source: ContainerAsync<GenericImage>,
}

impl InstallTestInfra {
    async fn new() -> Result<Self, E2EError> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();

        // Build the install-test image
        ensure_install_test_image().map_err(E2EError::Setup)?;

        // Ensure m87-client:latest is built (need it to extract binary)
        super::setup::ensure_images_built()
            .await
            .map_err(E2EError::Setup)?;

        // Start a container from m87-client:latest to extract the binary
        let binary_source = GenericImage::new("m87-client", "latest")
            .with_entrypoint("sh")
            .with_cmd(vec!["-c", "sleep infinity"])
            .start()
            .await
            .map_err(|e| E2EError::Setup(e.to_string()))?;

        // Start the test container
        let container = GenericImage::new("m87-install-test", "e2e")
            .with_entrypoint("sh")
            .with_cmd(vec!["-c", "sleep infinity"])
            .start()
            .await
            .map_err(|e| E2EError::Setup(e.to_string()))?;

        Ok(Self {
            container,
            binary_source,
        })
    }

    fn container_id(&self) -> &str {
        self.container.id()
    }

    fn source_container_id(&self) -> &str {
        self.binary_source.id()
    }

    /// Copy install.sh to the serve directory via docker cp
    async fn copy_install_script_to_server(&self) -> Result<(), E2EError> {
        // Get workspace root to find install.sh
        let workspace_root = std::env::current_dir()
            .map(|p| p.parent().map(|p| p.to_path_buf()).unwrap_or(p))
            .unwrap_or_else(|_| std::path::PathBuf::from(".."));

        let install_script_path = workspace_root.join("m87-client/install.sh");

        // Create temp file to modify the script with test version
        let temp_dir = tempfile::tempdir()
            .map_err(|e| E2EError::Setup(format!("Failed to create temp dir: {}", e)))?;
        let temp_script = temp_dir.path().join("install.sh");

        // Read original script and replace VERSION line with test version
        let script_content = std::fs::read_to_string(&install_script_path)
            .map_err(|e| E2EError::Setup(format!("Failed to read install.sh: {}", e)))?;
        // Replace the VERSION assignment line entirely to avoid shell expansion issues
        let modified_script = script_content.replace(
            r#"VERSION="${M87_VERSION:-__VERSION__}""#,
            &format!(r#"VERSION="{}""#, TEST_VERSION),
        );

        std::fs::write(&temp_script, modified_script)
            .map_err(|e| E2EError::Setup(format!("Failed to write temp script: {}", e)))?;

        // Copy to container's serve directory
        let output = Command::new("docker")
            .args([
                "cp",
                temp_script.to_str().unwrap(),
                &format!("{}:/srv/download/install.sh", self.container_id()),
            ])
            .output()
            .await
            .map_err(|e| E2EError::Setup(format!("Failed to copy install.sh: {}", e)))?;

        if !output.status.success() {
            return Err(E2EError::Setup(format!(
                "docker cp install.sh failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        Ok(())
    }

    /// Setup the HTTP server with binary, checksums, and install.sh
    async fn setup_download_server(&self, correct_checksum: bool) -> Result<(), E2EError> {
        let target = get_target_triple();
        let binary_name = format!("m87-{}", target);

        // Create serve directory structure
        exec_shell(
            &self.container,
            &format!("mkdir -p /srv/download/v{}", TEST_VERSION),
        )
        .await?;

        // Copy binary from source container to test container
        let dest_binary = format!("/srv/download/v{}/{}", TEST_VERSION, binary_name);
        copy_file_between_containers(
            self.source_container_id(),
            "/app/m87",
            self.container_id(),
            &dest_binary,
        )
        .await?;

        // Generate SHA256SUMS
        if correct_checksum {
            exec_shell(
                &self.container,
                &format!(
                    "cd /srv/download/v{} && sha256sum {} > SHA256SUMS",
                    TEST_VERSION, binary_name
                ),
            )
            .await?;
        } else {
            // Wrong checksum for error testing
            exec_shell(
                &self.container,
                &format!(
                    "echo '0000000000000000000000000000000000000000000000000000000000000000  {}' > /srv/download/v{}/SHA256SUMS",
                    binary_name, TEST_VERSION
                ),
            )
            .await?;
        }

        // Copy install.sh to serve directory (with version baked in)
        self.copy_install_script_to_server().await?;

        // Start HTTP server in background
        exec_shell(
            &self.container,
            "cd /srv/download && nohup python3 -m http.server 8000 > /tmp/http.log 2>&1 &",
        )
        .await?;

        // Wait for server to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        Ok(())
    }

    /// Run install via curl | sh (mimics real install flow)
    async fn run_install(&self, extra_env: &str) -> Result<String, E2EError> {
        // Mimics: curl -fsSL https://get.make87.com | sh
        // Need to export M87_DOWNLOAD_URL so it's available in the piped sh process
        let extra = if extra_env.is_empty() {
            String::new()
        } else {
            format!("export {} && ", extra_env)
        };
        let cmd = format!(
            "{}export M87_DOWNLOAD_URL=http://localhost:8000 && curl -fsSL http://localhost:8000/install.sh | sh 2>&1",
            extra
        );
        exec_shell(&self.container, &cmd).await
    }

    /// Run install via curl | sh expecting failure
    async fn run_install_expect_fail(&self, extra_env: &str) -> Result<String, E2EError> {
        let extra = if extra_env.is_empty() {
            String::new()
        } else {
            format!("export {} && ", extra_env)
        };
        let cmd = format!(
            "({}export M87_DOWNLOAD_URL=http://localhost:8000 && curl -fsSL http://localhost:8000/install.sh | sh 2>&1) || echo 'INSTALL_FAILED'",
            extra
        );
        exec_shell(&self.container, &cmd).await
    }
}

/// Test successful installation via curl | sh
#[tokio::test]
async fn test_install_success() -> Result<(), E2EError> {
    let infra = InstallTestInfra::new().await?;

    // Setup download server with binary, checksums, and install.sh
    infra.setup_download_server(true).await?;

    // Run install via curl | sh (mimics: curl -fsSL https://get.make87.com | sh)
    let output = infra.run_install("").await?;
    tracing::info!("Install output:\n{}", output);

    // Verify binary exists
    let check = exec_shell(&infra.container, "ls -la ~/.local/bin/m87").await?;
    assert!(
        check.contains("m87"),
        "Binary should be installed at ~/.local/bin/m87, got: {}",
        check
    );

    // Verify binary is executable
    let version = exec_shell(
        &infra.container,
        "~/.local/bin/m87 --version 2>&1 || echo 'FAILED'",
    )
    .await?;
    assert!(
        !version.contains("FAILED"),
        "Binary should be executable, got: {}",
        version
    );

    // Verify PATH warning is shown (since ~/.local/bin is not in PATH by default)
    assert!(
        output.contains("not in your PATH") || output.contains("not found in PATH"),
        "Should show PATH warning, got: {}",
        output
    );

    tracing::info!("test_install_success passed!");
    Ok(())
}

/// Test that install creates ~/.local/bin directory
#[tokio::test]
async fn test_install_creates_directory() -> Result<(), E2EError> {
    let infra = InstallTestInfra::new().await?;

    // Ensure directory doesn't exist
    exec_shell(&infra.container, "rm -rf ~/.local/bin").await?;

    // Setup download server
    infra.setup_download_server(true).await?;

    // Run install
    let output = infra.run_install("").await?;
    tracing::info!("Install output:\n{}", output);

    // Verify directory was created
    let check = exec_shell(&infra.container, "ls -la ~/.local/bin/").await?;
    assert!(
        check.contains("m87"),
        "Directory should be created with binary, got: {}",
        check
    );

    // Verify output mentions creating directory
    assert!(
        output.contains("Creating") && output.contains(".local/bin"),
        "Should mention creating directory, got: {}",
        output
    );

    tracing::info!("test_install_creates_directory passed!");
    Ok(())
}

/// Test checksum verification failure
#[tokio::test]
async fn test_install_checksum_failure() -> Result<(), E2EError> {
    let infra = InstallTestInfra::new().await?;

    // Setup server with WRONG checksum
    infra.setup_download_server(false).await?;

    // Run install - should fail
    let output = infra.run_install_expect_fail("").await?;
    tracing::info!("Install output:\n{}", output);

    // Verify it failed with checksum error
    assert!(
        output.contains("Checksum verification failed") || output.contains("INSTALL_FAILED"),
        "Should fail with checksum error, got: {}",
        output
    );

    // Verify binary was NOT installed
    let check = exec_shell(
        &infra.container,
        "ls ~/.local/bin/m87 2>&1 || echo 'NOT_FOUND'",
    )
    .await?;
    assert!(
        check.contains("NOT_FOUND") || check.contains("No such file"),
        "Binary should NOT be installed after checksum failure, got: {}",
        check
    );

    tracing::info!("test_install_checksum_failure passed!");
    Ok(())
}

/// Test handling of missing binary (404)
#[tokio::test]
async fn test_install_missing_binary() -> Result<(), E2EError> {
    let infra = InstallTestInfra::new().await?;

    // Setup download server WITHOUT copying the binary (only install.sh)
    exec_shell(
        &infra.container,
        &format!("mkdir -p /srv/download/v{}", TEST_VERSION),
    )
    .await?;

    // Copy install.sh to serve directory
    infra.copy_install_script_to_server().await?;

    // Start HTTP server (but no binary file)
    exec_shell(
        &infra.container,
        "cd /srv/download && nohup python3 -m http.server 8000 > /tmp/http.log 2>&1 &",
    )
    .await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Run install - should fail
    let output = infra.run_install_expect_fail("").await?;
    tracing::info!("Install output:\n{}", output);

    // Verify it failed
    assert!(
        output.contains("INSTALL_FAILED") || output.contains("Error") || output.contains("404"),
        "Should fail when binary not found, got: {}",
        output
    );

    tracing::info!("test_install_missing_binary passed!");
    Ok(())
}

/// Test that PATH warning is NOT shown when ~/.local/bin is already in PATH
#[tokio::test]
async fn test_install_path_in_path() -> Result<(), E2EError> {
    let infra = InstallTestInfra::new().await?;

    // Setup download server
    infra.setup_download_server(true).await?;

    // Run install with ~/.local/bin already in PATH
    let output = infra.run_install("PATH=$HOME/.local/bin:$PATH").await?;
    tracing::info!("Install output:\n{}", output);

    // Verify binary was installed
    let check = exec_shell(&infra.container, "ls ~/.local/bin/m87").await?;
    assert!(check.contains("m87"), "Binary should be installed");

    // Verify PATH warning is NOT shown
    assert!(
        !output.contains("not in your PATH"),
        "Should NOT show PATH warning when already in PATH, got: {}",
        output
    );

    tracing::info!("test_install_path_in_path passed!");
    Ok(())
}
