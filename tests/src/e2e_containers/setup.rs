use std::process::{Command, Stdio};
use std::sync::Once;

static BUILD_IMAGES: Once = Once::new();
static CREATE_NETWORK: Once = Once::new();

pub const NETWORK_NAME: &str = "m87-e2e-network";
pub const SERVER_IMAGE_NAME: &str = "m87-server";
pub const SERVER_IMAGE_TAG: &str = "e2e";
pub const CLIENT_IMAGE_NAME: &str = "m87-client";
pub const CLIENT_IMAGE_TAG: &str = "e2e";
// Full image references for building
pub const SERVER_IMAGE: &str = "m87-server:e2e";
pub const CLIENT_IMAGE: &str = "m87-client:e2e";

/// Build Docker images for E2E tests (runs once per test run)
/// Always rebuilds to pick up code changes - Docker layer caching makes this fast when unchanged
pub fn ensure_images_built() -> Result<(), String> {
    let mut build_error: Option<String> = None;

    BUILD_IMAGES.call_once(|| {
        // Get workspace root (parent of tests/)
        let workspace_root = std::env::current_dir()
            .map(|p| p.parent().map(|p| p.to_path_buf()).unwrap_or(p))
            .unwrap_or_else(|_| std::path::PathBuf::from(".."));

        // Build server image
        tracing::info!("Building {} (Docker cache will speed up if unchanged)...", SERVER_IMAGE);
        let status = Command::new("docker")
            .args([
                "build",
                "-t",
                SERVER_IMAGE,
                "-f",
                "m87-server/Dockerfile",
                "--build-arg",
                "BUILD_PROFILE=release",
                ".",
            ])
            .current_dir(&workspace_root)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status();

        match status {
            Ok(s) if !s.success() => {
                build_error = Some(format!("Failed to build server image (exit code: {:?})", s.code()));
                return;
            }
            Err(e) => {
                build_error = Some(format!("Failed to run docker build for server: {}", e));
                return;
            }
            _ => {
                tracing::info!("Server image built successfully");
            }
        }

        // Build client image
        tracing::info!("Building {} (Docker cache will speed up if unchanged)...", CLIENT_IMAGE);
        let status = Command::new("docker")
            .args([
                "build",
                "-t",
                CLIENT_IMAGE,
                "-f",
                "m87-client/Dockerfile",
                "--build-arg",
                "BUILD_PROFILE=release",
                ".",
            ])
            .current_dir(&workspace_root)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status();

        match status {
            Ok(s) if !s.success() => {
                build_error = Some(format!("Failed to build client image (exit code: {:?})", s.code()));
            }
            Err(e) => {
                build_error = Some(format!("Failed to run docker build for client: {}", e));
            }
            _ => {
                tracing::info!("Client image built successfully");
            }
        }
    });

    match build_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Create Docker network for container communication (runs once per test run)
pub fn ensure_network_created() -> Result<(), String> {
    let mut network_error: Option<String> = None;

    CREATE_NETWORK.call_once(|| {
        tracing::info!("Creating E2E Docker network: {}", NETWORK_NAME);

        // First try to remove existing network (ignore errors)
        let _ = Command::new("docker")
            .args(["network", "rm", NETWORK_NAME])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        // Create network
        let result = Command::new("docker")
            .args(["network", "create", NETWORK_NAME])
            .output();

        match result {
            Ok(output) if !output.status.success() => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // Ignore "already exists" error
                if !stderr.contains("already exists") {
                    network_error = Some(format!("Failed to create network: {}", stderr));
                }
            }
            Err(e) => {
                network_error = Some(format!("Failed to run docker network create: {}", e));
            }
            _ => {
                tracing::info!("Docker network created successfully");
            }
        }
    });

    match network_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Clean up network after tests (call manually if needed)
#[allow(dead_code)]
pub fn cleanup_network() {
    let _ = Command::new("docker")
        .args(["network", "rm", NETWORK_NAME])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}
