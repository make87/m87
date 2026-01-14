use std::time::Duration;
use testcontainers::{
    core::{ExecCommand, IntoContainerPort, Mount, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::time::sleep;
use uuid::Uuid;

use super::helpers::E2EError;
use super::setup::{
    ensure_images_built, ensure_network_created, CLIENT_IMAGE_NAME, CLIENT_IMAGE_TAG, NETWORK_NAME,
    SERVER_IMAGE_NAME, SERVER_IMAGE_TAG,
};

const ADMIN_KEY: &str = "e2e-admin-key";

/// Infrastructure for E2E tests - manages all containers
pub struct E2EInfra {
    pub mongo: ContainerAsync<GenericImage>,
    pub server: ContainerAsync<GenericImage>,
    pub runtime: ContainerAsync<GenericImage>,
    pub cli: ContainerAsync<GenericImage>,
    /// Unique ID for this test run (used for container names)
    run_id: String,
}

impl E2EInfra {
    /// Initialize test infrastructure with tracing, images, and network
    ///
    /// This is the preferred entry point for tests. It sets up tracing,
    /// ensures Docker images are built, creates the network, and starts containers.
    pub async fn init() -> Result<Self, E2EError> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();

        ensure_images_built()
            .await
            .map_err(|e| E2EError::Setup(e.to_string()))?;
        ensure_network_created().map_err(|e| E2EError::Setup(e.to_string()))?;

        Self::start()
            .await
            .map_err(|e| E2EError::Setup(e.to_string()))
    }

    /// Start all E2E infrastructure containers
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        // Generate unique run ID for this test
        let run_id = Uuid::new_v4().to_string()[..8].to_string();
        tracing::info!("Starting E2E infrastructure (run_id: {})...", run_id);

        // Start MongoDB
        let mongo = Self::start_mongo(&run_id).await?;
        tracing::info!("MongoDB started");

        // Wait for MongoDB to be ready
        sleep(Duration::from_secs(2)).await;

        // Start server
        let server = Self::start_server(&run_id).await?;
        tracing::info!("Server started");

        // Wait for server to be ready
        Self::wait_for_server(&server).await?;

        // Start runtime and CLI containers
        let runtime = Self::start_runtime(&run_id).await?;
        let cli = Self::start_cli(&run_id).await?;
        tracing::info!("Runtime and CLI containers started");

        // Configure runtime and CLI
        Self::configure_runtime(&runtime, &run_id).await?;
        Self::configure_cli(&cli, &run_id).await?;
        tracing::info!("Runtime and CLI configured");

        Ok(Self {
            mongo,
            server,
            runtime,
            cli,
            run_id,
        })
    }

    async fn start_mongo(run_id: &str) -> Result<ContainerAsync<GenericImage>, Box<dyn std::error::Error>> {
        let container_name = format!("e2e-mongo-{}", run_id);
        let image = GenericImage::new("mongo", "7")
            .with_exposed_port(27017.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
            .with_network(NETWORK_NAME)
            .with_container_name(&container_name);

        let container = image.start().await?;
        Ok(container)
    }

    async fn start_server(run_id: &str) -> Result<ContainerAsync<GenericImage>, Box<dyn std::error::Error>> {
        let container_name = format!("e2e-server-{}", run_id);
        let mongo_name = format!("e2e-mongo-{}", run_id);
        let image = GenericImage::new(SERVER_IMAGE_NAME, SERVER_IMAGE_TAG)
            .with_exposed_port(8084.tcp())
            .with_env_var("RUST_LOG", "info,m87_server=debug")
            .with_env_var("MONGO_URI", format!("mongodb://{}:27017", mongo_name))
            .with_env_var("MONGO_DB", "e2e-tests")
            .with_env_var("PUBLIC_ADDRESS", &container_name)
            .with_env_var("UNIFIED_PORT", "8084")
            .with_env_var("STAGING", "1")
            .with_env_var("ADMIN_KEY", ADMIN_KEY)
            .with_env_var("CERTIFICATE_PATH", "/data/m87/certs/")
            .with_network(NETWORK_NAME)
            .with_container_name(&container_name);

        let container = image.start().await?;
        Ok(container)
    }

    async fn start_runtime(run_id: &str) -> Result<ContainerAsync<GenericImage>, Box<dyn std::error::Error>> {
        let container_name = format!("e2e-runtime-{}", run_id);
        // Note: with_entrypoint and with_cmd must be called on GenericImage before ImageExt methods
        let mut image = GenericImage::new(CLIENT_IMAGE_NAME, CLIENT_IMAGE_TAG)
            .with_entrypoint("sh")
            .with_cmd(vec!["-c", "sleep infinity"])
            .with_env_var("RUST_LOG", "info,m87_client=debug")
            .with_network(NETWORK_NAME)
            .with_container_name(&container_name);

        // Mount Docker socket for Docker-in-Docker support
        // Handle cross-platform Docker socket paths:
        // - Linux: /var/run/docker.sock
        // - macOS Docker Desktop: ~/.docker/run/docker.sock (or /var/run/docker.sock if symlink enabled)
        // - Windows WSL2: /var/run/docker.sock
        let docker_socket = get_docker_socket_path();
        if let Some(socket_path) = docker_socket {
            tracing::info!("Mounting Docker socket from: {}", socket_path);
            image = image.with_mount(Mount::bind_mount(
                socket_path,
                "/var/run/docker.sock",
            ));
        } else {
            tracing::warn!("Docker socket not found - Docker tests may fail");
        }

        let container = image.start().await?;
        Ok(container)
    }

    async fn start_cli(run_id: &str) -> Result<ContainerAsync<GenericImage>, Box<dyn std::error::Error>> {
        let container_name = format!("e2e-cli-{}", run_id);
        // Note: with_entrypoint and with_cmd must be called on GenericImage before ImageExt methods
        // IMPORTANT: We intentionally do NOT mount the Docker socket here.
        // The CLI container has Docker CLI installed but no local Docker access.
        // This ensures Docker tests truly verify that `m87 <device> docker` correctly
        // proxies commands to the agent (which does have the Docker socket mounted).
        let image = GenericImage::new(CLIENT_IMAGE_NAME, CLIENT_IMAGE_TAG)
            .with_entrypoint("sh")
            .with_cmd(vec!["-c", "sleep infinity"])
            .with_env_var("RUST_LOG", "info,m87_client=debug")
            .with_env_var("M87_API_KEY", ADMIN_KEY)
            .with_network(NETWORK_NAME)
            .with_container_name(&container_name);

        let container = image.start().await?;
        Ok(container)
    }

    async fn wait_for_server(
        server: &ContainerAsync<GenericImage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Waiting for server to be ready...");

        let port = server.get_host_port_ipv4(8084).await?;
        tracing::info!("Server mapped to host port {}", port);

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .http1_only()
            .build()?;

        for attempt in 1..=30 {
            let url = format!("https://localhost:{}/status", port);
            match client
                .get(&url)
                .timeout(Duration::from_secs(2))
                .send()
                .await
            {
                Ok(resp) => {
                    tracing::info!("Server responded with status: {}", resp.status());
                    if resp.status().is_success() {
                        tracing::info!("Server is ready");
                        return Ok(());
                    }
                }
                Err(e) => {
                    if attempt % 5 == 0 {
                        tracing::info!("Still waiting for server... (attempt {}, error: {:?})", attempt, e);
                    }
                }
            }
            sleep(Duration::from_secs(2)).await;
        }

        Err("Server did not become ready within timeout".into())
    }

    async fn configure_runtime(
        runtime: &ContainerAsync<GenericImage>,
        run_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let server_name = format!("e2e-server-{}", run_id);
        let config = format!(
            r#"{{
  "api_url": "https://{}:8084",
  "make87_api_url": "https://{}:8084",
  "make87_app_url": "https://{}:8084",
  "log_level": "debug",
  "owner_reference": "e2e@test.local",
  "auth_domain": "https://auth.make87.com/",
  "auth_audience": "https://auth.make87.com",
  "auth_client_id": "test",
  "trust_invalid_server_cert": true
}}"#,
            server_name, server_name, server_name
        );

        runtime
            .exec(ExecCommand::new(vec![
                "sh",
                "-c",
                "mkdir -p /root/.config/m87",
            ]))
            .await?;

        runtime
            .exec(ExecCommand::new(vec![
                "sh",
                "-c",
                &format!(
                    "cat > /root/.config/m87/config.json << 'EOF'\n{}\nEOF",
                    config
                ),
            ]))
            .await?;

        Ok(())
    }

    async fn configure_cli(
        cli: &ContainerAsync<GenericImage>,
        run_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let server_name = format!("e2e-server-{}", run_id);
        let server_url = format!("https://{}:8084", server_name);
        let config = format!(
            r#"{{
  "api_url": "{}",
  "make87_api_url": "{}",
  "make87_app_url": "{}",
  "log_level": "debug",
  "owner_reference": null,
  "auth_domain": "https://auth.make87.com/",
  "auth_audience": "https://auth.make87.com",
  "auth_client_id": "test",
  "trust_invalid_server_cert": true,
  "manager_server_urls": ["{}"]
}}"#,
            server_url, server_url, server_url, server_url
        );

        let credentials = format!(
            r#"{{"credentials":{{"APIKey":{{"api_key":"{}"}}}}}}"#,
            ADMIN_KEY
        );

        cli.exec(ExecCommand::new(vec![
            "sh",
            "-c",
            "mkdir -p /root/.config/m87",
        ]))
        .await?;

        cli.exec(ExecCommand::new(vec![
            "sh",
            "-c",
            &format!(
                "cat > /root/.config/m87/config.json << 'EOF'\n{}\nEOF",
                config
            ),
        ]))
        .await?;

        cli.exec(ExecCommand::new(vec![
            "sh",
            "-c",
            &format!(
                "cat > /root/.config/m87/credentials.json << 'EOF'\n{}\nEOF",
                credentials
            ),
        ]))
        .await?;

        Ok(())
    }

    /// Execute a CLI command and return the output
    pub async fn cli_exec(&self, args: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
        let cmd = format!("m87 {} --verbose", args.join(" "));
        let mut result = self
            .cli
            .exec(ExecCommand::new(vec!["sh", "-c", &cmd]))
            .await?;

        let stdout = result.stdout_to_vec().await?;
        Ok(String::from_utf8_lossy(&stdout).to_string())
    }

    /// Execute a runtime command and return the output
    #[allow(dead_code)]
    pub async fn runtime_exec(&self, args: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
        let cmd = format!("m87 {}", args.join(" "));
        let mut result = self
            .runtime
            .exec(ExecCommand::new(vec!["sh", "-c", &cmd]))
            .await?;

        let stdout = result.stdout_to_vec().await?;
        Ok(String::from_utf8_lossy(&stdout).to_string())
    }

    /// Start runtime login in background (doesn't block)
    pub async fn start_runtime_login(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Run runtime login in background using nohup
        self.runtime
            .exec(ExecCommand::new(vec![
                "sh",
                "-c",
                "nohup m87 runtime login --org-id e2e@test.local > /tmp/runtime-login.log 2>&1 &",
            ]))
            .await?;

        Ok(())
    }

    /// Get runtime login log output
    pub async fn get_runtime_login_log(&self) -> Result<String, Box<dyn std::error::Error>> {
        let mut result = self
            .runtime
            .exec(ExecCommand::new(vec![
                "sh",
                "-c",
                "cat /tmp/runtime-login.log 2>/dev/null || echo ''",
            ]))
            .await?;

        let stdout = result.stdout_to_vec().await?;
        Ok(String::from_utf8_lossy(&stdout).to_string())
    }

    /// Get server stdout logs
    pub async fn get_server_logs(&self) -> Result<String, Box<dyn std::error::Error>> {
        let stdout = self.server.stdout_to_vec().await?;
        let stderr = self.server.stderr_to_vec().await?;
        let combined = format!(
            "=== SERVER STDOUT ===\n{}\n=== SERVER STDERR ===\n{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
        Ok(combined)
    }
}

/// Get the Docker socket path for the current platform
/// Returns None if no socket is found
fn get_docker_socket_path() -> Option<String> {
    use std::path::Path;

    // Check in order of preference:
    // 1. DOCKER_HOST environment variable (explicit override)
    if let Ok(docker_host) = std::env::var("DOCKER_HOST") {
        if docker_host.starts_with("unix://") {
            let path = docker_host.strip_prefix("unix://").unwrap();
            if Path::new(path).exists() {
                return Some(path.to_string());
            }
        }
    }

    // 2. Standard Linux/WSL2 path
    if Path::new("/var/run/docker.sock").exists() {
        return Some("/var/run/docker.sock".to_string());
    }

    // 3. macOS Docker Desktop user-specific path (since Docker Desktop 4.13)
    if let Ok(home) = std::env::var("HOME") {
        let macos_path = format!("{}/.docker/run/docker.sock", home);
        if Path::new(&macos_path).exists() {
            return Some(macos_path);
        }
    }

    // 4. Colima (popular Docker Desktop alternative on macOS)
    if let Ok(home) = std::env::var("HOME") {
        let colima_path = format!("{}/.colima/default/docker.sock", home);
        if Path::new(&colima_path).exists() {
            return Some(colima_path);
        }
    }

    None
}
