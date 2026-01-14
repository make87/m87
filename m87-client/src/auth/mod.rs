use anyhow::{Context, Result, anyhow};
#[cfg(feature = "runtime")]
use m87_shared::device::DeviceSystemInfo;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
#[cfg(feature = "runtime")]
use std::time::Duration;
use tracing::info;

#[cfg(feature = "runtime")]
mod device;
mod oauth;

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::server;
use crate::util::servers_parallel::{fanout_servers, find_on_servers};

pub const OWNER_REFERENCE_ENV_VAR: &str = "OWNER_REFERENCE";
pub const API_KEY_ENV_VAR: &str = "M87_API_KEY";

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct APIConfig {
    pub credentials: Option<Credentials>,
    pub device_credentials: Option<APIKey>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct APIKey {
    api_key: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Credentials {
    APIKey(APIKey),
    OAuth2Token(oauth::OAuth2Token),
}

impl Credentials {
    pub async fn get_token(&mut self) -> Result<String> {
        match self {
            Credentials::APIKey(credentials) => Ok(credentials.api_key.clone()),
            Credentials::OAuth2Token(credentials) => {
                let config = Config::load()?;
                credentials
                    .get_access_token(&config.auth_domain, &config.auth_client_id)
                    .await
            }
        }
    }
}

impl APIConfig {
    pub fn load_or_create() -> Result<Self> {
        let file_path = Self::default_credentials_path()?;
        if file_path.exists() {
            Self::load()
        } else {
            Self::create()
        }
    }

    fn create() -> Result<Self> {
        let api_config = APIConfig {
            ..Default::default()
        };
        api_config.save()?;
        Ok(api_config)
    }

    pub fn exists() -> Result<bool> {
        let file_path = Self::default_credentials_path()?;
        Ok(file_path.exists())
    }

    pub fn load() -> Result<Self> {
        let file_path = Self::default_credentials_path()?;
        let file = File::open(&file_path)?;
        let reader = BufReader::new(file);
        let api_config: APIConfig = serde_json::from_reader(reader)?;

        Ok(api_config)
    }

    pub fn save(&self) -> Result<()> {
        let file_path = Self::default_credentials_path()?;
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)?;

        #[cfg(unix)]
        {
            use std::fs::Permissions;
            file.set_permissions(Permissions::from_mode(0o600))?;
        }

        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }

    pub fn default_credentials_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir().context("Failed to get config directory")?;
        let path = config_dir.join("m87").join("credentials.json");
        Ok(path)
    }

    pub fn save_cli_credentials(credentials: Credentials) -> Result<()> {
        let mut config = Self::load_or_create()?;
        config.credentials = Some(credentials);
        config.save()?;
        Ok(())
    }

    pub fn save_device_credentials(key: String) -> Result<()> {
        let mut config = Self::load_or_create()?;
        config.device_credentials = Some(APIKey { api_key: key });
        config.save()?;
        Ok(())
    }

    pub fn delete_cli_credentials() -> Result<()> {
        let mut config = Self::load_or_create()?;
        config.credentials = None;
        config.save()?;
        Ok(())
    }

    pub fn delete_device_credentials() -> Result<()> {
        let mut config = Self::load_or_create()?;
        config.device_credentials = None;
        config.save()?;
        Ok(())
    }
}

pub struct AuthManager {}

impl AuthManager {
    pub async fn from_interactive_cli() -> Result<()> {
        let mut report_handler = oauth::PrintUserAuthRequestHandler {};
        AuthManager::from_device_flow(&mut report_handler).await
    }

    pub async fn from_device_flow(
        report_handler: &mut dyn oauth::SendUserAuthRequestHandler,
    ) -> Result<()> {
        let config = Config::load()?;
        let res = oauth::OAuth2Token::device_flow_login(
            &config.auth_domain,
            &config.auth_client_id,
            &config.auth_audience,
            report_handler,
        )
        .await;
        match res {
            Ok(token) => {
                let credentials = Credentials::OAuth2Token(token);
                APIConfig::save_cli_credentials(credentials)?;
                Ok(())
            }
            Err(e) => {
                eprintln!("Failed to auth: {}", e);
                Err(e.into())
            }
        }
    }

    pub async fn login_cli() -> Result<()> {
        if AuthManager::has_cli_credentials()? {
            return Ok(());
        }
        let _ = AuthManager::from_interactive_cli().await?;
        info!("Logged in successfully");
        Ok(())
    }

    // Runtime-specific: Device registration
    #[cfg(feature = "runtime")]
    pub async fn login_device(
        auth_handler: &mut device::DeviceAuthRequestHandler,
        timeout: Duration,
    ) -> Result<()> {
        match std::env::var(API_KEY_ENV_VAR) {
            Ok(api_key) => {
                if api_key.len() != 0 {
                    APIConfig::save_device_credentials(api_key)?;
                }
            }
            _ => {}
        };

        if AuthManager::has_device_credentials()? {
            return Ok(());
        }

        let api_key = auth_handler.handle_headless_auth(timeout).await?;
        APIConfig::save_device_credentials(api_key)?;
        info!("Logged device in successfully");
        Ok(())
    }

    pub async fn delete_cli_credentials() -> Result<()> {
        APIConfig::delete_cli_credentials()?;
        Ok(())
    }

    pub async fn delete_device_credentials() -> Result<()> {
        APIConfig::delete_device_credentials()?;
        Ok(())
    }

    pub async fn get_cli_token() -> Result<String> {
        APIConfig::load_or_create()?
            .credentials
            .ok_or_else(|| anyhow!("cli credentials not found"))?
            .get_token()
            .await
    }

    pub fn get_device_token() -> Result<String> {
        Ok(APIConfig::load_or_create()?
            .device_credentials
            .ok_or_else(|| anyhow!("device credentials not found"))?
            .api_key)
    }

    pub fn has_cli_credentials() -> Result<bool> {
        Ok(APIConfig::load_or_create()?.credentials.is_some())
    }

    pub fn has_device_credentials() -> Result<bool> {
        Ok(APIConfig::load_or_create()?.device_credentials.is_some())
    }
}

// m87 command line: OAuth2 login for device management
pub async fn login_cli() -> Result<()> {
    if AuthManager::has_cli_credentials()? {
        info!("Already logged in");

        let _ = update_server_urls().await?;
        return Ok(());
    }
    let _ = AuthManager::login_cli().await;

    let _ = update_server_urls().await?;
    Ok(())
}

async fn update_server_urls() -> Result<()> {
    let mut config = Config::load()?;
    if config.manager_server_urls.is_empty() {
        let token = AuthManager::get_cli_token().await?;
        let urls = server::get_manager_server_urls(&config.make87_api_url, &token).await?;
        config.manager_server_urls = urls;
        config.save()?;
    }
    Ok(())
}

// Runtime-specific: Device registration for runtimes
#[cfg(feature = "runtime")]
pub async fn register_device(
    owner_scope: Option<String>,
    device_system_info: DeviceSystemInfo,
) -> Result<()> {
    if AuthManager::has_device_credentials()? {
        info!("Already registered");
        return Ok(());
    }

    let mut config = Config::load()?;

    // resolve CLI owner, config owner, or env owner
    let mut owner_scope = owner_scope
        .or_else(|| {
            Config::has_owner_reference()
                .ok()
                .and_then(|b| b.then(Config::get_owner_reference).transpose().ok())
                .flatten()
        })
        .or_else(|| std::env::var(OWNER_REFERENCE_ENV_VAR).ok());

    let mut api_url = config.runtime_server_url.clone();

    // ------------------------------------------------------------
    // If either value is missing → call registration
    // ------------------------------------------------------------
    if api_url.is_none() || owner_scope.is_none() {
        let (resolved_api, resolved_owner) = server::get_server_url_and_owner_reference(
            &config.make87_api_url,
            &config.make87_app_url,
            owner_scope.clone(),
            api_url.clone(),
        )
        .await?;

        api_url = Some(resolved_api.clone());
        owner_scope = Some(resolved_owner.clone());

        if config.runtime_server_url.is_none() {
            config.runtime_server_url = Some(resolved_api);
        }
        if config.owner_reference.is_none() {
            config.owner_reference = Some(resolved_owner);
        }
        config.save()?;
    }

    // ------------------------------------------------------------
    // Final validation
    // ------------------------------------------------------------
    let api_url = api_url.expect("API URL must be set after registration");
    let owner_scope = owner_scope
        .ok_or_else(|| anyhow::anyhow!("No owner reference provided after registration"))?;

    //if @ is in owner_scope prefix with user: otherwise with org:
    let owner_scope = if owner_scope.contains('@') {
        format!("user:{}", owner_scope)
    } else {
        format!("org:{}", owner_scope)
    };
    let mut report_handler = device::DeviceAuthRequestHandler {
        api_url,
        device_info: device_system_info,
        device_id: config.device_id,
        owner_scope,
        request_id: None,
        trust_invalid_server_cert: config.trust_invalid_server_cert,
    };
    // endless retry if it fails with Err(anyhow::anyhow!("API key not approved within timeout"))
    while let Err(err) =
        AuthManager::login_device(&mut report_handler, Duration::from_secs(3600)).await
    {
        if err
            .to_string()
            .contains("API key not approved within timeout")
        {
            continue;
        }
        return Err(err);
    }
    Ok(())
}

pub async fn status() -> Result<()> {
    let _ = AuthManager::get_cli_token().await?;
    info!("Logged in");
    Ok(())
}

// m87 command line: Logout
pub async fn logout_cli() -> Result<()> {
    AuthManager::delete_cli_credentials().await
}

// Runtime-specific: Logout device credentials
#[cfg(feature = "runtime")]
pub async fn logout_device() -> Result<()> {
    AuthManager::delete_device_credentials().await
}

pub async fn list_auth_requests() -> Result<Vec<server::DeviceAuthRequest>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let requests = fanout_servers(config.manager_server_urls, 4, |server_url| {
        let token = token.clone();
        async move { server::list_auth_requests(&server_url, &token, trust).await }
    })
    .await?
    .iter()
    .map(|f| f.1.clone())
    .collect();
    Ok(requests)
}

pub async fn accept_auth_request(request_id: &str) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;

    let (server_url, _) = resolve_request_server(request_id).await?;

    server::handle_auth_request(
        &server_url,
        &token,
        request_id,
        true,
        config.trust_invalid_server_cert,
    )
    .await
}

pub async fn reject_auth_request(request_id: &str) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;

    let (server_url, _) = resolve_request_server(request_id).await?;

    server::handle_auth_request(
        &server_url,
        &token,
        request_id,
        false,
        config.trust_invalid_server_cert,
    )
    .await
}

pub struct AuthRequestWithServer {
    pub server_url: String,
    pub request: server::DeviceAuthRequest,
}

async fn resolve_request_server(request_id: &str) -> Result<(String, server::DeviceAuthRequest)> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let found = find_on_servers(config.manager_server_urls, 4, |server_url| {
        let token = token.clone();
        let request_id = request_id.to_string();
        async move {
            let requests = server::list_auth_requests(&server_url, &token, trust).await?;

            Ok(requests.into_iter().find(|r| r.request_id == request_id))
        }
    })
    .await?;

    found.ok_or_else(|| {
        anyhow::anyhow!(
            "Auth request '{}' not found on any server",
            request_id
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_config_default_credentials_path() {
        let path = APIConfig::default_credentials_path().unwrap();
        let path_str = path.to_string_lossy();
        assert!(path_str.ends_with("m87/credentials.json"));
    }

    #[test]
    fn test_api_key_serialization() {
        let api_key = APIKey {
            api_key: "test_api_key_12345".to_string(),
        };

        let json = serde_json::to_string(&api_key).unwrap();
        let deserialized: APIKey = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.api_key, "test_api_key_12345");
    }

    #[test]
    fn test_credentials_api_key_variant() {
        let credentials = Credentials::APIKey(APIKey {
            api_key: "my_secret_key".to_string(),
        });

        let json = serde_json::to_string(&credentials).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();

        match deserialized {
            Credentials::APIKey(key) => {
                assert_eq!(key.api_key, "my_secret_key");
            }
            _ => panic!("Expected APIKey variant"),
        }
    }

    #[test]
    fn test_api_config_default() {
        let config = APIConfig::default();
        assert!(config.credentials.is_none());
        assert!(config.device_credentials.is_none());
    }
}
