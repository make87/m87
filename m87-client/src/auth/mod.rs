use anyhow::{anyhow, Context, Result};
#[cfg(feature = "agent")]
use m87_shared::device::DeviceSystemInfo;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
#[cfg(feature = "agent")]
use std::time::Duration;
use tracing::info;

#[cfg(feature = "agent")]
mod device;
mod oauth;

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::server;

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

    // Agent-specific: Device registration
    #[cfg(feature = "agent")]
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

// Manager-specific: OAuth2 login for device management
pub async fn login_cli() -> Result<()> {
    if AuthManager::has_cli_credentials()? {
        info!("Already logged in");
        return Ok(());
    }
    let _ = AuthManager::login_cli().await;
    Ok(())
}

// Agent-specific: Device registration for agents
#[cfg(feature = "agent")]
pub async fn register_device(
    owner_scope: Option<String>,
    device_system_info: DeviceSystemInfo,
) -> Result<()> {
    if AuthManager::has_device_credentials()? {
        info!("Already registered");
        return Ok(());
    }

    let mut config = Config::load()?;
    let owner_scope = match owner_scope {
        Some(rid) => rid,
        None => match Config::has_owner_reference()? {
            true => Config::get_owner_reference()?,
            false => match std::env::var(OWNER_REFERENCE_ENV_VAR) {
                Ok(value) => value,
                Err(_) => {
                    if config.api_url.is_none() {
                        info!("Missing server url and owner reference. Starting registration process...");
                        let (url, owner) = server::get_server_url_and_owner_reference(
                            &config.make87_api_url,
                            &config.make87_app_url,
                        )
                        .await?;
                        config.api_url = Some(url);
                        config.owner_reference = Some(owner.clone());
                        config.save()?;
                        owner
                    } else {
                        return Err(anyhow::anyhow!("No owner reference provided"));
                    }
                }
            },
        },
    };
    //if @ is in owner_scope prefix with user: otherwise with org:
    let owner_scope = if owner_scope.contains('@') {
        format!("user:{}", owner_scope)
    } else {
        format!("org:{}", owner_scope)
    };
    let mut report_handler = device::DeviceAuthRequestHandler {
        api_url: config.get_server_url(),
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

// Manager-specific: Logout from CLI
pub async fn logout_cli() -> Result<()> {
    AuthManager::delete_cli_credentials().await
}

// Agent-specific: Logout device credentials
#[cfg(feature = "agent")]
pub async fn logout_device() -> Result<()> {
    AuthManager::delete_device_credentials().await
}

// Manager-specific: List pending device auth requests
pub async fn list_auth_requests() -> Result<Vec<server::DeviceAuthRequest>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    server::list_auth_requests(
        &config.get_server_url(),
        &token,
        config.trust_invalid_server_cert,
    )
    .await
}

// Manager-specific: Approve device registration
pub async fn accept_auth_request(request_id: &str) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;

    server::handle_auth_request(
        &config.get_server_url(),
        &token,
        request_id,
        true,
        config.trust_invalid_server_cert,
    )
    .await
}

// Manager-specific: Reject device registration
pub async fn reject_auth_request(request_id: &str) -> Result<()> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;

    server::handle_auth_request(
        &config.get_server_url(),
        &token,
        request_id,
        false,
        config.trust_invalid_server_cert,
    )
    .await
}
