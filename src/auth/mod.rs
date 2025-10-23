use anyhow::{anyhow, Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use tracing::info;
mod node;
mod oauth;
mod user;

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::util::macchina;

pub const OWNER_REFERENCE_ENV_VAR: &str = "OWNER_REFERENCE";
pub const API_KEY_ENV_VAR: &str = "M87_API_KEY";

#[derive(Serialize, Deserialize)]
pub struct APIConfig {
    pub credentials: Credentials,
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

#[derive(Serialize, Deserialize)]
pub struct AuthManager {
    pub api_config: APIConfig,
}

impl AuthManager {
    pub fn new(api_config: APIConfig) -> Self {
        AuthManager { api_config }
    }

    pub async fn from_interactive_cli() -> Result<Self> {
        let mut report_handler = oauth::PrintUserAuthRequestHandler {};
        AuthManager::from_device_flow(&mut report_handler).await
    }

    pub async fn from_device_flow(
        report_handler: &mut dyn oauth::SendUserAuthRequestHandler,
    ) -> Result<Self> {
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
                AuthManager::save_to_default_path(&credentials)?;
                let manager = AuthManager::from_default_path()?;
                Ok(manager)
            }
            Err(e) => {
                eprintln!("Failed to auth: {}", e);
                Err(e.into())
            }
        }
    }

    pub async fn get_or_login() -> Result<Self> {
        if AuthManager::has_stored_credentials()? {
            return AuthManager::from_default_path();
        }
        let manager = AuthManager::from_interactive_cli().await?;
        Ok(manager)
    }

    pub async fn get_or_login_headless(
        report_handler: &mut dyn oauth::SendUserAuthRequestHandler,
    ) -> Result<Self> {
        match std::env::var(API_KEY_ENV_VAR) {
            Ok(api_key) => {
                if api_key.len() != 0 {
                    let credentials = Credentials::APIKey(APIKey { api_key });
                    AuthManager::save_to_default_path(&credentials)?;
                }
            }
            _ => {}
        };

        if AuthManager::has_stored_credentials()? {
            return AuthManager::from_default_path();
        }
        let manager = AuthManager::from_device_flow(report_handler).await?;
        Ok(manager)
    }

    pub async fn delete_credentials() -> Result<()> {
        let file_path = AuthManager::default_credentials_path()?;
        if file_path.exists() {
            fs::remove_file(file_path)?;
        }
        Ok(())
    }

    pub async fn ensure_has_api_key(&mut self, api_url: &str, name: &str) -> Result<()> {
        if !AuthManager::has_stored_credentials()? {
            Err(anyhow!(
                "No API key found. Please run `m87 login` to authenticate"
            ))
        } else {
            let creds = &self.api_config.credentials;
            match creds {
                Credentials::APIKey(_) => Ok(()),
                Credentials::OAuth2Token(_t) => {
                    let token = self.get_token().await?;
                    let api_key = user::request_api_key(api_url, &token, name).await?;
                    let credentials = Credentials::APIKey(APIKey { api_key });
                    AuthManager::save_to_default_path(&credentials)?;
                    Ok(())
                }
            }
        }
    }

    pub async fn get_token(&mut self) -> Result<String> {
        self.api_config.credentials.get_token().await
    }

    pub fn from_default_path() -> Result<Self> {
        let file_path = AuthManager::default_credentials_path()?;
        let file = File::open(&file_path)?;
        let reader = BufReader::new(file);
        let api_config: APIConfig = serde_json::from_reader(reader)?;

        Ok(Self::new(api_config))
    }

    pub fn has_stored_credentials() -> Result<bool> {
        let file_path = AuthManager::default_credentials_path()?;
        Ok(file_path.exists())
    }

    pub fn save_to_default_path(credentials: &Credentials) -> Result<()> {
        let file_path = AuthManager::default_credentials_path()?;
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let api_config = APIConfig {
            credentials: credentials.clone(),
        };
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
        serde_json::to_writer_pretty(writer, &api_config)?;
        Ok(())
    }

    fn default_credentials_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir().context("Failed to get config directory")?;
        let path = config_dir.join("m87").join("credentials.json");
        Ok(path)
    }
}

fn get_host_name() -> Result<String> {
    let hostname_result = hostname::get()?;

    let name = hostname_result.to_string_lossy().into_owned();
    Ok(name)
}

pub async fn login() -> Result<()> {
    if AuthManager::has_stored_credentials()? {
        info!("Already logged in");
        return Ok(());
    }
    let _ = AuthManager::get_or_login().await;
    Ok(())
}

pub async fn register(owner_reference: Option<String>) -> Result<()> {
    if AuthManager::has_stored_credentials()? {
        info!("Already registered");
        return Ok(());
    }

    let config = Config::load()?;
    let owner_reference = match owner_reference {
        Some(rid) => rid,
        None => match Config::has_owner_reference()? {
            true => Config::get_owner_reference()?,
            false => std::env::var(OWNER_REFERENCE_ENV_VAR)
                .expect(format!("{} not set", OWNER_REFERENCE_ENV_VAR).as_str()),
        },
    };
    let node_info = macchina::get_detailed_printout();
    let host_name = get_host_name()?;
    let mut report_handler = node::NodeAuthRequestHandler {
        api_url: config.api_url.clone(),
        node_info: Some(node_info),
        hostname: host_name.clone(),
        node_id: config.node_id,
        owner_reference,
        request_id: None,
    };
    let mut manager = AuthManager::get_or_login_headless(&mut report_handler).await?;
    manager
        .ensure_has_api_key(&config.api_url, &host_name)
        .await?;
    Ok(())
}

pub async fn status() -> Result<()> {
    let config = Config::load()?;
    let mut manager = AuthManager::from_default_path()?;
    let token = manager.get_token().await?;
    let user = user::me(&config.api_url, &token).await?;
    info!("Logged in as {}", user.name);
    Ok(())
}

pub async fn logout() -> Result<()> {
    AuthManager::delete_credentials().await
}
