use anyhow::{Context, Result, anyhow};
use m87_shared::config::ObservationConfig;
use serde::{Deserialize, Serialize};
#[cfg(feature = "runtime")]
use sha1::{Digest, Sha1};
use std::{fs, path::PathBuf};
use tracing::{error, info, warn};

#[cfg(feature = "runtime")]
use crate::util::mac;

fn default_heartbeat_interval() -> u64 {
    300 // 5 min
}
fn default_make87_api_url() -> String {
    "https://api.make87.com".to_string()
}

fn default_make87_app_url() -> String {
    "https://app.make87.com".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    #[serde(default, alias = "agent_server_url", alias = "api_url")]
    pub runtime_server_url: Option<String>,
    #[serde(default = "default_make87_api_url")]
    pub make87_api_url: String,
    #[serde(default = "default_make87_app_url")]
    pub make87_app_url: String,
    #[serde(default = "get_default_device_id")]
    pub device_id: String,
    pub log_level: String,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,
    pub owner_reference: Option<String>,
    pub auth_domain: String,
    pub auth_audience: String,
    pub auth_client_id: String,
    #[serde(default)]
    pub trust_invalid_server_cert: bool,

    #[serde(default)]
    pub manager_server_urls: Vec<String>,
    #[serde(default)]
    pub observe: ObservationConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            runtime_server_url: None,
            make87_api_url: "https://api.make87.com".to_string(),
            make87_app_url: "https://app.make87.com".to_string(),
            device_id: get_default_device_id(),
            log_level: "info".to_string(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            owner_reference: None,
            auth_domain: "https://auth.make87.com/".to_string(),
            auth_audience: "https://auth.make87.com".to_string(),
            auth_client_id: "E2J7xfFLgexzvhHhz4YqaJBy8Ys82SmM".to_string(),
            trust_invalid_server_cert: false,
            manager_server_urls: vec![],
            observe: ObservationConfig::default(),
        }
    }
}

fn get_default_device_id() -> String {
    #[cfg(feature = "runtime")]
    {
        return Config::deterministic_device_id();
    }
    #[cfg(not(feature = "runtime"))]
    {
        "".to_string()
    }
}

impl Config {
    // Removes all config from the system
    pub fn clear() -> Result<()> {
        let path = Self::config_file_path()?;
        if path.exists() {
            fs::remove_file(&path).context("Failed to delete config file")?;
            tracing::info!("Deleted config file at {:?}", path);
        } else {
            tracing::warn!("No config file found at {:?}", path);
        }
        Ok(())
    }

    pub fn get_runtime_server_url(&self) -> String {
        match &self.runtime_server_url {
            Some(url) => url.clone(),
            None => {
                error!("API URL not set. Make sure to login in order to set it!");
                panic!("API URL not set");
            }
        }
    }

    pub fn get_runtime_server_hostname(&self) -> String {
        let url = self.get_runtime_server_url();
        url.trim_start_matches("https://")
            .trim_start_matches("http://")
            .to_string()
    }

    /// Create a deterministic BSON-style ObjectId string from hostname and MAC address.
    /// Runtime-specific: Used for device registration
    #[cfg(feature = "runtime")]
    pub fn deterministic_device_id() -> String {
        use sysinfo::System;

        let hostname = System::host_name().unwrap_or_else(|| "not found".to_string());
        let mac = mac::get_mac_address().unwrap_or_else(|| "00:00:00:00:00:00".into());

        // Hash hostname + mac
        let mut hasher = Sha1::new();
        hasher.update(hostname.as_bytes());
        hasher.update(mac.as_bytes());
        let hash = hasher.finalize();

        // Take first 12 bytes and convert to hex
        hash[..12].iter().map(|b| format!("{:02x}", b)).collect()
    }

    pub fn load() -> Result<Self> {
        let config_path = Self::config_file_path()?;

        if config_path.exists() {
            let contents =
                std::fs::read_to_string(&config_path).context("Failed to read config file")?;
            let config: Config =
                serde_json::from_str(&contents).context("Failed to parse config file")?;
            Ok(config)
        } else {
            warn!("Config file not found, using defaults");
            let config = Self::default();
            config.save()?;
            Ok(config)
        }
    }

    pub fn save(&self) -> Result<()> {
        let config_path = Self::config_file_path()?;
        let config_dir = config_path
            .parent()
            .context("Failed to get config directory")?;

        std::fs::create_dir_all(config_dir).context("Failed to create config directory")?;

        let contents = serde_json::to_string_pretty(self).context("Failed to serialize config")?;

        std::fs::write(&config_path, contents).context("Failed to write config file")?;

        // When running with sudo, fix ownership to the original user
        #[cfg(unix)]
        if let Ok(sudo_user) = std::env::var("SUDO_USER") {
            use std::process::Command;
            // chown the config directory and file to the original user
            let _ = Command::new("chown")
                .args(["-R", &sudo_user, config_dir.to_str().unwrap_or("")])
                .status();
        }

        info!("Config saved to: {:?}", config_path);
        Ok(())
    }

    pub fn config_file_path() -> Result<PathBuf> {
        let config_dir = Self::get_config_dir()?;
        Ok(config_dir.join("m87").join("config.json"))
    }

    /// Get config directory, respecting SUDO_USER on Unix systems.
    /// Falls back to dirs::config_dir() which handles platform specifics.
    fn get_config_dir() -> Result<PathBuf> {
        // Check for SUDO_USER (Unix only - Windows doesn't have sudo)
        #[cfg(unix)]
        if let Ok(sudo_user) = std::env::var("SUDO_USER") {
            let home = homedir::home(&sudo_user)
                .ok()
                .flatten()
                .context("Failed to get sudo user's home directory")?;

            // On Linux, respect XDG_CONFIG_HOME if set to absolute path
            #[cfg(target_os = "linux")]
            if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
                let xdg_path = PathBuf::from(&xdg);
                if xdg_path.is_absolute() {
                    return Ok(xdg_path);
                }
            }

            // Platform-specific defaults (matching dirs crate behavior)
            #[cfg(target_os = "macos")]
            return Ok(home.join("Library/Application Support"));

            #[cfg(not(target_os = "macos"))]
            return Ok(home.join(".config"));
        }

        // No sudo or Windows - use standard dirs crate
        dirs::config_dir().context("Failed to get config directory")
    }

    pub fn add_owner_reference(owner_reference: String) -> Result<()> {
        let mut config = Self::load().context("Failed to load config")?;
        config.owner_reference = Some(owner_reference);
        config.save().context("Failed to save config")?;
        Ok(())
    }

    pub fn has_owner_reference() -> Result<bool> {
        let config = Self::load().context("Failed to load config")?;
        Ok(config.owner_reference.is_some())
    }

    pub fn get_owner_reference() -> Result<String> {
        let config = Self::load().context("Failed to load config")?;
        match config.owner_reference {
            Some(owner_reference) => Ok(owner_reference),
            None => Err(anyhow!(
                "No owner reference found. Pass a valid user email or organization id!"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.runtime_server_url, None);
        assert_eq!(config.log_level, "info");
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(config.runtime_server_url, deserialized.runtime_server_url);
    }

    #[test]
    fn test_get_runtime_server_hostname_strips_https() {
        let mut config = Config::default();
        config.runtime_server_url = Some("https://api.example.com".to_string());
        assert_eq!(config.get_runtime_server_hostname(), "api.example.com");
    }

    #[test]
    fn test_get_runtime_server_hostname_strips_http() {
        let mut config = Config::default();
        config.runtime_server_url = Some("http://api.example.com".to_string());
        assert_eq!(config.get_runtime_server_hostname(), "api.example.com");
    }

    #[test]
    fn test_get_runtime_server_hostname_with_port() {
        let mut config = Config::default();
        config.runtime_server_url = Some("https://api.example.com:8443".to_string());
        assert_eq!(config.get_runtime_server_hostname(), "api.example.com:8443");
    }

    #[test]
    fn test_get_runtime_server_hostname_no_protocol() {
        let mut config = Config::default();
        config.runtime_server_url = Some("api.example.com".to_string());
        assert_eq!(config.get_runtime_server_hostname(), "api.example.com");
    }

    #[test]
    fn test_default_heartbeat_interval() {
        assert_eq!(default_heartbeat_interval(), 300);
    }

    #[test]
    fn test_default_api_url() {
        assert_eq!(default_make87_api_url(), "https://api.make87.com");
    }
}
