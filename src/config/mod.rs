use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::path::PathBuf;
use tracing::{info, warn};

use crate::util::mac;

fn default_heartbeat_interval() -> u64 {
    300 // 5 min
}
fn default_update_check_interval() -> u64 {
    3600 // 1h
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub api_url: String,
    pub node_id: String,
    pub log_level: String,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_update_check_interval")]
    pub update_check_interval_secs: u64,
    pub owner_reference: Option<String>,
    pub auth_domain: String,
    pub auth_audience: String,
    pub auth_client_id: String,
    pub server_port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_url: "https://api.make87.com".to_string(),
            node_id: Config::deterministic_node_id(),
            log_level: "info".to_string(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            update_check_interval_secs: default_update_check_interval(),
            owner_reference: None,
            auth_domain: "https://auth.make87.com/".to_string(),
            auth_audience: "https://auth.make87.com".to_string(),
            auth_client_id: "E2J7xfFLgexzvhHhz4YqaJBy8Ys82SmM".to_string(),
            server_port: 8337,
        }
    }
}

impl Config {
    /// Create a deterministic BSON-style ObjectId string from hostname and MAC address.
    pub fn deterministic_node_id() -> String {
        let hostname = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
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

        info!("Config saved to: {:?}", config_path);
        Ok(())
    }

    fn config_file_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir().context("Failed to get config directory")?;
        Ok(config_dir.join("m87").join("config.json"))
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
        assert_eq!(config.api_url, "https://api.make87.com");
        assert_eq!(config.log_level, "info");
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(config.api_url, deserialized.api_url);
    }
}
