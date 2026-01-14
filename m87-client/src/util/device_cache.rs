use anyhow::{Result, anyhow};
use dirs::cache_dir;
use m87_shared::device::PublicDevice;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

const CACHE_TTL_SECS: u64 = 60;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CachedDevice {
    pub id: String,
    pub short_id: String,
    pub name: String,
    pub updated_at: u64,
    pub server_url: String,
}

type DeviceCache = HashMap<String, Vec<CachedDevice>>;

fn cache_path() -> Result<PathBuf> {
    let mut base = cache_dir().ok_or_else(|| anyhow!("Could not determine cache directory"))?;
    base.push("m87");
    base.push("device_index.json");
    Ok(base)
}

pub fn load_cache() -> Result<DeviceCache> {
    let path = cache_path()?;
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let data = fs::read(&path)?;
    Ok(serde_json::from_slice(&data)?)
}

pub fn try_cache(name: &str) -> Result<Vec<CachedDevice>> {
    let cache = load_cache()?;
    let entries = match cache.get(name) {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let valid = entries
        .iter()
        .filter(|e| now.saturating_sub(e.updated_at) <= CACHE_TTL_SECS)
        .cloned()
        .collect();

    Ok(valid)
}

pub fn update_cache(device: &PublicDevice, server_url: &str) -> Result<()> {
    let mut cache = load_cache()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let entry = CachedDevice {
        id: device.id.clone(),
        short_id: device.short_id.clone(),
        name: device.name.clone(),
        updated_at: now,
        server_url: server_url.to_string(),
    };

    let list = cache.entry(device.name.clone()).or_default();

    if let Some(existing) = list.iter_mut().find(|e| e.id == device.id) {
        *existing = entry;
    } else {
        list.push(entry);
    }

    let path = cache_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let data = serde_json::to_vec_pretty(&cache)?;
    write_atomic(&path, &data)?;

    Ok(())
}

pub fn update_cache_bulk(devices: &[PublicDevice], server_url: &str) -> Result<()> {
    let mut cache = load_cache()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    for d in devices {
        let entry = CachedDevice {
            id: d.id.clone(),
            short_id: d.short_id.clone(),
            name: d.name.clone(),
            updated_at: now,
            server_url: server_url.to_string(),
        };

        let list = cache.entry(d.name.clone()).or_default();

        if let Some(existing) = list.iter_mut().find(|e| e.id == d.id) {
            *existing = entry;
        } else {
            list.push(entry);
        }
    }

    let path = cache_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let data = serde_json::to_vec_pretty(&cache)?;
    write_atomic(&path, &data)?;
    Ok(())
}

fn write_atomic(path: &PathBuf, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, data)?;
    fs::rename(tmp, path)?;
    Ok(())
}

pub fn try_get_name_from_long_id(id: &str) -> Result<String> {
    let cache = load_cache()?;
    let name = cache
        .values()
        .find_map(|list| list.iter().find(|d| d.id == id).map(|d| d.name.clone()));

    name.ok_or_else(|| anyhow!("Device with ID {} not found", id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_device_serialization() {
        let device = CachedDevice {
            id: "abc123".to_string(),
            short_id: "abc".to_string(),
            name: "my-device".to_string(),
            updated_at: 1700000000,
            server_url: "https://api.example.com".to_string(),
        };

        let json = serde_json::to_string(&device).unwrap();
        let deserialized: CachedDevice = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, "abc123");
        assert_eq!(deserialized.name, "my-device");
        assert_eq!(deserialized.updated_at, 1700000000);
    }

    #[test]
    fn test_cache_path_structure() {
        let path = cache_path().unwrap();
        let path_str = path.to_string_lossy();
        assert!(path_str.ends_with("m87/device_index.json"));
    }

    #[test]
    fn test_write_atomic_creates_file() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join(format!("m87_test_atomic_{}.json", std::process::id()));

        let data = b"test content";
        write_atomic(&test_file, data).unwrap();

        assert!(test_file.exists());
        let content = fs::read(&test_file).unwrap();
        assert_eq!(content, data);

        // Cleanup
        let _ = fs::remove_file(&test_file);
    }
}
