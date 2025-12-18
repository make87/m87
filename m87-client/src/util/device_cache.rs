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
}

type DeviceCache = HashMap<String, Vec<CachedDevice>>;

// --------------------------------------------------
// Cache path (Linux / macOS / Windows)
// --------------------------------------------------
fn cache_path() -> Result<PathBuf> {
    let mut base = cache_dir().ok_or_else(|| anyhow!("Could not determine cache directory"))?;
    base.push("m87");
    base.push("device_index.json");
    Ok(base)
}

// --------------------------------------------------
// Load cache
// --------------------------------------------------
pub fn load_cache() -> Result<DeviceCache> {
    let path = cache_path()?;
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let data = fs::read(&path)?;
    Ok(serde_json::from_slice(&data)?)
}

// --------------------------------------------------
// Try cache (returns ALL valid entries for name)
// --------------------------------------------------
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

// --------------------------------------------------
// Update cache (append or refresh by id)
// --------------------------------------------------
pub fn update_cache(device: &PublicDevice) -> Result<()> {
    let mut cache = load_cache()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let entry = CachedDevice {
        id: device.id.clone(),
        short_id: device.short_id.clone(),
        name: device.name.clone(),
        updated_at: now,
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

pub fn update_cache_bulk(devices: &[PublicDevice]) -> Result<()> {
    let mut cache = load_cache()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    for d in devices {
        let entry = CachedDevice {
            id: d.id.clone(),
            short_id: d.short_id.clone(),
            name: d.name.clone(),
            updated_at: now,
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

// --------------------------------------------------
// Atomic write helper
// --------------------------------------------------
fn write_atomic(path: &PathBuf, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, data)?;
    fs::rename(tmp, path)?;
    Ok(())
}
