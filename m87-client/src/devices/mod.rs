use std::io::{self, Write};

use anyhow::{Result, anyhow};
use m87_shared::{auth::DeviceAuthRequest, device::PublicDevice};
use tracing::warn;

use crate::util::device_cache;
use crate::{auth::AuthManager, config::Config, server};

pub async fn list_devices() -> Result<Vec<PublicDevice>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    server::list_devices(
        &config.get_server_url(),
        &token,
        config.trust_invalid_server_cert,
    )
    .await
}

pub async fn list_auth_requests() -> Result<Vec<DeviceAuthRequest>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    server::list_auth_requests(
        &config.get_server_url(),
        &token,
        config.trust_invalid_server_cert,
    )
    .await
}

pub async fn get_device_by_name(name: &str) -> Result<PublicDevice> {
    list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == name)
        .map(|d| {
            if !d.online {
                warn!("Device '{}' is offline", d.name);
            }
            d
        })
        .ok_or_else(|| anyhow::anyhow!("Device '{}' not found", name))
}

pub async fn resolve_device_short_id_cached(name: &str) -> Result<String> {
    let cached = device_cache::try_cache(name)?;

    match cached.len() {
        1 => return Ok(cached[0].short_id.clone()),
        n if n > 1 => return prompt_cached_selection(name, cached),
        _ => {}
    }

    let devices = list_devices().await?;

    // Warm cache with full device list
    device_cache::update_cache_bulk(&devices)?;

    let matches: Vec<_> = devices.into_iter().filter(|d| d.name == name).collect();

    match matches.len() {
        0 => Err(anyhow!("Device '{}' not found", name)),
        1 => {
            let d = &matches[0];
            if !d.online {
                warn!("Device '{}' is offline", d.name);
            }
            Ok(d.short_id.clone())
        }
        _ => {
            let d = prompt_device_selection(name, matches)?;
            if !d.online {
                warn!("Device '{}' is offline", d.name);
            }
            Ok(d.short_id)
        }
    }
}

fn prompt_cached_selection(name: &str, devices: Vec<device_cache::CachedDevice>) -> Result<String> {
    println!("Multiple cached devices named '{}':", name);

    for (i, d) in devices.iter().enumerate() {
        println!("  [{}] {} (id={})", i + 1, d.name, d.short_id);
    }

    print!("Select device: ");
    let idx = read_user_index()?;

    let selected = devices
        .get(idx.saturating_sub(1))
        .ok_or_else(|| anyhow!("Invalid selection"))?;

    Ok(selected.short_id.clone())
}

fn prompt_device_selection(name: &str, devices: Vec<PublicDevice>) -> Result<PublicDevice> {
    println!("Multiple devices named '{}':", name);

    for (i, d) in devices.iter().enumerate() {
        println!(
            "  [{}] {} (id={}, online={})",
            i + 1,
            d.name,
            d.short_id,
            d.online
        );
    }

    print!("Select device: ");
    let idx = read_user_index()?;

    devices
        .get(idx.saturating_sub(1))
        .cloned()
        .ok_or_else(|| anyhow!("Invalid selection"))
}

// --------------------------------------------------
// User selection helper (used by caller)
// --------------------------------------------------
pub fn read_user_index() -> Result<usize> {
    let mut input = String::new();
    io::stdout().flush()?;
    io::stdin().read_line(&mut input)?;

    let idx = input
        .trim()
        .parse::<usize>()
        .map_err(|_| anyhow!("Invalid numeric input"))?;

    if idx == 0 {
        return Err(anyhow!("Selection must be >= 1"));
    }

    Ok(idx)
}
