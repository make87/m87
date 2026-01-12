use std::io::{self, Write};

use anyhow::{Result, anyhow};
use m87_shared::device::{DeviceStatus, PublicDevice};
use tracing::warn;

use crate::util::device_cache;
use crate::util::servers_parallel::fanout_servers;
use crate::{auth::AuthManager, config::Config, server};

pub async fn list_devices() -> Result<Vec<PublicDevice>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;

    let results = fanout_servers(config.manager_server_urls, 4, |server_url| {
        let token = token.clone();
        async move { server::list_devices(&server_url, &token, trust).await }
    })
    .await?;

    let mut out = Vec::new();

    for (server_url, device) in results {
        device_cache::update_cache(&device, &server_url)?;
        out.push(device);
    }

    Ok(out)
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

pub async fn resolve_device_short_id_cached(name: &str) -> Result<ResolvedDevice> {
    // 1) try cache first
    let cached = device_cache::try_cache(name)?;

    if let Some(res) = select_from_cache(name, cached)? {
        return Ok(res);
    }

    // 2) warm cache (parallel fan-out)
    list_devices().await?;

    // 3) re-read cache
    let cached = device_cache::try_cache(name)?;

    select_from_cache(name, cached)?.ok_or_else(|| anyhow!("Device '{}' not found", name))
}

fn prompt_cached_selection(name: &str, devices: Vec<device_cache::CachedDevice>) -> Result<String> {
    println!("Multiple cached devices named '{}':", name);

    for (i, d) in devices.iter().enumerate() {
        println!(
            "  [{}] {} (id={} server={})",
            i + 1,
            d.name,
            d.short_id,
            d.server_url
        );
    }

    print!("Select device: ");
    let idx = read_user_index()?;

    let selected = devices
        .get(idx.saturating_sub(1))
        .ok_or_else(|| anyhow!("Invalid selection"))?;

    Ok(selected.short_id.clone())
}

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

#[derive(Debug, Clone)]
pub struct ResolvedDevice {
    pub short_id: String,
    pub host: String,
    pub url: String,
    pub id: String,
}

pub fn select_from_cache(
    name: &str,
    cached: Vec<device_cache::CachedDevice>,
) -> Result<Option<ResolvedDevice>> {
    match cached.len() {
        0 => Ok(None),
        1 => Ok(Some(to_resolved(&cached[0]))),
        _ => {
            let short_id = prompt_cached_selection(name, cached.clone())?;
            let selected = cached
                .into_iter()
                .find(|d| d.short_id == short_id)
                .ok_or_else(|| anyhow!("Invalid selection"))?;

            Ok(Some(to_resolved(&selected)))
        }
    }
}

pub fn to_resolved(d: &device_cache::CachedDevice) -> ResolvedDevice {
    ResolvedDevice {
        short_id: d.short_id.clone(),
        url: d.server_url.clone(),
        host: d
            .server_url
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .to_string(),
        id: d.id.clone(),
    }
}

pub async fn get_device_status(name: &str, since: Option<u32>) -> Result<DeviceStatus> {
    let resolved = resolve_device_short_id_cached(name).await?;

    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    let trust = config.trust_invalid_server_cert;
    let status =
        server::get_device_status(&resolved.url, &token, &resolved.id, trust, since).await?;

    Ok(status)
}
