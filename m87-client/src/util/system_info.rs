use anyhow::Result;
use reqwest::Client;
use serde_json::Value;

use std::process::Command;

use crate::server;
use crate::util::macchina;

pub async fn get_system_info(enable_geo_lookup: bool) -> Result<server::DeviceSystemInfo> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()?;

    let mut sys_info = server::DeviceSystemInfo {
        ..Default::default()
    };
    if enable_geo_lookup {
        match client.get("http://ip-api.com/json").send().await {
            Ok(resp) => {
                // example: {"status":"success","country":"Germany","countryCode":"DE","region":"BW","regionName":"Baden-Wurttemberg","city":"Karlsruhe","zip":"76185","lat":49.0099,"lon":8.3592,"timezone":"Europe/Berlin","isp":"Deutsche Telekom AG","org":"Deutsche Telekom AG","as":"AS3320 Deutsche Telekom AG","query":"84.150.209.224"}
                let text = resp.text().await?;
                let parsed: Value = serde_json::from_str(&text)?;
                let ip = parsed
                    .get("query")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                if ip.is_some() {
                    sys_info.public_ip_address = ip;
                }
                let country_code = parsed
                    .get("countryCode")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                if country_code.is_some() {
                    sys_info.country_code = country_code;
                }
                let latitude = parsed.get("lat").and_then(|v| v.as_f64()).map(|f| f as f64);
                if latitude.is_some() {
                    sys_info.latitude = latitude;
                }
                let longitude = parsed.get("lon").and_then(|v| v.as_f64()).map(|f| f as f64);
                if longitude.is_some() {
                    sys_info.longitude = longitude;
                }
            }
            Err(_) => {}
        };
    }

    // Determine architecture
    let arch = Command::new("uname")
        .arg("-m")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| match s.trim() {
            "aarch64" => "arm64".to_string(),
            "x86_64" => "amd64".to_string(),
            s => s.to_string(),
        })
        .unwrap_or_else(|| "unknown".to_string());
    sys_info.architecture = arch;

    // get current user name
    let user = Command::new("whoami")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    sys_info.username = user;

    let readout = macchina::get_readout();
    sys_info.cores = Some(readout.cpu_cores);
    sys_info.cpu_name = readout.cpu;
    sys_info.memory = Some((readout.memory as f64) / 1024. / 1024.);
    sys_info.gpus = readout.gpus;
    sys_info.hostname = readout.name;
    sys_info.operating_system = readout.distribution;

    Ok(sys_info)
}
