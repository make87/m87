use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use tokio::{
    pin, signal,
    time::{sleep, Duration},
};
use tracing::{error, info};

use std::path::Path;
use std::process::Command;

use crate::{
    agent::{services::collect_all_services, system_metrics::collect_system_metrics},
    auth::register_agent,
    server,
};
use crate::{auth::AuthManager, config::Config, rest::serve_server, util::macchina};

use crate::server::send_heartbeat;
use crate::util::logging::init_tracing_with_log_layer;

const SERVICE_NAME: &str = "m87-agent";
const SERVICE_FILE: &str = "/etc/systemd/system/m87-agent.service";

pub async fn install_service() -> Result<()> {
    let exe_path = std::env::current_exe().context("Unable to resolve binary path")?;
    let service_content = format!(
        "[Unit]
Description=Agent Service for make87
After=network.target

[Service]
ExecStart={} agent run --headless
Restart=always
RestartSec=3
User=root
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
",
        exe_path.display()
    );

    std::fs::write(SERVICE_FILE, &service_content)
        .context("Failed to write systemd service file")?;

    Command::new("systemctl").args(["daemon-reload"]).status()?;
    Command::new("systemctl")
        .args(["enable", SERVICE_NAME])
        .status()?;
    Command::new("systemctl")
        .args(["start", SERVICE_NAME])
        .status()?;

    info!("Installed and started systemd service at {}", SERVICE_FILE);
    Ok(())
}

pub async fn uninstall_service() -> Result<()> {
    if Path::new(SERVICE_FILE).exists() {
        Command::new("systemctl")
            .args(["stop", SERVICE_NAME])
            .status()
            .ok();
        Command::new("systemctl")
            .args(["disable", SERVICE_NAME])
            .status()
            .ok();
        std::fs::remove_file(SERVICE_FILE).context("Failed to remove service file")?;
        Command::new("systemctl")
            .args(["daemon-reload"])
            .status()
            .ok();
        info!("Uninstalled m87 agent service");
    } else {
        info!("Service not found, nothing to uninstall");
    }

    Ok(())
}

pub async fn status_service() -> Result<()> {
    let output = Command::new("systemctl")
        .args(["status", SERVICE_NAME])
        .output()
        .context("Failed to query service status")?;

    let msg = match output.stdout.len() == 0 {
        true => String::from_utf8_lossy(&output.stderr),
        false => String::from_utf8_lossy(&output.stdout),
    };
    info!("{}", msg);
    Ok(())
}

pub async fn run() -> Result<()> {
    let _log_tx = init_tracing_with_log_layer("info");
    info!("Running agent");
    let shutdown = signal::ctrl_c();
    pin!(shutdown);
    tokio::select! {
        _ = login_and_run() => {},
        _ = &mut shutdown => {
            info!("Received shutdown signal, stopping agent");
        }
    }

    Ok(())
}

async fn login_and_run() -> Result<()> {
    // retry login/register until wit works, then call agent_loop
    loop {
        let success = register_agent(None).await;
        if success.is_ok() {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    let config = Config::load().context("Failed to load configuration")?;
    let token = AuthManager::get_agent_token()?;
    let res = report_agent_details(
        &config.api_url,
        &config.agent_id,
        &token,
        config.enable_geo_lookup,
        config.trust_invalid_server_cert,
    )
    .await;

    tokio::task::spawn_local(async move {
        loop {
            println!("Starting log server...");
            if let Err(e) = serve_server().await {
                eprintln!("Log server crashed with error: {e}. Restarting in 2 seconds...");
            } else {
                eprintln!("Log server exited normally. Restarting in 2 seconds...");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    tokio::task::spawn_local(async move {
        loop {
            println!("Starting control tunnel...");
            if let Err(e) = server::connect_control_tunnel().await {
                eprintln!("Control tunnel crashed with error: {e}. Restarting in 2 seconds...");
            } else {
                eprintln!("Control tunnel exited normally. Restarting in 2 seconds...");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    if res.is_err() {
        error!("Failed to report agent details: {:?}", res);
    }

    agent_loop().await?;
    Ok(())
}

async fn agent_loop() -> Result<()> {
    loop {
        if let Err(e) = sync_with_backend().await {
            error!("Sync failed: {:?}", e);
        }
        sleep(Duration::from_secs(60)).await; // 5 minutes
    }
}

async fn sync_with_backend() -> Result<()> {
    info!("Syncing with backend...");

    let config = Config::load().context("Failed to load configuration")?;
    let last_instruciotn_hash = "";

    let token = AuthManager::get_agent_token()?;
    let metrics = collect_system_metrics().await?;
    let services = collect_all_services().await?;
    let _instruction = send_heartbeat(
        last_instruciotn_hash,
        &config.agent_id,
        &config.api_url,
        &token,
        metrics,
        services,
    )
    .await?;
    info!("Sync complete");
    Ok(())
}

pub async fn report_agent_details(
    api_url: &str,
    agent_id: &str,
    token: &str,
    enable_geo_lookup: bool,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    info!("Reporting agent details");

    // Build update body
    let body = server::UpdateAgentBody {
        client_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        system_info: Some(get_system_info(enable_geo_lookup).await?),
    };
    server::report_agent_details(api_url, token, agent_id, body, trust_invalid_server_cert).await
}

async fn get_system_info(enable_geo_lookup: bool) -> Result<server::AgentSystemInfo> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()?;

    let mut sys_info = server::AgentSystemInfo {
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
