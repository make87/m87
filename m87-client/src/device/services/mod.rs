use anyhow::Result;
use tracing::warn;

mod docker;
mod podman;
pub mod service_info;
mod systemd;

use service_info::ServiceInfo;

pub async fn collect_all_services() -> Result<Vec<ServiceInfo>> {
    let mut all = Vec::new();

    if let Ok(mut v) = docker::collect_docker_services().await {
        all.append(&mut v);
    } else {
        warn!("Docker service collection failed");
    }

    if let Ok(mut v) = podman::collect_podman_services().await {
        all.append(&mut v);
    } else {
        warn!("Podman service collection failed");
    }

    if let Ok(mut v) = systemd::collect_systemd_services().await {
        all.append(&mut v);
    } else {
        warn!("Systemd service collection failed");
    }

    Ok(all)
}
