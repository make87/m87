use anyhow::Result;

use sysinfo::System;

use crate::server;
use crate::util::network::get_public_ip;
use libc::{geteuid, getpwuid};
use std::ffi::CStr;

fn username() -> String {
    unsafe {
        let pw = getpwuid(geteuid());
        if pw.is_null() {
            return "unknown".into();
        }
        let name = CStr::from_ptr((*pw).pw_name);
        name.to_string_lossy().into_owned()
    }
}

pub async fn get_system_info() -> Result<server::DeviceSystemInfo> {
    let mut sys_info = server::DeviceSystemInfo {
        ..Default::default()
    };

    if let Ok(ip) = get_public_ip().await {
        sys_info.public_ip_address = Some(ip);
    }

    #[cfg(target_arch = "x86_64")]
    {
        sys_info.architecture = "amd64".to_string();
    }

    #[cfg(target_arch = "aarch64")]
    {
        sys_info.architecture = "arm64".to_string();
    }
    // add

    sys_info.username = username();

    let mut sys = System::new_all();
    sys.refresh_all();

    sys_info.cores = Some(sys.cpus().len() as u32);
    sys_info.cpu_name = sys
        .cpus()
        .first()
        .map(|c| c.brand().to_string())
        .unwrap_or_else(|| "not found".to_string());
    sys_info.memory = Some((sys.total_memory() as f64) / 1024. / 1024. / 1024.);
    sys_info.hostname = System::host_name().unwrap_or_else(|| "not found".to_string());
    sys_info.operating_system = format!(
        "{} {}",
        System::name().unwrap_or_else(|| "Unknown".to_string()),
        System::os_version().unwrap_or_else(|| "".to_string())
    );

    Ok(sys_info)
}
