use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::process::Command;
use sysinfo::{Disks, Networks, System};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SystemMetrics {
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub uptime_secs: u64,
    pub cpu: CpuMetrics,
    pub memory: MemoryMetrics,
    pub disk: DiskMetrics,
    pub network: NetworkMetrics,
    pub gpu: Vec<GpuMetrics>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CpuMetrics {
    pub usage_percent: f32,
    pub cores: usize,
    pub load_avg: (f32, f32, f32),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemoryMetrics {
    pub total_mb: u64,
    pub used_mb: u64,
    pub usage_percent: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DiskMetrics {
    pub total_gb: u64,
    pub used_gb: u64,
    pub usage_percent: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NetworkMetrics {
    pub rx_mbps: f32,
    pub tx_mbps: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GpuMetrics {
    pub name: String,
    pub usage_percent: f32,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
}

pub async fn collect_system_metrics() -> Result<SystemMetrics> {
    // Create the base system snapshot
    let mut sys = System::new_all();
    sys.refresh_all();

    let hostname = System::host_name().unwrap_or_else(|| "unknown".into());
    let os = System::name().unwrap_or_else(|| "Unknown".into());
    let arch = std::env::consts::ARCH.to_string();
    let uptime_secs = System::uptime();

    // ---------- CPU ----------
    let cpu_usage = if sys.cpus().is_empty() {
        0.0
    } else {
        sys.cpus().iter().map(|c| c.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32
    };

    let load = System::load_average();
    let cpu = CpuMetrics {
        usage_percent: cpu_usage,
        cores: sys.cpus().len(),
        load_avg: (load.one as f32, load.five as f32, load.fifteen as f32),
    };

    // ---------- Memory ----------
    let total_mb = sys.total_memory() / 1024;
    let used_mb = sys.used_memory() / 1024;
    let memory = MemoryMetrics {
        total_mb,
        used_mb,
        usage_percent: if total_mb == 0 {
            0.0
        } else {
            (used_mb as f32 / total_mb as f32) * 100.0
        },
    };

    // ---------- Disks ----------
    let disks = Disks::new_with_refreshed_list();
    let mut total_gb = 0;
    let mut used_gb = 0;
    for disk in &disks {
        total_gb += disk.total_space() / (1024 * 1024 * 1024);
        used_gb += (disk.total_space() - disk.available_space()) / (1024 * 1024 * 1024);
    }
    let disk = DiskMetrics {
        total_gb,
        used_gb,
        usage_percent: if total_gb == 0 {
            0.0
        } else {
            (used_gb as f32 / total_gb as f32) * 100.0
        },
    };

    // ---------- Networks ----------
    let networks = Networks::new_with_refreshed_list();
    let mut rx_bytes = 0u64;
    let mut tx_bytes = 0u64;
    for (_, data) in &networks {
        rx_bytes = rx_bytes.saturating_add(data.total_received());
        tx_bytes = tx_bytes.saturating_add(data.total_transmitted());
    }
    let network = NetworkMetrics {
        rx_mbps: (rx_bytes as f32) / 1_000_000.0,
        tx_mbps: (tx_bytes as f32) / 1_000_000.0,
    };

    // ---------- GPU ----------
    let gpu = collect_gpu_metrics().unwrap_or_default();

    Ok(SystemMetrics {
        hostname,
        os,
        arch,
        uptime_secs,
        cpu,
        memory,
        disk,
        network,
        gpu,
    })
}

// ---------- GPU collection (unchanged) ----------
fn collect_gpu_metrics() -> Result<Vec<GpuMetrics>> {
    if let Ok(out) = Command::new("nvidia-smi")
        .args([
            "--query-gpu=name,utilization.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits",
        ])
        .output()
    {
        if out.status.success() {
            let s = String::from_utf8_lossy(&out.stdout);
            let mut gpus = Vec::new();
            for line in s.lines().filter(|l| !l.trim().is_empty()) {
                let parts: Vec<_> = line.split(',').map(|x| x.trim()).collect();
                if parts.len() >= 4 {
                    gpus.push(GpuMetrics {
                        name: parts[0].to_string(),
                        usage_percent: parts[1].parse::<f32>().unwrap_or(0.0),
                        memory_used_mb: parts[2].parse::<u64>().unwrap_or(0),
                        memory_total_mb: parts[3].parse::<u64>().unwrap_or(0),
                    });
                }
            }
            if !gpus.is_empty() {
                return Ok(gpus);
            }
        }
    }

    Ok(Vec::new())
}
