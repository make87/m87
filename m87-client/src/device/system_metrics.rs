use anyhow::Result;
use std::{collections::HashMap, process::Command, sync::OnceLock};
use sysinfo::{Disks, Networks, System};
use tokio::sync::Mutex;

use m87_shared::metrics::{
    CpuCoreMetrics, CpuMetrics, DiskMetrics, GpuMetrics, MemoryMetrics, NetworkInterfaceMetrics,
    NetworkMetrics, SystemMetrics,
};

// ---------------------------------------------------------
// Global System instance (safe)
// ---------------------------------------------------------

static SYS: OnceLock<Mutex<System>> = OnceLock::new();

fn sys() -> &'static Mutex<System> {
    SYS.get_or_init(|| {
        let mut sys = System::new_all();
        sys.refresh_all();
        Mutex::new(sys)
    })
}

static PREV_NET: OnceLock<Mutex<HashMap<String, (u64, u64)>>> = OnceLock::new();

fn prev_net() -> &'static Mutex<HashMap<String, (u64, u64)>> {
    PREV_NET.get_or_init(|| Mutex::new(HashMap::new()))
}

// ---------------------------------------------------------
// Collect metrics
// ---------------------------------------------------------

pub async fn collect_system_metrics() -> Result<SystemMetrics> {
    // Scope for sys lock
    let (cpu, memory) = {
        let mut sys = sys().lock().await;

        // ---------------- CPU ----------------
        // CPU usage needs TWO calls with a pause
        sys.refresh_cpu_usage();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        sys.refresh_cpu_usage();

        let cores = sys.cpus().len();

        let avg_usage = if cores == 0 {
            0.0
        } else {
            sys.cpus().iter().map(|c| c.cpu_usage()).sum::<f32>() / cores as f32
        };

        let per_core = sys
            .cpus()
            .iter()
            .enumerate()
            .map(|(i, c)| CpuCoreMetrics {
                id: i,
                usage_percent: c.cpu_usage(),
            })
            .collect();

        let load = System::load_average();
        let cpu = CpuMetrics {
            usage_percent: avg_usage,
            cores,
            load_avg: (load.one as f32, load.five as f32, load.fifteen as f32),
            per_core,
        };

        // ---------------- MEMORY ----------------
        sys.refresh_memory();

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

        (cpu, memory)
    };

    // ---------------- DISKS ----------------
    // sysinfo 0.37 does NOT have per-instance disk refresh
    // Must recreate Disks list every time
    let disks = Disks::new_with_refreshed_list();

    let mut total_gb = 0;
    let mut used_gb = 0;

    for disk in &disks {
        let t = disk.total_space();
        let a = disk.available_space();
        total_gb += t / (1024 * 1024 * 1024);
        used_gb += (t - a) / (1024 * 1024 * 1024);
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

    // ---------------- NETWORK ----------------
    // Create a fresh snapshot of all interfaces
    let networks = Networks::new_with_refreshed_list();

    let interfaces = {
        let mut prev = prev_net().lock().await;
        let mut interfaces = Vec::new();

        for (name, data) in &networks {
            let rx_now = data.total_received();
            let tx_now = data.total_transmitted();

            // Prev values (default 0)
            let (rx_prev, tx_prev) = prev.get(name).cloned().unwrap_or((rx_now, tx_now));

            // Deltas = amount of data since last update
            let rx_delta = rx_now.saturating_sub(rx_prev);
            let tx_delta = tx_now.saturating_sub(tx_prev);

            // Store new snapshot
            prev.insert(name.to_string(), (rx_now, tx_now));

            interfaces.push(NetworkInterfaceMetrics {
                name: name.to_string(),
                rx_bytes: rx_delta,
                tx_bytes: tx_delta,
            });
        }

        interfaces
    };

    // total aggregated MB/s (optional)
    let total_rx: u64 = interfaces.iter().map(|i| i.rx_bytes).sum();
    let total_tx: u64 = interfaces.iter().map(|i| i.tx_bytes).sum();

    let network = NetworkMetrics {
        rx_mbps: total_rx as f32 / 1_000_000.0,
        tx_mbps: total_tx as f32 / 1_000_000.0,
        interfaces,
    };

    // ---------------- GPU ----------------
    let gpu = collect_gpu_metrics().unwrap_or_default();

    // ---------------- META ----------------
    let hostname = System::host_name().unwrap_or_else(|| "unknown".into());
    let os = System::name().unwrap_or_else(|| "Unknown".into());
    let arch = std::env::consts::ARCH.to_string();
    let uptime_secs = System::uptime();

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

// ---------------------------------------------------------
// GPU detection
// ---------------------------------------------------------

static HAS_NVIDIA_SMI: OnceLock<bool> = OnceLock::new();

fn has_nvidia_smi() -> bool {
    *HAS_NVIDIA_SMI.get_or_init(|| {
        Command::new("nvidia-smi")
            .arg("--help")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    })
}

fn collect_gpu_metrics() -> Result<Vec<GpuMetrics>> {
    if !has_nvidia_smi() {
        return Ok(Vec::new());
    }

    let out = Command::new("nvidia-smi")
        .args([
            "--query-gpu=name,utilization.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits",
        ])
        .output();

    let out = match out {
        Ok(o) if o.status.success() => o,
        _ => return Ok(Vec::new()),
    };

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

    Ok(gpus)
}
