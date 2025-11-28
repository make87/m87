use serde::{Deserialize, Serialize};

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
    /// existing fields
    pub usage_percent: f32, // average over all cores
    pub cores: usize,
    pub load_avg: (f32, f32, f32),

    /// new detailed per-core metrics
    pub per_core: Vec<CpuCoreMetrics>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CpuCoreMetrics {
    pub id: usize,
    pub usage_percent: f32,
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
    /// existing aggregate metrics
    pub rx_mbps: f32,
    pub tx_mbps: f32,

    /// new per-interface stats
    pub interfaces: Vec<NetworkInterfaceMetrics>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NetworkInterfaceMetrics {
    pub name: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GpuMetrics {
    pub name: String,
    pub usage_percent: f32,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
}
