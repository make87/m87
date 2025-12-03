use sysinfo::System;

pub struct Readout {
    pub cpu_cores: u32,
    pub cpu: String,
    pub name: String,
    pub distribution: String,
    pub memory: u64,
}

pub fn get_detailed_printout() -> String {
    let mut sys = System::new_all();
    sys.refresh_all();

    let cpu_cores = sys.cpus().len();
    let cpu = sys
        .cpus()
        .first()
        .map(|c| c.brand().to_string())
        .unwrap_or_else(|| "not found".to_string());
    let uptime = System::uptime();
    let name = System::host_name().unwrap_or_else(|| "not found".to_string());
    let distribution = format!(
        "{} {}",
        System::name().unwrap_or_else(|| "Unknown".to_string()),
        System::os_version().unwrap_or_else(|| "".to_string())
    );
    let machine = System::cpu_arch();
    let total_mem = sys.total_memory() / 1024; // bytes to kB

    let system_info = format!(
        "System Info:\n\
        CPU: {} with {} cores\n\
        GPUs: \n\
        Uptime: {} seconds\n\
        Hostname: {}\n\
        Distribution: {}\n\
        Machine: {}\n\
        Memory: {} kB",
        cpu, cpu_cores, uptime, name, distribution, machine, total_mem
    );

    system_info
}

pub fn get_readout() -> Readout {
    let mut sys = System::new_all();
    sys.refresh_all();

    let memory = sys.total_memory() / 1024; // bytes to kB

    Readout {
        cpu_cores: sys.cpus().len() as u32,
        cpu: sys
            .cpus()
            .first()
            .map(|c| c.brand().to_string())
            .unwrap_or_else(|| "not found".to_string()),
        name: System::host_name().unwrap_or_else(|| "not found".to_string()),
        distribution: format!(
            "{} {}",
            System::name().unwrap_or_else(|| "Unknown".to_string()),
            System::os_version().unwrap_or_else(|| "".to_string())
        ),
        memory,
    }
}
