use libmacchina::{GeneralReadout, MemoryReadout};

pub fn get_detailed_printout() -> String {
    use libmacchina::traits::GeneralReadout as _;

    let general_readout = GeneralReadout::new();

    // There are many more metrics we can query
    // i.e. username, distribution, terminal, shell, etc.
    let cpu_cores = general_readout.cpu_cores().unwrap_or(0); // 8 [logical cores]
    let cpu = general_readout
        .cpu_model_name()
        .unwrap_or("not found".to_string()); // Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    let uptime = general_readout.uptime().unwrap_or(0); // 1500 [in seconds]
    let name = general_readout
        .hostname()
        .unwrap_or("not found".to_string()); // my-hostname
    let distribution = general_readout
        .distribution()
        .unwrap_or("not found".to_string()); // Ubuntu 20.04.2 LTS
    let gpus = general_readout.gpus().unwrap_or(vec![]);
    // joined string
    let gpus = gpus.join(", "); // NVIDIA GeForce MX250, Intel UHD Graphics 620

    // Now we'll import the MemoryReadout trait to get an
    // idea of what the host's memory usage looks like.
    use libmacchina::traits::MemoryReadout as _;

    let memory_readout = MemoryReadout::new();

    let total_mem = memory_readout.total().unwrap_or(0); // 20242204 [in kB]
    let machine = general_readout.machine().unwrap_or("not found".to_string()); // x86_64
                                                                                // create nicely formatted string

    let system_info = format!(
        "System Info:\n\
        CPU: {} with {} cores\n\
        GPUs: {}\n\
        Uptime: {} seconds\n\
        Hostname: {}\n\
        Distribution: {}\n\
        Machine: {}\n\
        Memory: {} kB",
        cpu, cpu_cores, gpus, uptime, name, distribution, machine, total_mem
    );

    system_info
}

pub fn get_operating_system() -> String {
    use libmacchina::traits::GeneralReadout as _;

    let general_readout = GeneralReadout::new();
    let distribution = general_readout
        .distribution()
        .unwrap_or("not found".to_string());

    distribution
}
