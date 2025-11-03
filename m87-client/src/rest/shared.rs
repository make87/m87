use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{broadcast, watch};

/// Shared, ref-counted background publishers (logs/metrics/etc.)
pub struct SharedTask {
    pub name: String,
    pub tx: broadcast::Sender<String>,
    ref_count: AtomicUsize,
    shutdown_tx: watch::Sender<bool>,
}

impl SharedTask {
    fn new(name: String, tx: broadcast::Sender<String>, shutdown_tx: watch::Sender<bool>) -> Self {
        Self {
            name,
            tx,
            ref_count: AtomicUsize::new(0),
            shutdown_tx,
        }
    }

    pub fn inc(&self) {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn dec_or_shutdown(&self) {
        if self.ref_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            let _ = self.shutdown_tx.send(true);
        }
    }
}

pub static TASKS: Lazy<DashMap<String, Arc<SharedTask>>> = Lazy::new(DashMap::new);

/// Subscribe to a task by name, or spawn it if missing. Returns (task, rx).
pub async fn acquire_process_task(
    name: &str,
    program: &str,
    args: &[&str],
) -> (Arc<SharedTask>, broadcast::Receiver<String>) {
    if let Some(entry) = TASKS.get(name) {
        entry.inc();
        return (entry.clone(), entry.tx.subscribe());
    }

    // create and spawn
    let (tx, _) = broadcast::channel::<String>(256);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let task = Arc::new(SharedTask::new(
        name.to_string(),
        tx.clone(),
        shutdown_tx.clone(),
    ));
    task.inc();

    let key = name.to_string();
    TASKS.insert(key.clone(), task.clone());

    tokio::spawn({
        let task = task.clone();
        let prog = program.to_string();
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();

        async move {
            let mut child = match Command::new(&prog)
                .args(&args)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
            {
                Ok(c) => c,
                Err(e) => {
                    let _ = task
                        .tx
                        .send(format!("Failed to spawn {} {:?}: {}", prog, args, e));
                    TASKS.remove(&task.name);
                    return;
                }
            };

            let mut out = BufReader::new(child.stdout.take().unwrap());
            let mut err = BufReader::new(child.stderr.take().unwrap());

            let tx_out = task.tx.clone();
            let mut shutdown_rx_out = shutdown_rx.clone();
            let out_task = tokio::spawn(async move {
                let mut line = String::new();
                loop {
                    tokio::select! {
                        _ = shutdown_rx_out.changed() => break,
                        n = out.read_line(&mut line) => {
                            if n.unwrap_or(0) == 0 { break; }
                            let _ = tx_out.send(line.trim_end().to_string());
                            line.clear();
                        }
                    }
                }
            });

            let tx_err = task.tx.clone();
            let mut shutdown_rx_err = shutdown_rx.clone();
            let err_task = tokio::spawn(async move {
                let mut line = String::new();
                loop {
                    tokio::select! {
                        _ = shutdown_rx_err.changed() => break,
                        n = err.read_line(&mut line) => {
                            if n.unwrap_or(0) == 0 { break; }
                            let _ = tx_err.send(line.trim_end().to_string());
                            line.clear();
                        }
                    }
                }
            });

            // wait for shutdown
            let _ = shutdown_rx.changed().await;
            let _ = child.kill().await;
            let _ = out_task.await;
            let _ = err_task.await;

            TASKS.remove(&task.name);
        }
    });

    (task.clone(), task.clone().tx.subscribe())
}

/// Spawn (or reuse) a single metrics loop that publishes JSON each second.
pub async fn acquire_metrics_task(name: &str) -> (Arc<SharedTask>, broadcast::Receiver<String>) {
    if let Some(entry) = TASKS.get(name) {
        entry.inc();
        return (entry.clone(), entry.tx.subscribe());
    }

    let (tx, _) = broadcast::channel::<String>(256);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let task = Arc::new(SharedTask::new(
        name.to_string(),
        tx.clone(),
        shutdown_tx.clone(),
    ));
    task.inc();

    TASKS.insert(name.to_string(), task.clone());

    tokio::spawn({
        let task = task.clone();
        async move {
            use serde_json::json;
            use sysinfo::{Components, Disks, Networks, System};
            use tokio::time::{interval, Duration};

            let mut system = System::new_all();
            let mut networks = Networks::new_with_refreshed_list();
            let mut ticker = interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = ticker.tick() => {
                        system.refresh_all();
                        networks.refresh(true);

                        let per_cpu: Vec<_> = system.cpus().iter().map(|c| {
                            json!({"name": c.name(), "usage_percent": c.cpu_usage()})
                        }).collect();

                        let total = system.total_memory();
                        let used = system.used_memory();
                        let total_swap = system.total_swap();
                        let used_swap = system.used_swap();
                        let mem_pct = if total > 0 { (used as f64 / total as f64) * 100.0 } else { 0.0 };

                        let nets: Vec<_> = networks.iter().map(|(n, d)| {
                            json!({"name": n, "rx_bytes": d.received(), "tx_bytes": d.transmitted()})
                        }).collect();

                        let disks = Disks::new_with_refreshed_list();
                        let disk_info: Vec<_> = disks.iter().map(|d| {
                            json!({"name": d.name().to_str().unwrap_or("Unknown"),
                                   "total_space": d.total_space(),
                                   "available_space": d.available_space()})
                        }).collect();

                        let comps = Components::new_with_refreshed_list();
                        let temps: Vec<_> = comps.iter().map(|c| {
                            json!({"component": c.label(), "temperature": c.temperature()})
                        }).collect();

                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

                        let payload = json!({
                            "cpus": per_cpu,
                            "memory": {
                                "total": total, "used": used,
                                "free": total.saturating_sub(used),
                                "usage_percent": mem_pct,
                                "total_swap": total_swap, "used_swap": used_swap
                            },
                            "networks": nets,
                            "disks": disk_info,
                            "temperatures": temps,
                            "time": now
                        }).to_string();

                        let _ = task.tx.send(payload);
                    }
                }
            }

            TASKS.remove(&task.name);
        }
    });

    (task.clone(), task.clone().tx.subscribe())
}
