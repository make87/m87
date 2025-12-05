use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::sync::{broadcast, watch};

use crate::device::system_metrics::collect_system_metrics;

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

pub struct SharedReceiver {
    rx: broadcast::Receiver<String>,
    task: Arc<SharedTask>,
}

impl SharedReceiver {
    pub fn new(task: Arc<SharedTask>, rx: broadcast::Receiver<String>) -> Self {
        Self { task, rx }
    }

    pub fn inner_mut(&mut self) -> &mut broadcast::Receiver<String> {
        &mut self.rx
    }
}

impl Drop for SharedReceiver {
    fn drop(&mut self) {
        self.task.dec_or_shutdown();
    }
}

pub static TASKS: Lazy<DashMap<String, Arc<SharedTask>>> = Lazy::new(DashMap::new);

pub async fn acquire_metrics_task(name: &str) -> (Arc<SharedTask>, SharedReceiver) {
    if let Some(entry) = TASKS.get(name) {
        entry.inc();
        let rx = SharedReceiver::new(entry.clone(), entry.tx.subscribe());
        return (entry.clone(), rx);
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
            use tokio::time::{Duration, interval};
            let mut ticker = interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = ticker.tick() => {
                        match collect_system_metrics().await {
                            Ok(m) => {
                                if let Ok(payload) = serde_json::to_string(&m) {
                                    let _ = task.tx.send(payload);
                                }
                            }
                            Err(_) => {}
                        }
                    }
                }
            }

            TASKS.remove(&task.name);
        }
    });

    let rx = SharedReceiver::new(task.clone(), task.tx.subscribe());
    (task.clone(), rx)
}
