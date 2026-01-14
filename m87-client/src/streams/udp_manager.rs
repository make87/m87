use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, time::Instant};
use tokio::sync::{Mutex, mpsc};

use crate::streams::stream_type::UdpTarget;

#[derive(Clone)]
pub struct UdpChannel {
    pub target: UdpTarget,
    pub sender: mpsc::Sender<Bytes>, // sends payload to UDP to QUIC worker
    pub last_used: Instant,
}

#[derive(Clone)]
pub struct UdpChannelManager {
    inner: Arc<Mutex<UdpChannelState>>,
}

struct UdpChannelState {
    next_id: u32,
    channels: HashMap<u32, UdpChannel>,
}

impl UdpChannelManager {
    pub fn new() -> Self {
        let manager = Self {
            inner: Arc::new(Mutex::new(UdpChannelState {
                next_id: 1,
                channels: HashMap::new(),
            })),
        };

        tokio::spawn({
            let mgr = manager.inner.clone();
            async move {
                let timeout = Duration::from_secs(30);
                loop {
                    tokio::time::sleep(Duration::from_secs(10)).await;

                    let mut state = mgr.lock().await;
                    let now = Instant::now();

                    state
                        .channels
                        .retain(|_, ch| now.duration_since(ch.last_used) < timeout);
                }
            }
        });
        manager
    }

    pub async fn alloc(&self, target: UdpTarget) -> (u32, mpsc::Receiver<Bytes>) {
        let mut g = self.inner.lock().await;

        let id = g.next_id;
        g.next_id += 1;

        let (tx, rx) = mpsc::channel::<Bytes>(1024);

        g.channels.insert(
            id,
            UdpChannel {
                target,
                sender: tx,
                last_used: Instant::now(),
            },
        );

        (id, rx)
    }

    pub async fn get(&self, id: u32) -> Option<UdpChannel> {
        let g = self.inner.lock().await;
        let mut ch = g.channels.get(&id).cloned();

        if let Some(ch_some) = &mut ch {
            ch_some.last_used = Instant::now();
        }

        ch
    }

    pub async fn remove(&self, id: u32) -> bool {
        let mut g = self.inner.lock().await;
        g.channels.remove(&id).is_some()
    }

    pub async fn remove_all(&self) {
        let mut g = self.inner.lock().await;
        g.channels.clear();
    }

    #[cfg(test)]
    pub fn new_without_cleanup() -> Self {
        Self {
            inner: Arc::new(Mutex::new(UdpChannelState {
                next_id: 1,
                channels: HashMap::new(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_target() -> UdpTarget {
        UdpTarget {
            remote_host: "127.0.0.1".to_string(),
            remote_port: 8080,
            local_port: 9090,
        }
    }

    #[tokio::test]
    async fn test_udp_manager_alloc_increments_id() {
        let mgr = UdpChannelManager::new_without_cleanup();

        let (id1, _rx1) = mgr.alloc(make_target()).await;
        let (id2, _rx2) = mgr.alloc(make_target()).await;
        let (id3, _rx3) = mgr.alloc(make_target()).await;

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[tokio::test]
    async fn test_udp_manager_get_existing() {
        let mgr = UdpChannelManager::new_without_cleanup();

        let (id, _rx) = mgr.alloc(make_target()).await;
        let channel = mgr.get(id).await;

        assert!(channel.is_some());
        let ch = channel.unwrap();
        assert_eq!(ch.target.remote_port, 8080);
        assert_eq!(ch.target.local_port, 9090);
    }

    #[tokio::test]
    async fn test_udp_manager_get_nonexistent() {
        let mgr = UdpChannelManager::new_without_cleanup();

        let channel = mgr.get(999).await;
        assert!(channel.is_none());
    }

    #[tokio::test]
    async fn test_udp_manager_remove_existing() {
        let mgr = UdpChannelManager::new_without_cleanup();

        let (id, _rx) = mgr.alloc(make_target()).await;
        let removed = mgr.remove(id).await;

        assert!(removed);
        assert!(mgr.get(id).await.is_none());
    }

    #[tokio::test]
    async fn test_udp_manager_remove_nonexistent() {
        let mgr = UdpChannelManager::new_without_cleanup();

        let removed = mgr.remove(999).await;
        assert!(!removed);
    }

    #[tokio::test]
    async fn test_udp_manager_remove_all() {
        let mgr = UdpChannelManager::new_without_cleanup();

        let (id1, _rx1) = mgr.alloc(make_target()).await;
        let (id2, _rx2) = mgr.alloc(make_target()).await;

        mgr.remove_all().await;

        assert!(mgr.get(id1).await.is_none());
        assert!(mgr.get(id2).await.is_none());
    }
}
