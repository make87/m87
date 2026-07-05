use std::collections::HashMap;
use std::sync::Arc;

use quinn::Connection;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Clone)]
pub struct RelayState {
    tunnels: Arc<RwLock<HashMap<String, Connection>>>,
    lost: Arc<RwLock<HashMap<String, ()>>>, // just a set, we don't need Instant
}

impl RelayState {
    pub fn new() -> Self {
        Self {
            tunnels: Arc::new(RwLock::new(HashMap::new())),
            lost: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert a new tunnel and close the old one if present.
    pub async fn replace_tunnel(&self, device_short_id: &str, conn: Connection) {
        info!("Replacing tunnel for device {}", device_short_id);

        // Replace old tunnel atomically.
        let old = {
            let mut t = self.tunnels.write().await;
            t.insert(device_short_id.to_string(), conn)
        };

        // Remove "lost" flag — the device is now online.
        {
            let mut lost = self.lost.write().await;
            lost.remove(device_short_id);
        }

        // Clean up old tunnel if there was one
        if let Some(old_conn) = old {
            warn!("Closing old tunnel for device {}", device_short_id);
            old_conn.close(0u32.into(), b"replaced-by-new-connection");
        }
    }

    /// Remove the tunnel ONLY if this connection is still the active one.
    pub async fn remove_if_match(&self, device_short_id: &str, conn_id: usize) {
        let mut tunnels = self.tunnels.write().await;

        if let Some(active) = tunnels.get(device_short_id) {
            // Connection ID must be compared to ensure we don't remove a newer tunnel
            if active.stable_id() == conn_id {
                info!("Removing tunnel for device {} (matched)", device_short_id);
                tunnels.remove(device_short_id);

                // Mark device lost
                let mut lost = self.lost.write().await;
                lost.insert(device_short_id.to_string(), ());
            } else {
                warn!(
                    "Skipping removal for device {} because connection ID does not match (stale close event)",
                    device_short_id
                );
            }
        }
    }

    /// Returns true only if device has an active and *not lost* tunnel.
    ///
    /// Locks are acquired `tunnels` before `lost`, matching `remove_if_match`
    /// and `replace_tunnel`. A consistent order across all methods is what
    /// prevents the AB-BA deadlock between the two relay locks.
    pub async fn has_tunnel(&self, device_short_id: &str) -> bool {
        let tunnels = self.tunnels.read().await;
        if !tunnels.contains_key(device_short_id) {
            return false;
        }

        let lost = self.lost.read().await;
        !lost.contains_key(device_short_id)
    }

    /// Return active (non-lost) tunnel.
    ///
    /// Locks are acquired `tunnels` before `lost` (see `has_tunnel`).
    pub async fn get_tunnel(&self, device_short_id: &str) -> Option<Connection> {
        let tunnels = self.tunnels.read().await;
        let conn = tunnels.get(device_short_id).cloned()?;

        let lost = self.lost.read().await;
        if lost.contains_key(device_short_id) {
            return None;
        }
        Some(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Reproduces the AB-BA deadlock between `tunnels` and `lost`.
    ///
    /// `remove_if_match` acquires `tunnels.write()` then `lost.write()`
    /// (tunnels -> lost), while `has_tunnel`/`get_tunnel` acquire
    /// `lost.read()` then `tunnels.read()` (lost -> tunnels). With a reader
    /// in flight, a writer holding `tunnels` can never obtain `lost` and the
    /// whole relay wedges.
    ///
    /// We simulate the writer side by holding `tunnels.write()` (exactly what
    /// `remove_if_match` holds when it reaches `lost.write()`), run the *real*
    /// `has_tunnel` concurrently, then assert the writer can still acquire
    /// `lost.write()`. On the pre-fix reader ordering this times out.
    #[tokio::test]
    async fn writer_is_not_deadlocked_by_concurrent_reader() {
        let state = RelayState::new();

        // Writer side: hold `tunnels.write()`, as `remove_if_match` does.
        let tunnels_guard = state.tunnels.write().await;

        // Reader side: real `has_tunnel`, running concurrently.
        let reader_state = state.clone();
        let reader = tokio::spawn(async move { reader_state.has_tunnel("device-1").await });

        // Let the reader reach its first lock acquisition and park on the
        // second one. On current-thread runtime this yields to `reader`.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Now complete the writer's second acquisition. With a consistent
        // `tunnels -> lost` order this succeeds immediately; with the buggy
        // `lost -> tunnels` reader order the reader holds `lost.read()` while
        // blocked on `tunnels.read()`, so this blocks forever.
        let acquired = timeout(Duration::from_secs(3), state.lost.write()).await;
        assert!(
            acquired.is_ok(),
            "writer holding `tunnels` could not acquire `lost` while a reader \
             was in flight — AB-BA deadlock between the relay locks"
        );

        drop(tunnels_guard);
        let _ = reader.await;
    }
}
