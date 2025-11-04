// src/relay/relay_state.rs
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_yamux::Session;

use crate::response::{ServerError, ServerResult};

#[derive(Clone, Debug)]
pub struct ForwardMeta {
    pub agent_id: String,
    pub target_port: u16,
    pub allowed_ips: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct RelayState {
    /// agent_id -> yamux session for active tunnel
    pub tunnels: Arc<
        RwLock<HashMap<String, Arc<Mutex<Session<tokio_rustls::server::TlsStream<TcpStream>>>>>>,
    >,

    /// sni_host -> ForwardMeta
    pub forwards: Arc<RwLock<HashMap<String, ForwardMeta>>>,
}

impl RelayState {
    pub fn new() -> ServerResult<Self> {
        Ok(Self {
            tunnels: Arc::new(RwLock::new(HashMap::new())),
            forwards: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    // --- Tunnel management --- agent to nexus conenciton
    pub async fn register_tunnel(
        &self,
        agent_id: String,
        connection: Session<tokio_rustls::server::TlsStream<TcpStream>>,
    ) {
        self.tunnels
            .write()
            .await
            .insert(agent_id, Arc::new(Mutex::new(connection)));
    }

    pub async fn remove_tunnel(&self, agent_id: &str) {
        self.tunnels.write().await.remove(agent_id);
    }

    pub async fn get_tunnel(
        &self,
        agent_id: &str,
    ) -> Option<Arc<Mutex<Session<tokio_rustls::server::TlsStream<TcpStream>>>>> {
        self.tunnels.read().await.get(agent_id).cloned()
    }

    // --- Forward management --- public to agent proxing
    /// `sni_host` is the hostname clients will connect to (e.g. camera1.nexus.make87.com)
    pub async fn register_forward(
        &self,
        sni_host: String,
        agent_id: String,
        target_port: u16,
        allowed_ips: Option<Vec<String>>,
    ) {
        let meta = ForwardMeta {
            agent_id,
            target_port,
            allowed_ips,
        };
        self.forwards.write().await.insert(sni_host, meta);
    }

    pub async fn remove_forward(&self, sni_host: &str) {
        self.forwards.write().await.remove(sni_host);
    }

    pub async fn get_forward(&self, sni_host: &str) -> Option<ForwardMeta> {
        self.forwards.read().await.get(sni_host).cloned()
    }

    pub async fn list_forwards_for_agent(&self, agent_id: &str) -> Vec<(String, ForwardMeta)> {
        self.forwards
            .read()
            .await
            .iter()
            .filter_map(|(sni, meta)| {
                if meta.agent_id == agent_id {
                    Some((sni.clone(), meta.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    // allow all ips by setting allowed ips to none
    pub async fn open_all_ips(&self, sni_host: &str) -> ServerResult<()> {
        let mut forwards = self.forwards.write().await;
        let meta = forwards
            .get_mut(sni_host)
            .ok_or_else(|| ServerError::not_found(&format!("forward {sni_host} not found")))?;

        meta.allowed_ips = None;
        Ok(())
    }

    /// Add one or more IPs to a forward’s whitelist (idempotent).
    pub async fn add_allowed_ips(&self, sni_host: &str, new_ips: Vec<String>) -> ServerResult<()> {
        let mut forwards = self.forwards.write().await;
        let meta = forwards
            .get_mut(sni_host)
            .ok_or_else(|| ServerError::not_found(&format!("forward {sni_host} not found")))?;

        match &mut meta.allowed_ips {
            Some(ips) => {
                for ip in new_ips {
                    if !ips.contains(&ip) {
                        ips.push(ip);
                    }
                }
            }
            None => {
                // If None, it means all IPs are allowed — disallowing again requires explicitly setting Some(vec)
                meta.allowed_ips = Some(new_ips);
            }
        }
        Ok(())
    }

    pub async fn remove_allowed_ips(
        &self,
        sni_host: &str,
        ips_to_remove: Vec<String>,
    ) -> ServerResult<()> {
        let mut forwards = self.forwards.write().await;
        let meta = forwards
            .get_mut(sni_host)
            .ok_or_else(|| ServerError::not_found(&format!("forward {sni_host} not found")))?;

        if let Some(ref mut allowed_ips) = meta.allowed_ips {
            allowed_ips.retain(|ip| !ips_to_remove.contains(ip));
            if allowed_ips.is_empty() {
                // If list becomes empty, you might want to interpret that as “no access”
                meta.allowed_ips = Some(vec![]);
            }
        }
        Ok(())
    }
}
