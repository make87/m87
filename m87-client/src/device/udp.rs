use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use tokio::{
    io::AsyncWriteExt,
    net::UdpSocket,
    sync::{Mutex, RwLock, mpsc},
};
use tracing::debug;

use crate::streams::{
    quic::open_quic_stream,
    stream_type::{Additions, Protocols, StreamType},
};

const FLOW_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

pub struct UdpFlow {
    pub last_seen: Arc<Mutex<Instant>>,
    pub tx: mpsc::Sender<Vec<u8>>,
}

pub struct UdpFlowManager {
    flows: Arc<RwLock<HashMap<(IpAddr, u16), Arc<UdpFlow>>>>,
    conn: quinn::Connection,
    udp: Arc<UdpSocket>,
}

impl UdpFlowManager {
    pub fn new(conn: quinn::Connection, udp: Arc<UdpSocket>) -> Self {
        Self {
            flows: Arc::new(RwLock::new(HashMap::new())),
            conn,
            udp,
        }
    }

    pub async fn get_or_create_flow(
        &self,
        src_ip: IpAddr,
        src_port: u16,
        token: String,
        remote_port: u16,
        host: Option<String>,
        addition: Option<Additions>,
    ) -> Result<mpsc::Sender<Vec<u8>>> {
        let key = (src_ip, src_port);

        // existing?
        if let Some(flow) = self.flows.read().await.get(&key) {
            *flow.last_seen.lock().await = Instant::now();
            return Ok(flow.tx.clone());
        }

        // create stream type
        let stream_type = StreamType::Port {
            token,
            port: remote_port,
            protocol: Protocols::Udp,
            host,
            addition,
        };

        // open QUIC stream
        let io = Arc::new(Mutex::new(open_quic_stream(&self.conn, stream_type).await?));

        // channel for UDP uplink
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(64);

        let last_seen = Arc::new(Mutex::new(Instant::now()));
        let io_cl = io.clone();
        let udp_cl = self.udp.clone();
        let target_addr = SocketAddr::new(src_ip, src_port);
        let last_seen_cl = last_seen.clone();

        tokio::spawn(async move {
            let mut quic_buf = vec![0u8; 65535];

            loop {
                tokio::select! {

                    // UDP → QUIC
                    Some(packet) = rx.recv() => {
                        *last_seen_cl.lock().await = Instant::now();
                        let mut io = io_cl.lock().await;
                        let lenb = (packet.len() as u16).to_be_bytes();
                        if io.send.write_all(&lenb).await.is_err() { break; }
                        if io.send.write_all(&packet).await.is_err() { break; }
                        let _ = io.send.flush().await;
                    }

                    // QUIC → UDP
                    result = (async {
                        let mut io = io_cl.lock().await;

                        let mut lenb = [0u8; 2];
                        io.recv.read_exact(&mut lenb).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                        let size = u16::from_be_bytes(lenb) as usize;

                        io.recv.read_exact(&mut quic_buf[..size]).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

                        Ok::<usize, std::io::Error>(size)
                    }) => {
                        match result {
                            Ok(size) => {
                                let _ = udp_cl.send_to(&quic_buf[..size], target_addr).await;
                            }
                            Err(_) => {
                                break;
                            }
                        }
                    }

                }
            }

            // stream ended or error → let manager clean it up
        });

        // Insert flow
        let flow = Arc::new(UdpFlow {
            last_seen,
            tx: tx.clone(),
        });
        self.flows.write().await.insert(key, flow);

        Ok(tx)
    }

    /// Cleanup idle UDP flows.
    pub async fn cleanup_task(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            let mut map = self.flows.write().await;
            let now = Instant::now();

            map.retain(|key, flow| {
                let last = *flow.last_seen.blocking_lock();
                let alive = now.duration_since(last) < FLOW_IDLE_TIMEOUT;
                if !alive {
                    debug!("Reaping idle UDP flow {:?}", key);
                }
                alive
            });
        }
    }
}
