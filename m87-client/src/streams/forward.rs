use std::{net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use std::{collections::HashMap, time::Instant};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, UdpSocket, UnixStream},
};
use tokio::{
    sync::Mutex,
    time::{Duration, timeout},
};
use tracing::{debug, info, warn};

use crate::{
    streams::{
        quic::QuicIo,
        stream_type::{SocketTarget, TcpTarget, ForwardTarget, UdpTarget},
        udp_manager::UdpChannelManager,
    },
    util::udp::{decode_socket_addr, encode_socket_addr},
};

const MAX_PACKET: usize = 65535;
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn handle_port_forward_io(
    forward_target: ForwardTarget,
    mut io: QuicIo,
    manager: UdpChannelManager,
    datagram_tx: tokio::sync::mpsc::Sender<(u32, Bytes)>,
) {
    match forward_target {
        ForwardTarget::Tcp(target) => tcp_forward(target, &mut io).await,

        ForwardTarget::Udp(target) => udp_unicast_forward(target, io, manager, datagram_tx).await,
        ForwardTarget::Socket(target) => socket_forward(target, io).await,
        ForwardTarget::Vpn(_target) => {}
    }
}

pub async fn tcp_forward(target: TcpTarget, io: &mut QuicIo) {
    match TcpStream::connect((target.remote_host.as_str(), target.remote_port)).await {
        Ok(mut local) => match tokio::io::copy_bidirectional(io, &mut local).await {
            Ok((a, b)) => tracing::info!("tcp forward closed (rx={a}, tx={b})"),
            Err(e) => tracing::error!("tcp forward error: {e}"),
        },
        Err(e) => {
            let _ = io
                .send
                .write_all(format!("TCP connect failed: {e}\n").as_bytes())
                .await;
        }
    }
}

pub async fn udp_unicast_forward(
    target: UdpTarget,
    mut io: QuicIo,
    manager: UdpChannelManager,
    datagram_tx: tokio::sync::mpsc::Sender<(u32, Bytes)>,
) {
    let (id, rx) = manager.alloc(target.clone()).await;
    // return ID to CLI/server over the stream
    let id_buf = id.to_be_bytes();
    if let Err(e) = io.send.write_all(&id_buf).await {
        tracing::error!("Failed to send ID: {e}");
        return;
    }
    if let Err(e) = io.send.finish() {
        tracing::error!("Failed to finish sending ID: {e}");
        return;
    }
    // spawn actual UDP forward worker
    tokio::spawn(start_udp_forward(id, target, rx, datagram_tx, manager));
}

pub async fn start_udp_forward(
    chan_id: u32,
    target: UdpTarget,
    mut rx: tokio::sync::mpsc::Receiver<Bytes>,
    datagram_tx: tokio::sync::mpsc::Sender<(u32, Bytes)>,
    manager: UdpChannelManager,
) {
    // per-source flow map
    #[derive(Clone)]
    struct Flow {
        sock: Arc<UdpSocket>,
        last: Arc<Mutex<Instant>>,
    }

    // === QUIC → RUNTIME UDP ===
    {
        let target_host = target.remote_host.clone();
        let target_port = target.remote_port;
        let datagram_tx = datagram_tx.clone();

        tokio::spawn(async move {
            let flows: Arc<Mutex<HashMap<SocketAddr, Flow>>> = Arc::new(Mutex::new(HashMap::new()));

            while let Some(d) = rx.recv().await {
                let (src_addr, off) = match decode_socket_addr(&d) {
                    Some(v) => v,
                    None => {
                        debug!("decode src failed");
                        continue;
                    }
                };
                let payload = &d[off..];

                // get/create flow
                let flow = {
                    let mut map = flows.lock().await;
                    if let Some(f) = map.get(&src_addr) {
                        f.clone()
                    } else {
                        let sock = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
                        sock.connect((target_host.as_str(), target_port))
                            .await
                            .unwrap();

                        let f = Flow {
                            sock: sock.clone(),
                            last: Arc::new(Mutex::new(Instant::now())),
                        };

                        // spawn reader for this flow
                        {
                            let flows = flows.clone();
                            let datagram_tx = datagram_tx.clone();
                            let chan_id = chan_id;
                            let src_addr = src_addr;
                            let sock = sock.clone();
                            let last = f.last.clone();

                            tokio::spawn(async move {
                                let mut buf = [0u8; MAX_PACKET];
                                loop {
                                    let recv = timeout(IDLE_TIMEOUT, sock.recv(&mut buf)).await;
                                    let n = match recv {
                                        Ok(Ok(n)) => n,
                                        Ok(Err(e)) => {
                                            warn!("recv error {}: {:?}", src_addr, e);
                                            break;
                                        }
                                        Err(_) => {
                                            debug!("idle timeout {}, closing", src_addr);
                                            break;
                                        }
                                    };

                                    {
                                        let mut t = last.lock().await;
                                        *t = Instant::now();
                                    }

                                    let mut out = BytesMut::with_capacity(1 + 18 + n);
                                    encode_socket_addr(&mut out, src_addr);
                                    out.extend_from_slice(&buf[..n]);

                                    if datagram_tx.send((chan_id, out.freeze())).await.is_err() {
                                        break;
                                    }
                                }

                                // cleanup flow
                                let mut map = flows.lock().await;
                                map.remove(&src_addr);
                            });
                        }

                        map.insert(src_addr, f.clone());
                        f
                    }
                };

                {
                    let mut t = flow.last.lock().await;
                    *t = Instant::now();
                }

                if let Err(e) = flow.sock.send(payload).await {
                    warn!("send error {}: {:?}", src_addr, e);
                    let mut map = flows.lock().await;
                    map.remove(&src_addr);
                }
            }
            {
                let mut map = flows.lock().await;
                map.clear(); // close all per-source flows
            }
            // rx ended → QUIC closed or stream router closed this UDP forward
            manager.remove(chan_id).await;
        });
    }

    info!(
        "Runtime UDP forward ready: channel {} → {}:{}",
        chan_id, target.remote_host, target.remote_port
    );
}

pub async fn socket_forward(target: SocketTarget, mut io: QuicIo) {
    let path = target.remote_path;

    let mut stream = match UnixStream::connect(&path).await {
        Ok(s) => s,
        Err(e) => {
            let _ = io
                .write_all(format!("UNIX connect failed ({}): {}\n", path, e).as_bytes())
                .await;
            return;
        }
    };

    tracing::info!("unix forward opened to {}", path);

    match tokio::io::copy_bidirectional(&mut io, &mut stream).await {
        Ok((rx, tx)) => {
            tracing::info!("unix forward {} closed cleanly (rx={rx}, tx={tx})", path);
        }
        Err(e) => {
            // “sending stopped by peer: error 0” is quinn signalling that the
            // remote side stopped the stream – it’s often a normal shutdown.
            let msg = e.to_string();
            let is_expected = msg.contains("sending stopped by peer")
                || matches!(
                    e.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::UnexpectedEof
                );

            if is_expected {
                tracing::info!("unix forward {} closed (expected close: {})", path, msg);
            } else {
                tracing::error!("unix forward {} closed with error: {}", path, msg);
            }
        }
    }
}
