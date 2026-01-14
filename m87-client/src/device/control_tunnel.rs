#[cfg(feature = "runtime")]
use std::sync::Arc;

#[cfg(feature = "runtime")]
use anyhow::Context;
use anyhow::Result;

use serde::{Serialize, de::DeserializeOwned};
use tracing::error;
#[cfg(feature = "runtime")]
use tracing::{debug, warn};

#[cfg(feature = "runtime")]
use crate::{auth::AuthManager, config::Config, device::deployment_manager::DeploymentManager};

#[cfg(feature = "runtime")]
pub use m87_shared::heartbeat::{HeartbeatRequest, HeartbeatResponse};

use crate::util::system_info::get_system_info;

pub struct HeartbeatState {
    last_instruction_hash: String,
    heartbeat_interval: u64,
    first_heartbeat: bool,
}

// Runtime-specific: Maintain persistent control tunnel connection
#[cfg(feature = "runtime")]
pub async fn connect_control_tunnel(unit_manager: Arc<DeploymentManager>) -> Result<()> {
    use std::sync::Arc;

    use crate::streams::quic::get_quic_connection;
    use crate::streams::udp_manager::UdpChannelManager;
    use bytes::{BufMut, Bytes, BytesMut};
    use m87_shared::{
        config::DeviceClientConfig, deploy_spec::build_instruction_hash, device::short_device_id,
    };
    use quinn::Connection;
    use tokio::sync::watch;

    let config = Config::load().context("Failed to load configuration")?;
    let token = AuthManager::get_device_token()?;
    let short_id = short_device_id(&config.device_id);

    let control_host = format!(
        "control-{}.{}",
        short_id,
        config.get_runtime_server_hostname()
    );
    debug!("Connecting QUIC control tunnel to {}", control_host);

    let (_endpoint, quic_conn): (_, Connection) =
        get_quic_connection(&control_host, &token, config.trust_invalid_server_cert)
            .await
            .map_err(|e| {
                error!("QUIC connect failed: {}", e);
                e
            })
            .context("QUIC connect failed")?;

    //  SHUTDOWN SIGNAL
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // thread to send periodoic health reports to server

    let mut shutdown = shutdown_rx.clone();
    let (mut send, mut recv) = quic_conn.open_bi().await?;
    send.write_all(&[0x01]).await?; // send to make sure the server does not timeout waiting
    // let mut recv = quic_conn.accept_uni().await?;
    //
    let current_deploy_hash = DeploymentManager::get_current_deploy_hash();
    let config_hash = DeviceClientConfig {
        heartbeat_interval_secs: Some(config.heartbeat_interval_secs as u32),
    }
    .get_hash()
    .to_string();
    let last_deploy_hash = build_instruction_hash(&current_deploy_hash, &config_hash);

    let state = Arc::new(tokio::sync::Mutex::new(HeartbeatState {
        last_instruction_hash: last_deploy_hash,
        heartbeat_interval: config.heartbeat_interval_secs,
        first_heartbeat: true,
    }));

    let manager_clone = unit_manager.clone();
    let _receiver = tokio::spawn({
        let state = state.clone();
        let update_mutex = Arc::new(tokio::sync::Mutex::new(()));
        async move {
            loop {
                tokio::select! {
                    _ = shutdown.changed() => break,

                    msg = read_msg::<HeartbeatResponse>(&mut recv) => {
                        let _ = update_mutex.lock().await;
                        let resp = msg?;
                        tracing::info!("Received heartbeat response");

                        let mut st = state.lock().await;

                        if let Some(cfg) = resp.config {
                            tracing::info!("Received new config");
                            let mut new_cfg = Config::load()?;
                            if let Some(new) = cfg.heartbeat_interval_secs {
                                st.heartbeat_interval = new as u64;
                                new_cfg.heartbeat_interval_secs = new as u64;
                            }
                            new_cfg.save()?;
                        }
                        if let Some(target_units_config) = resp.target_revision {
                            tracing::info!("Received new target deployment");
                            let res = manager_clone.set_desired_units(target_units_config).await;
                            if let Err(e) = res {
                                tracing::error!("Failed to set target deployment: {}", e);
                                continue;
                            }
                        }

                        st.last_instruction_hash = resp.instruction_hash;
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        }
    });

    let mut shutdown = shutdown_rx.clone();

    let _sender = tokio::spawn({
        let state = state.clone();
        async move {
            loop {
                use std::time::Duration;

                use crate::device::deployment_manager::{ack_event, on_new_event};

                tokio::select! {
                    _ = shutdown.changed() => break,
                        // handle envent rx

                    data = on_new_event() => {
                        let Some(claimed) = data else { continue };
                        let st = state.lock().await;

                        let req = HeartbeatRequest {
                            last_instruction_hash: st.last_instruction_hash.clone(),
                            deploy_report: Some(claimed.report.clone()),
                            ..Default::default()
                        };

                        tracing::info!("Sending heartbeat with event udpate");

                        if write_msg(&mut send, &req).await.is_ok() {
                            let _ = ack_event(&claimed).await;
                        }
                    },


                    _ = async {
                        let (req, interval) = {
                            let mut st = state.lock().await;

                            let mut req = HeartbeatRequest {
                                last_instruction_hash: st.last_instruction_hash.clone(),
                                ..Default::default()
                            };

                            if st.first_heartbeat {
                                st.first_heartbeat = false;
                                req.client_version = Some(env!("CARGO_PKG_VERSION").to_string());
                                req.system_info = Some(get_system_info().await?);
                            }

                            (req, st.heartbeat_interval)
                        };

                        tracing::info!("Sending heartbeat request");

                        write_msg(&mut send, &req).await?;
                        tokio::time::sleep(Duration::from_secs(interval)).await;
                        Ok::<_, anyhow::Error>(())
                    } => {}
                }
            }
        }
    });

    let udp_channels = UdpChannelManager::new();

    let (datagram_tx, mut datagram_rx) = tokio::sync::mpsc::channel::<(u32, Bytes)>(2048);

    // This task frames datagrams and sends via QUIC
    {
        let conn = quic_conn.clone();
        let mut shutdown = shutdown_rx.clone();
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.changed() => break,

                    Some((id, payload)) = datagram_rx.recv() => {
                        let mut buf = BytesMut::with_capacity(4 + payload.len());
                        buf.put_u32(id);
                        buf.extend_from_slice(&payload);

                        if conn.send_datagram(buf.freeze()).is_err() {
                            warn!("send_datagram failed — shutting down");
                            let _ = shutdown_tx.send(true);
                            break;
                        }
                    }

                    else => break,
                }
            }
        });
    }

    //  DATAGRAM INPUT PIPE (QUIC → workers)
    {
        let udp_channels_clone = udp_channels.clone();
        let conn = quic_conn.clone();
        let mut shutdown = shutdown_rx.clone();
        let shutdown_tx = shutdown_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.changed() => break,

                    res = conn.read_datagram() => {
                        let d = match res {
                            Ok(d) => d,
                            Err(_) => {
                                let _ = shutdown_tx.send(true);
                                break;
                            }
                        };

                        if d.len() < 4 {
                            continue;
                        }

                        let id = u32::from_be_bytes([d[0], d[1], d[2], d[3]]);
                        let payload = Bytes::copy_from_slice(&d[4..]);

                        if let Some(ch) = udp_channels_clone.get(id).await {
                            let _ = ch.sender.try_send(payload);
                        }
                    }
                }
            }

            udp_channels_clone.remove_all().await;
        });
    }

    let mut shutdown = shutdown_rx.clone();
    //  CONTROL STREAM ACCEPT LOOP
    loop {
        use crate::streams::{self, quic::QuicIo};

        tokio::select! {

            _ = shutdown.changed() => {
                warn!("control tunnel shutting down");
                break;
            }
            incoming = quic_conn.accept_bi() => {
                match incoming {
                    Ok((send, recv)) => {
                        debug!("QUIC: new control stream accepted");

                        let io = QuicIo { recv, send };
                        let udp_channels_clone = udp_channels.clone();
                        let datagram_tx_clone = datagram_tx.clone();
                        let unit_manager_clone = unit_manager.clone();

                        tokio::spawn(async move {
                            if let Err(e) =
                                streams::router::handle_incoming_stream(
                                    io, udp_channels_clone, datagram_tx_clone, unit_manager_clone
                                ).await
                            {
                                warn!("control stream error: {:?}", e);
                            }
                        });
                    }

                    Err(e) => {
                        warn!("Control accept failed: {:?}", e);
                        let _ = shutdown_tx.send(true);
                        break;
                    }
                }
            }

            // QUIC connection closed
            res = quic_conn.closed() => {
                warn!("control tunnel closed by peer {:?}", res);
                udp_channels.remove_all().await;
                break;
            }
        }
    }

    let _ = shutdown_tx.send(true);
    udp_channels.remove_all().await;
    debug!("control tunnel terminated");
    Ok(())
}

pub async fn write_msg<T: Serialize>(io: &mut quinn::SendStream, msg: &T) -> Result<()> {
    let json = serde_json::to_vec(&msg)?;
    let len = (json.len() as u32).to_be_bytes();

    io.write_all(&len).await?;
    io.write_all(&json).await?;
    Ok(())
}

pub async fn read_msg<T: DeserializeOwned>(io: &mut quinn::RecvStream) -> Result<T> {
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // json body
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;

    // deserialize directly into enum
    let msg: T = serde_json::from_slice::<T>(&buf)?;

    Ok(msg)
}
