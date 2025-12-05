use tracing::{debug, warn};

use crate::streams::auth::validate_token;
use crate::streams::quic::QuicIo;
use crate::streams::serial::handle_serial_io;
use crate::streams::stream_type::StreamType;
use crate::streams::{
    docker::handle_docker_io, exec::handle_exec_io, logs::handle_logs_io,
    metrics::handle_system_metrics_io, port::handle_port_forward_io, ssh::handle_ssh_io,
    terminal::handle_terminal_io,
};

pub async fn handle_incoming_stream(mut io: QuicIo) -> anyhow::Result<()> {
    debug!("router: parsing stream type header");
    let stream_type = match StreamType::from_incoming_stream(&mut io.recv).await {
        Ok(st) => st,
        Err(e) => {
            warn!("router: failed to parse stream type: {e:?}");
            return Err(e);
        }
    };

    debug!("router: stream type = {:?}", stream_type.variant_name());

    let token = stream_type.get_token();
    if let Err(e) = validate_token(token).await {
        warn!("router: token validation failed: {e:?}");
        return Err(e);
    }

    match stream_type {
        StreamType::Terminal { .. } => {
            debug!("router: dispatching to terminal handler");
            handle_terminal_io(&mut io).await;
        }
        StreamType::Exec { .. } => {
            debug!("router: dispatching to exec handler");
            handle_exec_io(io).await;
        }
        StreamType::Logs { .. } => {
            debug!("router: dispatching to logs handler");
            handle_logs_io(&mut io).await;
        }
        StreamType::Port {
            port,
            host,
            addition,
            protocol,
            ..
        } => {
            debug!("router: dispatching to port forward handler");
            handle_port_forward_io(port, host, protocol, addition, &mut io).await;
        }
        StreamType::Serial { name, baud, .. } => {
            debug!("router: dispatching to serial handler");
            handle_serial_io(name, baud, &mut io).await;
        }
        StreamType::Metrics { .. } => {
            debug!("router: dispatching to metrics handler");
            handle_system_metrics_io(&mut io).await;
        }
        StreamType::Docker { .. } => {
            debug!("router: dispatching to docker handler");
            handle_docker_io(&mut io).await;
        }
        StreamType::Ssh { .. } => {
            debug!("router: dispatching to ssh handler");
            handle_ssh_io(io).await;
        }
    }
    debug!("router: handler finished");
    Ok(())
}
