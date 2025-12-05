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
    let stream_type = StreamType::from_incoming_stream(&mut io.recv).await?;
    let token = stream_type.get_token();
    let _ = validate_token(token).await?;
    match stream_type {
        StreamType::Terminal { .. } => {
            handle_terminal_io(&mut io).await;
        }
        StreamType::Exec { .. } => {
            handle_exec_io(io).await;
        }
        StreamType::Logs { .. } => {
            handle_logs_io(&mut io).await;
        }
        StreamType::Port {
            port,
            host,
            addition,
            protocol,
            ..
        } => {
            handle_port_forward_io(port, host, protocol, addition, &mut io).await;
        }
        StreamType::Serial { name, baud, .. } => {
            handle_serial_io(name, baud, &mut io).await;
        }
        StreamType::Metrics { .. } => {
            handle_system_metrics_io(&mut io).await;
        }
        StreamType::Docker { .. } => {
            handle_docker_io(&mut io).await;
        }
        StreamType::Ssh { .. } => {
            handle_ssh_io(io).await;
        }
    }
    Ok(())
}
