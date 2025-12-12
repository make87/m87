use std::path::PathBuf;

use russh::server;

use crate::{
    streams::quic::QuicIo,
    util::ssh::{M87SshHandler, make_server_config},
};

pub async fn handle_ssh_io(io: QuicIo) {
    let config = make_server_config();
    let handler = M87SshHandler::new(PathBuf::from("/"));

    match server::run_stream(config, io, handler).await {
        Ok(running) => {
            tracing::info!("SSH handshake complete, session running");
            // Second stage: lifetime of the connection
            if let Err(e) = running.await {
                tracing::error!("SSH connection closed: {:?}", e);
            }
            tracing::debug!("SSH session ended normally");
        }
        Err(e) => {
            tracing::error!("SSH handshake aborted: {:?}", e);
        }
    }
}
