use russh::server;

use crate::{
    streams::quic::QuicIo,
    util::ssh::{M87SshHandler, make_server_config},
};

pub async fn handle_ssh_io(io: QuicIo) {
    let home_dir = dirs::home_dir().unwrap();
    let home_dir = home_dir.to_path_buf();

    let config = make_server_config();
    let handler = M87SshHandler::new(home_dir);

    match server::run_stream(config, io, handler).await {
        Ok(running) => {
            // Second stage: lifetime of the connection
            if let Err(e) = running.await {
                tracing::error!("SSH connection closed: {:?}", e);
            }
        }
        Err(e) => {
            tracing::error!("SSH handshake aborted: {:?}", e);
        }
    }
}
