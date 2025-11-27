use tokio::io::{copy_bidirectional, AsyncWriteExt};
use tokio::net::UnixStream;
use tracing::{error, info};

use crate::rest::upgrade::BoxedIo;

pub async fn handle_docker_io(_: (), mut io: BoxedIo) {
    // Connect to the Docker Unix socket
    let mut docker = match UnixStream::connect("/var/run/docker.sock").await {
        Ok(sock) => sock,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to connect to Docker: {e}\n").as_bytes())
                .await;
            error!("Failed to connect to Docker socket: {}", e);
            return;
        }
    };

    info!("Docker I/O proxy established");

    match copy_bidirectional(&mut io, &mut docker).await {
        Ok((a, b)) => {
            info!("copy_bidirectional finished: io→docker={a} bytes, docker→io={b} bytes");
        }
        Err(e) => {
            error!("I/O proxy error: {e}");
        }
    }

    info!("Docker I/O proxy closed");
}
