use axum::extract::{Path, Query};
use tokio::{
    io::{self, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{error, info};

use crate::rest::upgrade::BoxedIo;


#[derive(Debug, serde::Deserialize)]
pub struct HostQuery {
    pub host: Option<String>,
}

pub async fn handle_port_forward_io(
    (Path(port), Query(HostQuery { host })): (Path<String>, Query<HostQuery>),
    mut io: BoxedIo,
) {

    let port: u16 = match port.parse() {
        Ok(p) => p,
        Err(e) => {
            error!("Invalid port '{port}': {e}");
            return;
        }
    };
    let host = host.unwrap_or_else(|| "127.0.0.1".to_string());

    match TcpStream::connect((host, port)).await {
        Ok(mut local) => match io::copy_bidirectional(&mut io, &mut local).await {
            Ok((a, b)) => info!("port forward closed cleanly (remote→local={a}, local→remote={b})"),
            Err(e) => error!("port forward error: {e}"),
        },
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to connect to localhost:{port}: {e}\n").as_bytes())
                .await;
        }
    }
}
