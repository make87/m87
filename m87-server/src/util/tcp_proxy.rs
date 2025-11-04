use socket2::Socket;
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::{io::copy_bidirectional, net::TcpStream};
use tracing::{info, warn};

use crate::response::{ServerError, ServerResult};

/// Bidirectional TCP proxy between inbound and reverse sockets.
/// Adds TCP_NODELAY and keepalive for stability, and performs full cleanup on exit.
pub async fn proxy_bidirectional_tcp(inbound: TcpStream, reverse: TcpStream) -> ServerResult<()> {
    let socket = Socket::from(inbound.into_std()?);
    let _ = socket.set_tcp_nodelay(true);
    let _ = socket.set_keepalive(true);
    let mut t_inbound = TcpStream::from_std(socket.into())?;

    let socket = Socket::from(reverse.into_std()?);
    let _ = socket.set_tcp_nodelay(true);
    let _ = socket.set_keepalive(true);
    let mut t_reverse = TcpStream::from_std(socket.into())?;

    match copy_bidirectional(&mut t_inbound, &mut t_reverse).await {
        Ok((from_client, from_node)) => {
            info!(from_client, from_node, "proxy session closed cleanly");
            t_inbound.shutdown().await.ok();
            t_reverse.shutdown().await.ok();
            Ok(())
        }
        Err(e) => {
            warn!(error=%e, "proxy error, shutting down sockets");
            t_inbound.shutdown().await.ok();
            t_reverse.shutdown().await.ok();
            Err(ServerError::internal_error(&format!("{}", e)))
        }
    }
}

pub async fn proxy_bidirectional<A, B>(a: &mut A, b: &mut B) -> io::Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    match io::copy_bidirectional(a, b).await {
        Ok((_a, _b)) => {
            info!("proxy session closed cleanly ");
            let _ = a.shutdown().await;
            let _ = b.shutdown().await;
            Ok(())
        }
        Err(e) => {
            info!("proxy session closed with error ");
            let _ = a.shutdown().await;
            let _ = b.shutdown().await;
            Err(e)
        }
    }
}
