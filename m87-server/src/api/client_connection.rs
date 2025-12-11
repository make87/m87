use bytes::Bytes;
use h3::quic::BidiStream;
use h3_webtransport::server::AcceptedBi;
use h3_webtransport::server::WebTransportSession;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{self, AsyncRead, AsyncWrite};

use crate::response::ServerError;
use crate::response::ServerResult;

#[derive(Clone)]
pub struct WebConn {
    pub session: Arc<WebTransportSession<h3_quinn::Connection, Bytes>>,
    pub quinn_conn: quinn::Connection,
}

impl WebConn {
    pub fn new(
        session: Arc<WebTransportSession<h3_quinn::Connection, Bytes>>,
        quinn_conn: quinn::Connection,
    ) -> Self {
        WebConn {
            session,
            quinn_conn,
        }
    }
}

#[derive(Clone)]
pub enum ClientConn {
    Raw(quinn::Connection),
    Web(WebConn),
}

impl ClientConn {
    pub async fn accept_bi(
        &self,
    ) -> ServerResult<(
        Pin<Box<dyn AsyncWrite + Send>>,
        Pin<Box<dyn AsyncRead + Send>>,
    )> {
        match self {
            ClientConn::Raw(conn) => {
                let (send, recv) = conn.accept_bi().await?;
                Ok((Box::pin(send), Box::pin(recv)))
            }
            ClientConn::Web(web) => {
                let (send, recv) = match web.session.accept_bi().await {
                    Ok(Some(AcceptedBi::BidiStream(_, stream))) => BidiStream::split(stream),
                    _ => return Err(ServerError::bad_request("unexpected stream type")),
                };
                Ok((Box::pin(send), Box::pin(recv)))
            }
        }
    }

    pub fn send_datagram(&self, data: Bytes) -> io::Result<()> {
        match self {
            ClientConn::Raw(conn) => conn
                .send_datagram(data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
            ClientConn::Web(web) => web
                .session
                .datagram_sender()
                .send_datagram(data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    pub async fn read_datagram(&self) -> io::Result<Bytes> {
        match self {
            ClientConn::Raw(conn) => conn
                .read_datagram()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
            ClientConn::Web(web) => web
                .session
                .datagram_reader()
                .read_datagram()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                .map(|d| d.into_payload()),
        }
    }

    pub async fn closed(&self) {
        match self {
            ClientConn::Raw(conn) => {
                let _ = conn.closed().await;
            }
            ClientConn::Web(web) => {
                // adjust if the actual API name differs
                let _ = web.quinn_conn.closed().await;
            }
        }
    }
}
