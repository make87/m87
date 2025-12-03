use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use axum::{
    body::Body,
    extract::FromRequestParts,
    http::{HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use bytes::BytesMut;
use futures::{Sink, Stream};
use futures_util::SinkExt;
use hyper::{header, upgrade};
use hyper_util::rt::TokioIo;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::{
    tungstenite::{self, Message},
    WebSocketStream,
};

use crate::rest::auth::validate_token;

// -----------------------------------------------------------------------------
// Common IO abstraction
// -----------------------------------------------------------------------------

pub trait IoStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> IoStream for T {}

pub type BoxedIo = Pin<Box<dyn IoStream>>;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn unauthorized(msg: &'static str) -> Response {
    (StatusCode::UNAUTHORIZED, msg).into_response()
}

fn bad_request(msg: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, msg).into_response()
}

/// Extract `bearer.<jwt>` from `Sec-WebSocket-Protocol` and validate via `validate_token`.
///
/// This is used for both WS and RAW upgrades. Adjust if you want Authorization: Bearer instead.
async fn extract_and_validate_jwt(headers: &HeaderMap) -> Result<String, Response> {
    let proto = headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|h| h.to_str().ok());

    let jwt = match proto {
        Some(p) if p.starts_with("bearer.") => p.trim_start_matches("bearer.").to_string(),
        _ => {
            return Err(unauthorized(
                "Missing or invalid WebSocket protocol (bearer.<jwt>)",
            ))
        }
    };

    if validate_token(&jwt).await.is_err() {
        return Err(unauthorized("Invalid auth token"));
    }

    Ok(jwt)
}

/// Compute `Sec-WebSocket-Accept` header from `Sec-WebSocket-Key`.
fn websocket_accept(key: &str) -> String {
    const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    let mut sha1 = Sha1::new();
    sha1.update(key.as_bytes());
    sha1.update(GUID.as_bytes());
    let digest = sha1.finalize();
    BASE64.encode(digest)
}

// -----------------------------------------------------------------------------
// WebSocket <-> AsyncRead/AsyncWrite adapter
// -----------------------------------------------------------------------------

/// Adapter that exposes a WebSocketStream as a plain byte stream.
///
/// - Reads: concatenates all binary frames into a continuous byte stream (text frames ignored).
/// - Writes: each poll_write becomes a single binary frame.
pub struct WsIo<S> {
    ws: WebSocketStream<S>,
    read_buf: BytesMut,
    closed: bool,
}

impl<S> WsIo<S> {
    pub fn new(ws: WebSocketStream<S>) -> Self {
        Self {
            ws,
            read_buf: BytesMut::new(),
            closed: false,
        }
    }
}

impl<S> AsyncRead for WsIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            if !self.read_buf.is_empty() {
                let to_copy = std::cmp::min(self.read_buf.len(), buf.remaining());
                let chunk = self.read_buf.split_to(to_copy);
                buf.put_slice(&chunk);
                return Poll::Ready(Ok(()));
            }

            if self.closed {
                return Poll::Ready(Ok(()));
            }

            match futures_util::ready!(Pin::new(&mut self.ws).poll_next(cx)) {
                Some(Ok(msg)) => {
                    let data = msg.into_data(); // TEXT → bytes, BINARY → bytes
                    if !data.is_empty() {
                        self.read_buf.extend_from_slice(&data);
                    }
                }
                Some(Err(e)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("websocket read error: {e}"),
                    )));
                }
                None => {
                    self.closed = true;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl<S> AsyncWrite for WsIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.closed {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "websocket closed",
            )));
        }

        // Ensure sink is ready
        futures_util::ready!(Pin::new(&mut self.ws).poll_ready(cx)).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("poll_ready: {e}"))
        })?;

        // Send as binary frame
        self.ws
            .start_send_unpin(Message::binary(data.to_vec()))
            .map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("ws send error: {e}"))
            })?;

        futures_util::ready!(Pin::new(&mut self.ws).poll_flush(cx)).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("ws flush: {e}"))
        })?;

        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match Pin::new(&mut self.ws).poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.closed = true;

        match Pin::new(&mut self.ws).poll_close(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))),
        }
    }
}

pub fn io_upgrade<Args, Fut>(
    handler: fn(Args, BoxedIo) -> Fut,
) -> impl Fn(Request<Body>) -> Pin<Box<dyn Future<Output = Response> + Send>> + Clone + Send + 'static
where
    Args: FromRequestParts<()> + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    move |req: Request<Body>| {
        Box::pin(async move {
            // 1) Split into parts + body for extractors
            let (mut parts, body) = req.into_parts();

            let args = match Args::from_request_parts(&mut parts, &()).await {
                Ok(v) => v,
                Err(_e) => {
                    tracing::warn!("param extraction failed");
                    return (StatusCode::BAD_REQUEST, "invalid parameters").into_response();
                }
            };

            // Rebuild Request for hyper upgrade
            let mut req = Request::from_parts(parts, body);

            // 2) Auth via Sec-WebSocket-Protocol: bearer.<jwt>
            let headers_clone = req.headers().clone();
            let jwt = match extract_and_validate_jwt(&headers_clone).await {
                Ok(jwt) => jwt,
                Err(resp) => return resp, // 401 already built
            };

            // 3) Decide transport by Upgrade header
            let upgrade_val = req
                .headers()
                .get(header::UPGRADE)
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .to_ascii_lowercase();

            let on_upgrade = upgrade::on(&mut req);
            let handler_fn = handler;

            if upgrade_val.contains("raw") {
                // ------------ RAW path ------------
                tokio::spawn(async move {
                    match on_upgrade.await {
                        Ok(upgraded) => {
                            let io = Box::pin(TokioIo::new(upgraded)) as BoxedIo;
                            handler_fn(args, io).await;
                        }
                        Err(e) => tracing::error!("raw upgrade failed: {e:?}"),
                    }
                });

                return Response::builder()
                    .status(StatusCode::SWITCHING_PROTOCOLS)
                    .header(header::CONNECTION, "upgrade")
                    .header(header::UPGRADE, "raw")
                    // optional, but harmless to echo protocol here too:
                    .header(header::SEC_WEBSOCKET_PROTOCOL, format!("bearer.{jwt}"))
                    .body(Body::empty())
                    .unwrap()
                    .into_response();
            }

            if upgrade_val.contains("websocket") {
                // ------------ WS path ------------
                let key = match req.headers().get(header::SEC_WEBSOCKET_KEY) {
                    Some(v) => match v.to_str() {
                        Ok(s) => s.to_string(),
                        Err(_) => {
                            return (StatusCode::BAD_REQUEST, "invalid Sec-WebSocket-Key")
                                .into_response();
                        }
                    },
                    None => {
                        return (StatusCode::BAD_REQUEST, "missing Sec-WebSocket-Key")
                            .into_response();
                    }
                };

                let accept = websocket_accept(&key);
                let proto_hdr = format!("bearer.{jwt}");

                tokio::spawn(async move {
                    match on_upgrade.await {
                        Ok(upgraded) => {
                            let io = TokioIo::new(upgraded);
                            let ws = WebSocketStream::from_raw_socket(
                                io,
                                tungstenite::protocol::Role::Server,
                                None,
                            )
                            .await;

                            let ws_io = Box::pin(WsIo::new(ws)) as BoxedIo;
                            handler_fn(args, ws_io).await;
                        }
                        Err(e) => tracing::error!("ws upgrade failed: {e:?}"),
                    }
                });

                return Response::builder()
                    .status(StatusCode::SWITCHING_PROTOCOLS)
                    .header(header::CONNECTION, "upgrade")
                    .header(header::UPGRADE, "websocket")
                    .header(header::SEC_WEBSOCKET_ACCEPT, accept)
                    .header(header::SEC_WEBSOCKET_PROTOCOL, proto_hdr)
                    .body(Body::empty())
                    .unwrap()
                    .into_response();
            }

            (StatusCode::BAD_REQUEST, "Upgrade must be websocket or raw").into_response()
        })
    }
}
