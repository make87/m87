use anyhow::{Result, Context, bail};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, debug, error};

pub struct WebSocketClient {
    url: String,
    write: Option<futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>>,
    read: Option<futures_util::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>,
}

impl WebSocketClient {
    pub fn new(url: &str) -> Result<Self> {
        Ok(Self {
            url: url.to_string(),
            write: None,
            read: None,
        })
    }
    
    pub async fn connect(&mut self) -> Result<()> {
        info!("Connecting to WebSocket: {}", self.url);
        
        let (ws_stream, _) = connect_async(&self.url)
            .await
            .context("Failed to connect to WebSocket")?;
        
        let (write, read) = ws_stream.split();
        self.write = Some(write);
        self.read = Some(read);
        
        info!("WebSocket connected successfully");
        Ok(())
    }
    
    #[allow(dead_code)]
    pub async fn send_message(&mut self, message: &str) -> Result<()> {
        let write = self.write.as_mut()
            .context("WebSocket not connected")?;
        
        debug!("Sending message: {}", message);
        write.send(Message::Text(message.to_string()))
            .await
            .context("Failed to send message")?;
        
        Ok(())
    }
    
    pub async fn receive_message(&mut self) -> Result<String> {
        let read = self.read.as_mut()
            .context("WebSocket not connected")?;
        
        match read.next().await {
            Some(Ok(msg)) => {
                match msg {
                    Message::Text(text) => {
                        debug!("Received text message: {}", text);
                        Ok(text)
                    }
                    Message::Binary(data) => {
                        debug!("Received binary message: {} bytes", data.len());
                        Ok(String::from_utf8_lossy(&data).to_string())
                    }
                    Message::Ping(_) => {
                        debug!("Received ping");
                        // Pong is handled automatically by tungstenite
                        Box::pin(self.receive_message()).await
                    }
                    Message::Pong(_) => {
                        debug!("Received pong");
                        Box::pin(self.receive_message()).await
                    }
                    Message::Close(_) => {
                        info!("Received close message");
                        bail!("WebSocket connection closed by server")
                    }
                    Message::Frame(_) => {
                        // Raw frames are not expected in normal operation
                        Box::pin(self.receive_message()).await
                    }
                }
            }
            Some(Err(e)) => {
                error!("Error receiving message: {}", e);
                Err(e).context("Failed to receive message")
            }
            None => {
                info!("WebSocket stream ended");
                bail!("WebSocket connection closed")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_websocket_client_new() {
        let client = WebSocketClient::new("wss://echo.websocket.org");
        assert!(client.is_ok());
    }
}
