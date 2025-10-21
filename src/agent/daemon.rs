use anyhow::{Result, Context};
use tracing::{info, warn, error, debug};
use tokio::time::{sleep, Duration};

use crate::backend::WebSocketClient;
use crate::config::Config;

pub async fn run_foreground() -> Result<()> {
    info!("Running agent in foreground mode");
    
    let config = Config::load().context("Failed to load configuration")?;
    
    let mut ws_client = WebSocketClient::new(&config.backend_url)?;
    
    loop {
        match ws_client.connect().await {
            Ok(_) => {
                info!("Connected to make87 backend");
                
                if let Err(e) = handle_messages(&mut ws_client).await {
                    error!("Error handling messages: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to connect to backend: {}", e);
            }
        }
        
        warn!("Connection lost, reconnecting in 5 seconds...");
        sleep(Duration::from_secs(5)).await;
    }
}

pub async fn run_background() -> Result<()> {
    info!("Running agent in background mode");
    
    // For background mode, we would typically daemonize the process
    // For now, we'll just run in the same way as foreground
    // In a production implementation, this would use proper daemonization
    run_foreground().await
}

async fn handle_messages(ws_client: &mut WebSocketClient) -> Result<()> {
    loop {
        match ws_client.receive_message().await {
            Ok(message) => {
                debug!("Received message: {:?}", message);
                process_message(message).await?;
            }
            Err(e) => {
                error!("Error receiving message: {}", e);
                return Err(e);
            }
        }
    }
}

async fn process_message(message: String) -> Result<()> {
    // Process different types of messages from the backend
    // This is a placeholder for actual message processing logic
    info!("Processing message: {}", message);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_process_message() {
        let result = process_message("test message".to_string()).await;
        assert!(result.is_ok());
    }
}
