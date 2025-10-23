use anyhow::{anyhow, Result};
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use warp::ws::{Message, WebSocket};

use crate::config::Config;

#[derive(Debug, Serialize, Deserialize)]
pub struct Auth0Claims {
    pub sub: String,
    pub exp: usize,
    pub iat: usize,
    pub iss: String,
    pub aud: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    kid: String,
    //kty: String,
    //alg: String,
    n: String,
    e: String,
}

pub async fn validate_token(token: &str) -> Result<Auth0Claims> {
    let config = Config::load()?;

    let jwks_url = format!("{}.well-known/jwks.json", config.auth_domain);

    let header =
        decode_header(token).map_err(|e| anyhow!("Failed to decode token header: {}", e))?;
    let kid = header
        .kid
        .ok_or_else(|| anyhow!("Token missing 'kid' field"))?;

    let jwks: Jwks = Client::new()
        .get(&jwks_url)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to fetch JWKS: {}", e))?
        .json()
        .await
        .map_err(|e| anyhow!("Failed to parse JWKS: {}", e))?;

    let key = jwks
        .keys
        .iter()
        .find(|k| k.kid == kid)
        .ok_or_else(|| anyhow!("No matching JWK found"))?;

    let decoding_key = DecodingKey::from_rsa_components(&key.n, &key.e)
        .map_err(|_| anyhow!("Failed to create decoding key"))?;

    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&[config.auth_audience]);
    validation.set_issuer(&[config.auth_domain]);

    let decoded = decode::<Auth0Claims>(token, &decoding_key, &validation)
        .map_err(|e| anyhow!("Token verification failed: {}", e))?;

    Ok(decoded.claims)
}

/// Expect first WS text message to contain the token (optionally prefixed with 'Bearer ')
pub async fn validate_token_via_ws(
    ws_tx: &mut futures::stream::SplitSink<WebSocket, Message>,
    ws_rx: &mut futures::stream::SplitStream<WebSocket>,
    send_updates: bool,
) -> Result<()> {
    if send_updates {
        let _ = ws_tx.send(Message::text("Authenticating...\n\r")).await;
    }

    let token_msg = ws_rx.next().await.and_then(|m| m.ok()).and_then(|m| {
        if m.is_text() {
            Some(m.to_str().unwrap_or("").to_string())
        } else {
            None
        }
    });

    let token = match token_msg {
        Some(t) => t.trim_start_matches("Bearer ").to_string(),
        None => return Err(anyhow!("No protocol provided")),
    };

    validate_token(&token).await?;

    if send_updates {
        let _ = ws_tx
            .send(Message::text("Connected successfully\n\r"))
            .await;
    }
    Ok(())
}
