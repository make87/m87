use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::response::{NexusError, NexusResult};

type HmacSha256 = Hmac<Sha256>;

pub fn verify_tunnel_token(token_b64: &str, secret: &str) -> NexusResult<String> {
    let decoded = URL_SAFE_NO_PAD.decode(token_b64)?;
    let decoded_str = String::from_utf8(decoded)?;
    let parts: Vec<&str> = decoded_str.split('|').collect();
    if parts.len() != 3 {
        return Err(NexusError::invalid_token("invalid token format"));
    }

    let node_id = parts[0];
    let expiry: i64 = parts[1]
        .parse()
        .map_err(|_| NexusError::invalid_token("invalid expiry"))?;
    let sig_hex = parts[2];

    if Utc::now().timestamp() > expiry {
        return Err(NexusError::invalid_token("token expired"));
    }

    let payload = format!("{}|{}", node_id, expiry);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| NexusError::invalid_token("invalid secret"))?;
    mac.update(payload.as_bytes());
    mac.verify_slice(&hex::decode(sig_hex)?)?;
    Ok(node_id.to_string())
}

pub fn issue_tunnel_token(node_id: &str, ttl_secs: u64, secret: &str) -> NexusResult<String> {
    type HmacSha256 = Hmac<Sha256>;

    // Compute expiry timestamp (UTC seconds)
    let expiry = Utc::now().timestamp() + ttl_secs as i64;

    // Payload = node_id|expiry
    let payload = format!("{}|{}", node_id, expiry);

    // Compute HMAC signature
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| NexusError::internal_error("Invalid HMAC secret"))?;
    mac.update(payload.as_bytes());
    let signature = mac.finalize().into_bytes();

    // Final token (base64url encoded, no padding)
    let token_raw = format!("{}|{}", payload, hex::encode(signature));
    Ok(URL_SAFE_NO_PAD.encode(token_raw))
}
