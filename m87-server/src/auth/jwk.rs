use std::sync::Arc;

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{
    config::AppConfig,
    response::{ServerError, ServerResult},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DecodedClaims {
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

pub async fn validate_token(token: &str, config: &Arc<AppConfig>) -> ServerResult<DecodedClaims> {
    let issuer = config.oauth.issuer.to_string();
    // add trailing / if missing
    let issuer = match issuer.ends_with('/') {
        true => issuer,
        false => format!("{}/", issuer),
    };
    let jwks_url = format!("{}.well-known/jwks.json", issuer);

    let header = decode_header(token).map_err(|e| {
        ServerError::invalid_token(&format!("Failed to decode token header: {}", e))
    })?;
    let kid = header
        .kid
        .ok_or_else(|| ServerError::invalid_token("Token missing 'kid' field"))?;

    let jwks: Jwks = Client::new()
        .get(&jwks_url)
        .send()
        .await
        .map_err(|e| ServerError::internal_error(&format!("Failed to fetch JWKS: {}", e)))?
        .json()
        .await
        .map_err(|e| ServerError::internal_error(&format!("Failed to parse JWKS: {}", e)))?;

    let key = jwks
        .keys
        .iter()
        .find(|k| k.kid == kid)
        .ok_or_else(|| ServerError::invalid_token("No matching JWK found"))?;

    let decoding_key = DecodingKey::from_rsa_components(&key.n, &key.e)
        .map_err(|_| ServerError::invalid_token("Failed to create decoding key"))?;

    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&[config.oauth.audience.to_string()]);
    validation.set_issuer(&[config.oauth.issuer.to_string()]);

    let decoded = decode::<DecodedClaims>(token, &decoding_key, &validation)
        .map_err(|e| ServerError::invalid_token(&format!("Token verification failed: {}", e)))?;

    Ok(decoded.claims)
}

pub async fn get_email_from_token(
    token: &str,
    config: &Arc<AppConfig>,
) -> ServerResult<Option<String>> {
    let issuer = config.oauth.issuer.trim_end_matches('/').to_string();
    let userinfo_url = format!("{}/userinfo", issuer);

    let resp = Client::new()
        .get(&userinfo_url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| ServerError::internal_error(&format!("Failed to call userinfo: {}", e)))?;

    if !resp.status().is_success() {
        return Err(ServerError::invalid_token(&format!(
            "userinfo endpoint returned {}",
            resp.status()
        )));
    }

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| ServerError::internal_error(&format!("Failed to parse userinfo: {}", e)))?;

    Ok(json
        .get("email")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()))
}
