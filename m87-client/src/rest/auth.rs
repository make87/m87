use anyhow::{anyhow, Result};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use reqwest::Client;
use serde::{Deserialize, Serialize};

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

    // todo get claims from m87 server

    Ok(decoded.claims)
}
