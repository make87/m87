use serde::Deserialize;

use crate::response::ServerResult;

#[derive(Debug, Clone, Deserialize)]
pub struct OAuthConfig {
    /// e.g. "https://auth.make87.com" or your Keycloak realm URL
    pub issuer: String,

    pub audience: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub mongo_uri: String,
    pub mongo_db: String,
    pub oauth: OAuthConfig,
    pub public_address: String,
    pub cert_contact: String,
    pub unified_port: u16,
    pub rest_port: u16,
    pub forward_secret: String,
    pub is_staging: bool,
    pub admin_emails: Vec<String>,
}

impl AppConfig {
    pub fn from_env() -> ServerResult<Self> {
        // Keep it simple: read from env; in prod you might use figment/envy.
        let mongo_uri =
            std::env::var("MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into());
        let mongo_db = std::env::var("MONGO_DB").unwrap_or_else(|_| "m87-server".into());
        let issuer =
            std::env::var("OAUTH_ISSUER").unwrap_or_else(|_| "https://auth.make87.com/".into());
        let audience =
            std::env::var("OAUTH_AUDIENCE").unwrap_or_else(|_| "https://auth.make87.com".into());

        let public_address = std::env::var("PUBLIC_ADDRESS").unwrap_or_else(|_| "localhost".into());
        let forward_secret =
            std::env::var("FORWARD_SECRET").unwrap_or_else(|_| "change_me_in_prod".into());

        let is_staging = std::env::var("STAGING").unwrap_or("1".to_string()) == "1";

        let unified_port = std::env::var("UNIFIED_PORT")
            .unwrap_or_else(|_| "8084".into())
            .parse()
            .unwrap();
        let rest_port = std::env::var("REST_PORT")
            .unwrap_or_else(|_| "8085".into())
            .parse()
            .unwrap();

        let cert_contact =
            std::env::var("CERT_CONTACT").unwrap_or_else(|_| "admin@make87.com".into());

        // no default
        let admin_emails = std::env::var("ADMIN_EMAILS")
            .unwrap_or_else(|_| "".to_string())
            .split(',')
            .map(|email| email.trim().to_string())
            .collect();

        Ok(Self {
            mongo_uri,
            mongo_db,
            oauth: OAuthConfig { issuer, audience },
            public_address,
            cert_contact,
            unified_port,
            rest_port,
            forward_secret,
            is_staging,
            admin_emails,
        })
    }
}
