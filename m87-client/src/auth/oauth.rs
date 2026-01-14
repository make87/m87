use anyhow::Result;
use openidconnect::core::{CoreClient, CoreDeviceAuthorizationResponse, CoreProviderMetadata};
use openidconnect::{
    AuthType, ClientId, DeviceAuthorizationUrl, EndpointMaybeSet, EndpointSet, IssuerUrl,
    OAuth2TokenResponse, RefreshToken, Scope,
};
use openidconnect::{EndpointNotSet, reqwest};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::util::shutdown::SHUTDOWN;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OAuth2Token {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: u64, // Unix timestamp of when the access token expires
}

impl OAuth2Token {
    pub fn is_valid(&self) -> bool {
        let current_time = OAuth2Token::current_time();
        current_time < self.expires_at
    }

    pub async fn get_access_token(&mut self, issuer_url: &str, client_id: &str) -> Result<String> {
        if self.is_valid() {
            Ok(self.access_token.clone())
        } else {
            self.refresh(issuer_url, client_id).await?;
            Ok(self.access_token.clone())
        }
    }

    fn current_time() -> u64 {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        current_time
    }

    pub async fn refresh(&mut self, issuer_url: &str, client_id: &str) -> Result<()> {
        let client = OAuth2Token::get_client(issuer_url, client_id).await?;

        let http_client = reqwest::ClientBuilder::new()
            // Following redirects opens the client up to SSRF vulnerabilities.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        let refresh_token = RefreshToken::new(
            self.refresh_token
                .clone()
                .ok_or_else(|| anyhow::anyhow!("missing refresh_token"))?,
        );

        let token_response = client
            .exchange_refresh_token(&refresh_token)?
            .request_async(&http_client)
            .await?;

        let new_access_token = token_response.access_token().secret().clone();
        let current_time = OAuth2Token::current_time();
        let new_expires_at = current_time + token_response.expires_in().unwrap().as_secs();

        self.access_token = new_access_token;
        self.expires_at = new_expires_at;

        // if server rotated the refresh token, keep the new one
        if let Some(rt) = token_response.refresh_token() {
            self.refresh_token = Some(rt.secret().to_string());
        }

        Ok(())
    }

    /// Build a CoreClient configured for device flow against the given issuer.
    ///
    /// Typestate of the returned client:
    /// CoreClient<EndpointMaybeSet, EndpointSet, EndpointMaybeSet, EndpointMaybeSet, EndpointMaybeSet, EndpointMaybeSet>
    pub async fn get_client(
        issuer_url: &str,
        client_id: &str,
    ) -> Result<
        CoreClient<
            EndpointSet,      // auth URL (discovered → always set)
            EndpointSet,      // token URL (discovered → always set)
            EndpointNotSet,   // device BEFORE calling set_device_authorization_url
            EndpointNotSet,   // introspection
            EndpointMaybeSet, // revocation
            EndpointMaybeSet, // userinfo
        >,
    > {
        let client_id = ClientId::new(client_id.to_string());
        let issuer_url = IssuerUrl::new(issuer_url.to_string())?;

        let http_client = reqwest::ClientBuilder::new()
            // Following redirects opens the client up to SSRF vulnerabilities.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        // Discover the provider metadata (includes token endpoint, etc.)
        let provider_metadata =
            CoreProviderMetadata::discover_async(issuer_url.clone(), &http_client).await?;

        // Create the OAuth2 client
        let client = CoreClient::from_provider_metadata(provider_metadata, client_id, None)
            .set_auth_type(AuthType::RequestBody)
            // Set the device authorization endpoint manually (Auth0-style)
            .set_device_authorization_url(DeviceAuthorizationUrl::new(format!(
                "{}oauth/device/code",
                issuer_url.as_str()
            ))?);

        Ok(client)
    }

    pub async fn device_flow_login(
        issuer_url: &str,
        client_id: &str,
        audience: &str,
        report_handler: &mut dyn SendUserAuthRequestHandler,
    ) -> Result<OAuth2Token> {
        let client = OAuth2Token::get_client(issuer_url, client_id).await?;

        let http_client = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        let scopes = vec![
            Scope::new("openid".to_string()),
            Scope::new("offline_access".to_string()),
            Scope::new("email".to_string()),
            Scope::new("profile".to_string()),
        ];

        let details: CoreDeviceAuthorizationResponse = client
            .exchange_device_code()
            .add_scopes(scopes)
            .add_extra_param("audience", audience.to_string())
            .request_async(&http_client)
            .await?;

        report_handler
            .send_auth_request(
                &details.verification_uri().to_string(),
                details.user_code().secret(),
            )
            .await?;

        let shutdown = SHUTDOWN.clone();

        // Spinner that stops on shutdown OR abort() OR completes naturally
        let spinner_handle = tokio::spawn(async move {
            let spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
            let mut i = 0;

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        print!("\r\x1b[2K");
                        let _ = std::io::stdout().flush();
                        return;
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(120)) => {
                        print!(
                            "\r{} Waiting for authentication...",
                            spinner_chars[i % spinner_chars.len()]
                        );
                        let _ = std::io::stdout().flush();
                        i += 1;
                    }
                }
            }
        });

        // token polling future
        let poll_fut = client
            .exchange_device_access_token(&details)?
            .request_async(&http_client, tokio_sleep, Some(Duration::from_secs(900)));

        let token_response = tokio::select! {
            res = poll_fut => {
                res?
            }

            _ = SHUTDOWN.cancelled() => {
                spinner_handle.abort();
                print!("\r\x1b[2K");
                std::io::stdout().flush().ok();
                return Err(anyhow::anyhow!("authentication aborted by user"));
            }
        };

        // Stop spinner
        spinner_handle.abort();
        print!("\r\x1b[2K");
        std::io::stdout().flush().ok();

        // Extract token info
        let access_token = token_response.access_token().secret().to_string();
        let refresh_token = token_response
            .refresh_token()
            .map(|t| t.secret().to_string());
        let expires_in = token_response
            .expires_in()
            .unwrap_or(Duration::from_secs(3600))
            .as_secs();
        let expires_at = OAuth2Token::current_time() + expires_in;

        let _ = report_handler.on_auth_success(&access_token).await;

        Ok(OAuth2Token {
            access_token,
            refresh_token,
            expires_at,
        })
    }
}

// New async sleep function matching the v4 DeviceAccessTokenRequest::request_async signature
async fn tokio_sleep(duration: std::time::Duration) {
    tokio::time::sleep(duration).await;
}

#[async_trait::async_trait]
pub trait SendUserAuthRequestHandler: Send + Sync {
    async fn send_auth_request(&mut self, verification_uri: &str, user_code: &str) -> Result<()>;

    async fn on_auth_success(&self, token: &str) -> Result<()>;
}

pub struct PrintUserAuthRequestHandler;

#[async_trait::async_trait]
impl SendUserAuthRequestHandler for PrintUserAuthRequestHandler {
    async fn send_auth_request(&mut self, verification_uri: &str, user_code: &str) -> Result<()> {
        tracing::info!("[done] Interaction needed");
        println!("\nTo authenticate, visit: {}", verification_uri);
        println!("Enter this code: {}\n", user_code);
        Ok(())
    }

    async fn on_auth_success(&self, _token: &str) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oauth2_token_is_valid_not_expired() {
        // Token expires far in the future
        let token = OAuth2Token {
            access_token: "test_access_token".to_string(),
            refresh_token: Some("test_refresh_token".to_string()),
            expires_at: u64::MAX, // Never expires (practically)
        };
        assert!(token.is_valid());
    }

    #[test]
    fn test_oauth2_token_is_valid_expired() {
        // Token expired in the past
        let token = OAuth2Token {
            access_token: "test_access_token".to_string(),
            refresh_token: Some("test_refresh_token".to_string()),
            expires_at: 0, // Expired at Unix epoch
        };
        assert!(!token.is_valid());
    }

    #[test]
    fn test_oauth2_token_serialization() {
        let token = OAuth2Token {
            access_token: "my_access_token".to_string(),
            refresh_token: Some("my_refresh_token".to_string()),
            expires_at: 1700000000,
        };

        let json = serde_json::to_string(&token).unwrap();
        let deserialized: OAuth2Token = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.access_token, "my_access_token");
        assert_eq!(
            deserialized.refresh_token,
            Some("my_refresh_token".to_string())
        );
        assert_eq!(deserialized.expires_at, 1700000000);
    }

    #[test]
    fn test_oauth2_token_serialization_no_refresh() {
        let token = OAuth2Token {
            access_token: "access_only".to_string(),
            refresh_token: None,
            expires_at: 1234567890,
        };

        let json = serde_json::to_string(&token).unwrap();
        let deserialized: OAuth2Token = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.access_token, "access_only");
        assert!(deserialized.refresh_token.is_none());
    }
}
