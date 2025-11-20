use anyhow::Result;
use openidconnect::core::{CoreClient, CoreDeviceAuthorizationResponse, CoreProviderMetadata};
use openidconnect::reqwest::async_http_client;
use openidconnect::{
    ClientId, DeviceAuthorizationUrl, IssuerUrl, OAuth2TokenResponse, RefreshToken,
    RequestTokenError, Scope,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        let token_response = client
            .exchange_refresh_token(&RefreshToken::new(self.refresh_token.clone().unwrap()))
            .request_async(async_http_client)
            .await?;

        let new_access_token = token_response.access_token().secret().clone();
        let current_time = OAuth2Token::current_time();
        let new_expires_at = current_time + token_response.expires_in().unwrap().as_secs();

        self.access_token = new_access_token;
        self.expires_at = new_expires_at;
        Ok(())
    }

    pub async fn get_client(issuer_url: &str, client_id: &str) -> Result<CoreClient> {
        let client_id = ClientId::new(client_id.to_string());
        let issuer_url = IssuerUrl::new(issuer_url.to_string())?;

        // Discover the provider metadata
        let provider_metadata =
            CoreProviderMetadata::discover_async(issuer_url.clone(), async_http_client).await?;

        // Create the OAuth2 client
        let client = CoreClient::from_provider_metadata(provider_metadata, client_id, None)
            // Set the device authorization endpoint manually
            .set_device_authorization_uri(DeviceAuthorizationUrl::new(format!(
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

        let scopes = vec![
            Scope::new("openid".to_string()),
            Scope::new("offline_access".to_string()), // If you need a refresh token
            Scope::new("email".to_string()),
            Scope::new("profile".to_string()),
        ];

        // Start the device authorization request
        let details: CoreDeviceAuthorizationResponse = client
            .exchange_device_code()?
            .add_scopes(scopes)
            .add_extra_param("audience", audience)
            .request_async(async_http_client)
            .await?;

        // Display instructions to the user
        report_handler
            .send_auth_request(
                &details.clone().verification_uri().to_string(),
                details.clone().user_code().secret(),
            )
            .await?;

        // Spawn a spinner task that runs in the background
        let spinner_handle = tokio::spawn(async {
            let spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
            let mut i = 0;
            loop {
                print!("\r{} Waiting for authentication...", spinner_chars[i % spinner_chars.len()]);
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(80)).await;
                i += 1;
            }
        });

        // Request the access token (no manual polling loop needed)
        let token_response = client
            .exchange_device_access_token(&details)
            .request_async(
                async_http_client,
                async_sleep, // Sleep function
                // timeout after 15 minutes. Thats the max time the device code is valid
                Some(Duration::from_secs(900)),
            )
            .await
            .map_err(|e| {
                match e {
                    RequestTokenError::ServerResponse(e) => RequestTokenError::ServerResponse(e),
                    RequestTokenError::Request(e) => RequestTokenError::Request(e),
                    RequestTokenError::Parse(e, v) => {
                        //vec u8 to string parse
                        let message = std::str::from_utf8(&v).unwrap();
                        println!("Error msg: {:?}", message);
                        println!("Error: {:?}", e);
                        RequestTokenError::Parse(e, v)
                    }
                    RequestTokenError::Other(e) => RequestTokenError::Other(e),
                }
            })?;

        // Stop the spinner
        spinner_handle.abort();
        print!("\r\x1b[2K"); // Clear the spinner line
        std::io::Write::flush(&mut std::io::stdout()).unwrap();

        // Extract token information
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

async fn async_sleep(duration: std::time::Duration) {
    sleep(duration);
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
        // Clear, prominent display of authentication information
        println!("\nTo authenticate, visit: {}", verification_uri);
        println!("Enter this code: {}\n", user_code);

        Ok(())
    }
    async fn on_auth_success(&self, _token: &str) -> Result<()> {
        Ok(())
    }
}
