use std::time::Duration;

use anyhow::Result;
use tokio::time::Instant;
use tracing::info;

use crate::server;

pub struct DeviceAuthRequestHandler {
    pub api_url: String,
    pub device_info: Option<String>,
    pub hostname: String,
    pub device_id: String,
    pub owner_scope: String,
    pub request_id: Option<String>,
    pub trust_invalid_server_cert: bool,
}

impl DeviceAuthRequestHandler {
    pub async fn send_auth_request(&mut self) -> Result<()> {
        let device_info = self.device_info.as_ref().expect(
            "Device info not set. This is needed for the user to know which device to authenticate",
        );
        let body = server::DeviceAuthRequestBody {
            device_info: device_info.clone(),
            hostname: self.hostname.clone(),
            owner_scope: self.owner_scope.clone(),
            device_id: self.device_id.clone(),
        };
        let request_id =
            server::set_auth_request(&self.api_url, body, self.trust_invalid_server_cert).await?;
        self.request_id = Some(request_id.clone());

        info!(
            "Posted auth request. To approve, check request id {} via cli or visit make87.com",
            request_id
        );
        Ok(())
    }

    pub async fn wait_for_approval(&self, timeout: Duration) -> Result<String> {
        let request_id = match &self.request_id {
            Some(id) => id,
            None => return Err(anyhow::anyhow!("Request ID not set")),
        };
        let start_time = Instant::now();
        while start_time.elapsed() < timeout {
            let res = server::check_auth_request(
                &self.api_url,
                request_id,
                self.trust_invalid_server_cert,
            )
            .await?;
            if let Some(api_key) = res.api_key {
                return Ok(api_key);
            } else {
                // sleep
                tokio::time::sleep(tokio::time::Duration::from_millis(10000)).await;
            }
        }
        Err(anyhow::anyhow!("API key not approved within timeout"))
    }

    pub async fn handle_headless_auth(&mut self, timeout: Duration) -> Result<String> {
        self.send_auth_request().await?;
        let api_key = self.wait_for_approval(timeout).await?;
        Ok(api_key)
    }
}
