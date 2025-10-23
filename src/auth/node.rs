use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::auth::oauth::SendUserAuthRequestHandler;
use crate::retry_async;

pub struct NodeAuthRequestHandler {
    pub api_url: String,
    pub node_info: Option<String>,
    pub hostname: String,
    pub node_id: String,
    pub owner_reference: String,
    pub request_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct NodeAuthRequestBody {
    pub node_info: String,
    pub verification_url: String,
    pub verification_code: String,
    pub hostname: String,
    pub owner_reference: String,
    pub node_id: String,
}

async fn set_auth_request(api_url: &str, body: NodeAuthRequestBody) -> Result<String> {
    let url = format!("{}/api/v0/nodes/auth", api_url);
    let client = Client::new();

    let res = retry_async!(3, 3, client.post(&url).json(&body).send())?;
    match res.error_for_status() {
        Ok(r) => {
            // returns a string with node id on success
            let node_id: String = r.json().await?;
            Ok(node_id)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

async fn delete_auth_request(request_id: &str, api_url: &str, token: &str) -> Result<()> {
    let url = format!("{}/api/v0/nodes/auth/{}", api_url, request_id);
    let client = Client::new();

    let res = retry_async!(3, 3, client.delete(&url).bearer_auth(token).send())?;
    match res.error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("[Node] Error deleting auth request: {}", e);
            Err(anyhow!(e))
        }
    }
}

#[async_trait::async_trait]
impl SendUserAuthRequestHandler for NodeAuthRequestHandler {
    async fn send_auth_request(&mut self, verification_uri: &str, user_code: &str) -> Result<()> {
        let node_info = self.node_info.as_ref().expect(
            "Node info not set. This is needed for the user to know which node to authenticate",
        );
        let body = NodeAuthRequestBody {
            node_info: node_info.clone(),
            verification_url: verification_uri.to_string(),
            verification_code: user_code.to_string(),
            hostname: self.hostname.clone(),
            owner_reference: self.owner_reference.clone(),
            node_id: self.node_id.clone(),
        };
        let request_id = set_auth_request(&self.api_url, body).await?;
        self.request_id = Some(request_id);
        Ok(())
    }

    async fn on_auth_success(&self, token: &str) -> Result<()> {
        let _ = delete_auth_request(&self.request_id.clone().unwrap(), &self.api_url, token).await;

        Ok(())
    }
}
