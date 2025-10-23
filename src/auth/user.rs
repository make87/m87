use anyhow::Result;
use reqwest::{Client, Error};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestAPIKeyBody {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub name: String,
    pub roles: Vec<String>,
    pub organization: String,
    pub organization_id: String,
    #[serde(default)]
    pub accepted_terms_hash: Option<String>,
    pub avatar_url: String,
    pub features: Vec<String>,
}

pub async fn request_api_key(api_url: &str, token: &str, name: &str) -> Result<String, Error> {
    let client = Client::new();
    let body = RequestAPIKeyBody {
        name: name.to_string(),
    };
    let res = client
        .post(format!("{}/api/v0/users/api-key", api_url))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;
    Ok(res.json::<String>().await?)
}

pub async fn me(api_url: &str, token: &str) -> Result<User> {
    let client = Client::new();
    let res = client
        .get(format!("{}/api/v0/users/me", api_url))
        .bearer_auth(token)
        .send()
        .await?;
    Ok(res.json::<User>().await?)
}
