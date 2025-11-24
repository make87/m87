use anyhow::Result;

/// Get the public IP address of the current machine
pub async fn get_public_ip() -> Result<String> {
    let response = reqwest::get("https://api.ipify.org").await?;
    let ip = response.text().await?;
    Ok(ip)
}
