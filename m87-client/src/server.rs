use std::time::Duration;

use anyhow::{Result, anyhow};
use m87_shared::deploy_spec::{
    CreateDeployRevisionBody, DeployReport, DeploymentRevision, DeploymentStatusSnapshot,
    UpdateDeployRevisionBody,
};
use m87_shared::device::{AddDeviceAccessBody, AuditLog, DeviceStatus, UpdateDeviceBody};
use m87_shared::org::{
    AcceptRejectBody, AddDeviceBody, CreateOrganizationBody, Invite, InviteMemberBody,
    Organization, UpdateOrganizationBody,
};
use m87_shared::roles::Role;
use m87_shared::users::User;
use reqwest::Client;

use tracing::error;

// Import shared types
pub use m87_shared::auth::{
    AuthRequestAction, CheckAuthRequest, DeviceAuthRequest, DeviceAuthRequestBody,
    DeviceAuthRequestCheckResponse,
};
pub use m87_shared::device::PublicDevice;
pub use m87_shared::heartbeat::{HeartbeatRequest, HeartbeatResponse};

pub async fn get_server_url_and_owner_reference(
    make87_api_url: &str,
    make87_app_url: &str,
    owner_reference: Option<String>,
    server_url: Option<String>,
) -> Result<(String, String)> {
    // if owner ref and server url are some return them right away
    if let Some(owner_ref) = &owner_reference {
        if let Some(server) = &server_url {
            return Ok((server.clone(), owner_ref.clone()));
        }
    }

    let client = reqwest::Client::new();

    let post_url = format!("{}/v1/device/login", make87_api_url);

    #[derive(serde::Serialize)]
    struct EmptyBody {
        owner_reference: Option<String>,
        server_url: Option<String>,
    }

    let id: String = client
        .post(&post_url)
        .json(&EmptyBody {
            owner_reference: owner_reference.clone(),
            server_url,
        })
        .send()
        .await?
        .error_for_status()
        .map_err(|e| {
            error!("{:?}", e);
            e
        })?
        .json()
        .await?;

    if owner_reference.is_none() {
        // we only need the user to interact if we are missing a assigned owner. If we know the owner server can be aut oassigned
        let browser_url = format!("{}/devices/login/{}", make87_app_url, id);
        tracing::error!("No server configured.");
        tracing::error!("Open this link in your browser to log in:");
        tracing::error!("{}", browser_url);
        tracing::error!("Waiting for authentication...");
    }

    let get_url = format!("{}/v1/device/login/{}", make87_api_url, id);

    #[derive(serde::Deserialize)]
    struct LoginUrlResponse {
        url: Option<String>,
        owner_reference: Option<String>,
    }

    let mut wait_time = 0;

    loop {
        let resp = client
            .get(&get_url)
            .send()
            .await?
            .error_for_status()?
            .json::<LoginUrlResponse>()
            .await?;

        match (resp.url, resp.owner_reference) {
            (Some(url), Some(owner_reference)) => {
                return Ok((url, owner_reference));
            }
            _ => {}
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        wait_time += 2;
        if wait_time >= 120 {
            tracing::error!("Timeout waiting 120s for authentication");
            return Err(anyhow::anyhow!("Timeout waiting for authentication"));
        }
    }
}

pub async fn get_manager_server_urls(make87_api_url: &str, token: &str) -> Result<Vec<String>> {
    let client = reqwest::Client::new();

    let get_url = format!("{}/v1/server", make87_api_url);
    // get will return all server objects.. get url form each json object

    #[derive(serde::Deserialize)]
    struct Server {
        url: String,
    }

    let response = client
        .get(&get_url)
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<Server>>()
        .await?;

    let manager_urls = response.into_iter().map(|s| s.url).collect::<Vec<String>>();

    Ok(manager_urls)
}

// Runtime-specific: Used by device registration
#[cfg(feature = "runtime")]
pub async fn set_auth_request(
    api_url: &str,
    body: DeviceAuthRequestBody,
    trust_invalid_server_cert: bool,
) -> Result<String> {
    let url = format!("{}/auth/request", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.post(&url).json(&body).send().await?;
    match res.error_for_status() {
        Ok(r) => {
            // returns a string with device id on success
            let device_id: String = r.json().await?;
            Ok(device_id)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

// Runtime-specific: Used by device registration
#[cfg(feature = "runtime")]
pub async fn check_auth_request(
    api_url: &str,
    request_id: &str,
    trust_invalid_server_cert: bool,
) -> Result<DeviceAuthRequestCheckResponse> {
    let url = format!("{}/auth/request/check", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client
        .post(&url)
        .json(&CheckAuthRequest {
            request_id: request_id.to_string(),
        })
        .send()
        .await?;
    match res.error_for_status() {
        Ok(r) => {
            // returns a string with device id on success
            let response: DeviceAuthRequestCheckResponse = r.json().await?;
            Ok(response)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

// m87 command line: List pending device auth requests
pub async fn list_auth_requests(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
) -> Result<Vec<DeviceAuthRequest>, anyhow::Error> {
    let url = format!("{}/auth/request", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.get(&url).bearer_auth(token).send().await?;
    match res.error_for_status() {
        Ok(r) => {
            let response: Vec<DeviceAuthRequest> = r.json().await?;
            Ok(response)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

// m87 command line: Approve or reject device registration
pub async fn handle_auth_request(
    api_url: &str,
    token: &str,
    request_id: &str,
    accept: bool,
    trust_invalid_server_cert: bool,
) -> Result<(), anyhow::Error> {
    let url = format!("{}/auth/request/approve", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&AuthRequestAction {
            accept,
            request_id: request_id.to_string(),
        })
        .send()
        .await?;
    match res.error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

// m87 command line: List all accessible devices
pub async fn list_devices(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
) -> Result<Vec<PublicDevice>> {
    let client = get_client(trust_invalid_server_cert)?;

    let res = client
        .get(&format!("{}/device", api_url))
        .bearer_auth(token)
        .send()
        .await?;
    match res.error_for_status() {
        Ok(res) => Ok(res.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

fn get_client(trust_invalid_server_cert: bool) -> Result<Client> {
    // if its localhost we accept invalid certificates
    if trust_invalid_server_cert {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(5))
            .build()?;
        Ok(client)
    } else {
        // otherwise we verify the certificate
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;
        Ok(client)
    }
}

pub async fn update_device(
    api_url: &str,
    token: &str,
    device_id: &str,
    body: UpdateDeviceBody,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    let client = get_client(trust_invalid_server_cert)?;
    let url = format!("{}/device/{}", api_url.trim_end_matches('/'), device_id);

    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await;
    if let Err(e) = res {
        tracing::error!("Error reporting device details: {}", e);
        return Err(anyhow!(e));
    }
    match res.unwrap().error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!("Error reporting device details: {}", e);
            Err(anyhow!(e))
        }
    }
}

pub async fn get_device_status(
    api_url: &str,
    token: &str,
    device_id: &str,
    trust_invalid_server_cert: bool,
) -> Result<DeviceStatus> {
    let client = get_client(trust_invalid_server_cert)?;

    let url = format!("{}/device/{}/status", api_url, device_id);

    let res = client
        .get(&url)
        .bearer_auth(token)
        // .query(&[("since", since)])
        .send()
        .await;
    if let Err(e) = res {
        tracing::error!("Error getting device status: {}", e);
        return Err(anyhow!(e));
    }
    match res.unwrap().error_for_status() {
        Ok(r) => {
            let status = r.json().await?;
            Ok(status)
        }
        Err(e) => {
            tracing::error!("Error getting device status: {}", e);
            Err(anyhow!(e))
        }
    }
}

// ------------------------- Deployment -------------------------

pub async fn get_deployments(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    offset: Option<u64>,
    limit: Option<u64>,
) -> Result<Vec<DeploymentRevision>> {
    let mut url = format!("{}/device/{}/revisions", api_url, device_id);

    if offset.is_some() || limit.is_some() {
        let mut params = vec![];
        if let Some(o) = offset {
            params.push(format!("offset={}", o));
        }
        if let Some(l) = limit {
            params.push(format!("limit={}", l));
        }
        url = format!("{}?{}", url, params.join("&"));
    }

    let client = get_client(trust_invalid_server_cert)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => {
            let deployments: Vec<DeploymentRevision> = r.json().await?;
            Ok(deployments)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn get_deployment(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    revision_id: &str,
) -> Result<DeploymentRevision> {
    let url = format!("{}/device/{}/revisions/{}", api_url, device_id, revision_id);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => {
            let revision: DeploymentRevision = r.json().await?;
            Ok(revision)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn create_deployment(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    body: CreateDeployRevisionBody,
) -> Result<DeploymentRevision> {
    let url = format!("{}/device/{}/revisions", api_url, device_id);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    match res.error_for_status() {
        Ok(r) => {
            let revision: DeploymentRevision = r.json().await?;
            Ok(revision)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn update_deployment(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    revision_id: &str,
    body: UpdateDeployRevisionBody,
) -> Result<()> {
    let url = format!("{}/device/{}/revisions/{}", api_url, device_id, revision_id);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    match res.error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn delete_deployment(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    revision_id: &str,
) -> Result<()> {
    let url = format!("{}/device/{}/revisions/{}", api_url, device_id, revision_id);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.delete(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn get_active_deployment_id(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
) -> Result<Option<String>> {
    let url = format!("{}/device/{}/revisions/active", api_url, device_id);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn get_deployment_reports(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    deployment_id: &str,
) -> Result<Vec<DeployReport>> {
    let url = format!(
        "{}/device/{}/revisions/{}/reports",
        api_url, device_id, deployment_id
    );
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn get_device_revision_snapshot(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    deployment_id: &str,
) -> Result<DeploymentStatusSnapshot> {
    let url = format!(
        "{}/device/{}/revisions/{}/snapshot",
        api_url, device_id, deployment_id
    );
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn get_device_audit_logs(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    limit: u32,
    since: Option<String>, // RFC3339, e.g. "2026-01-01T00:00:00Z"
    until: Option<String>,
) -> Result<Vec<AuditLog>> {
    let url = format!("{}/device/{}/audit_logs", api_url, device_id);
    let client = get_client(trust_invalid_server_cert)?;
    // Build query params
    let mut q: Vec<(&str, String)> = vec![("limit", limit.to_string())];
    if let Some(s) = since {
        q.push(("since", s.to_string()));
    }
    if let Some(u) = until {
        q.push(("until", u.to_string()));
    }

    let res = client.get(&url).bearer_auth(token).query(&q).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn get_device_users(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
) -> Result<Vec<User>> {
    let url = format!("{}/device/{}/users", api_url, device_id);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn remove_device_access(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    email_or_org_id: &str,
) -> Result<()> {
    let url = format!(
        "{}/device/{}/access/{}",
        api_url, device_id, email_or_org_id
    );
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.delete(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn add_device_access(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    email_or_org_id: &str,
    role: Role,
) -> Result<()> {
    let url = format!("{}/device/{}/access", api_url, device_id);
    let client = get_client(trust_invalid_server_cert)?;
    let body = AddDeviceAccessBody {
        email_or_org_id: email_or_org_id.to_string(),
        role,
    };

    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    match res.error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn update_device_access(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    email_or_org_id: &str,
    role: Role,
) -> Result<()> {
    let url = format!(
        "{}/device/{}/access/{}",
        api_url, device_id, email_or_org_id
    );
    let client = get_client(trust_invalid_server_cert)?;
    let body = AddDeviceAccessBody {
        email_or_org_id: email_or_org_id.to_string(),
        role,
    };

    let res = client
        .put(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    match res.error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn list_organizations(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
) -> Result<Vec<Organization>> {
    let url = format!("{}/organization", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn create_organization(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    id: &str,
    owner_email: &str,
) -> Result<()> {
    let url = format!("{}/organization", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let body = CreateOrganizationBody {
        id: id.to_string(),
        owner_email: owner_email.to_string(),
    };

    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn delete_organization(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    id: &str,
) -> Result<()> {
    let url = format!("{}/organization/{}", api_url, id);
    let client = get_client(trust_invalid_server_cert)?;

    let res = client.delete(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn update_organization(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    id: &str,
    new_id: &str,
) -> Result<Organization> {
    let url = format!("{}/organization/{}", api_url, id);
    let client = get_client(trust_invalid_server_cert)?;

    let body = UpdateOrganizationBody {
        new_id: new_id.to_string(),
    };

    let res = client
        .put(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn list_organization_members(
    server_url: &str,
    token: &str,
    trust: bool,
    org_id: String,
) -> Result<Vec<User>> {
    let url = format!("{}/organization/{}/members", server_url, org_id);
    let client = get_client(trust)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn add_organization_member(
    server_url: &str,
    token: &str,
    trust: bool,
    org_id: String,
    email: String,
    role: Role,
) -> Result<()> {
    let url = format!("{}/organization/{}/members", server_url, org_id);
    let client = get_client(trust)?;

    let body = InviteMemberBody { email, role };
    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn remove_organization_member(
    server_url: &str,
    token: &str,
    trust: bool,
    org_id: String,
    user_id: String,
) -> Result<()> {
    let url = format!("{}/organization/{}/members/{}", server_url, org_id, user_id);
    let client = get_client(trust)?;

    let res = client.delete(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn list_organization_invites(
    server_url: &str,
    token: &str,
    trust: bool,
) -> Result<Vec<Invite>> {
    let url = format!("{}/invites", server_url);
    let client = get_client(trust)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn handle_organization_invite(
    server_url: &str,
    token: &str,
    trust: bool,
    invite_id: &str,
    accept: bool,
) -> Result<String> {
    let url = format!("{}/invites/{}", server_url, invite_id);
    let client = get_client(trust)?;
    let body = AcceptRejectBody {
        invite_id: invite_id.to_string(),
        accepted: accept,
    };
    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.text().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn list_org_devices(
    server_url: &str,
    token: &str,
    trust: bool,
    org_id: &str,
) -> Result<Vec<PublicDevice>> {
    let url = format!("{}/organization/{}/devices", server_url, org_id);
    let client = get_client(trust)?;

    let res = client.get(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn add_org_device(
    server_url: &str,
    token: &str,
    trust: bool,
    org_id: &str,
    device_id: &str,
) -> Result<()> {
    let url = format!("{}/organization/{}/devices", server_url, org_id);
    let client = get_client(trust)?;

    let body = AddDeviceBody {
        device_id: device_id.to_string(),
    };

    let res = client
        .post(&url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn remove_org_device(
    server_url: &str,
    token: &str,
    trust: bool,
    org_id: &str,
    device_id: &str,
) -> Result<()> {
    let url = format!(
        "{}/organization/{}/devices/{}",
        server_url, org_id, device_id
    );
    let client = get_client(trust)?;

    let res = client.delete(&url).bearer_auth(token).send().await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}
