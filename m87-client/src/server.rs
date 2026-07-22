use std::time::Duration;

use anyhow::{Result, anyhow};
use m87_shared::deploy_spec::{
    CreateDeployRevisionBody, DeployReport, DeploymentRevision, DeploymentStatusSnapshot, JobRun,
    Lifecycle, LifecycleUpdate, TriggerJobBody, UpdateDeployRevisionBody,
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
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(client)
    } else {
        // otherwise we verify the certificate
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
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

/// Timeout + retry budget for "heavy" endpoints — ones that fan out to multiple
/// DB writes or stream/fold/aggregate over `deploy_reports` server-side. These
/// can legitimately take longer than a quick read, well past the default 10s
/// client timeout under load, which is the "operation timed out" customers hit
/// and fixed by re-running. Only applied to idempotent requests (GET reads and
/// the idempotent revision update), so retrying is always safe.
const HEAVY_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const HEAVY_MAX_ATTEMPTS: u32 = 3;

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
    let resp = send_retrying(
        || client.post(&url).bearer_auth(token).json(&body),
        HEAVY_REQUEST_TIMEOUT,
        HEAVY_MAX_ATTEMPTS,
    )
    .await?;
    resp.error_for_status().map(|_| ()).map_err(|e| anyhow!(e))
}

/// Send a request (rebuilt by `build` each attempt, since a `RequestBuilder` is
/// single-use), retrying on a timeout / connection drop with a per-request
/// timeout. Does NOT retry on an HTTP error status — a real server error won't
/// fix itself. Only use for idempotent requests.
async fn send_retrying<F>(
    build: F,
    timeout: Duration,
    max_attempts: u32,
) -> Result<reqwest::Response>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let mut last_err = None;
    for attempt in 1..=max_attempts {
        match build().timeout(timeout).send().await {
            Ok(resp) => return Ok(resp),
            Err(e) if (e.is_timeout() || e.is_connect()) && attempt < max_attempts => {
                tracing::warn!("request failed ({e}); retry {attempt}/{max_attempts}");
                last_err = Some(e);
            }
            Err(e) => return Err(anyhow!(e)),
        }
    }
    Err(anyhow!(last_err.expect("loop runs at least once")))
}

/// Test wrapper preserving the earlier `post_json_retrying` surface used by the
/// retry tests: POST JSON with the heavy retry policy, returning `()`.
#[cfg(test)]
async fn post_json_retrying<B: serde::Serialize>(
    client: &Client,
    url: &str,
    token: &str,
    body: &B,
    timeout: Duration,
    max_attempts: u32,
) -> Result<()> {
    let resp = send_retrying(
        || client.post(url).bearer_auth(token).json(body),
        timeout,
        max_attempts,
    )
    .await?;
    resp.error_for_status().map(|_| ()).map_err(|e| anyhow!(e))
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

    // Heavy read (server scans deploy_reports); tolerate a slow server under
    // load with a longer timeout + retry (idempotent GET).
    let res = send_retrying(
        || client.get(&url).bearer_auth(token),
        HEAVY_REQUEST_TIMEOUT,
        HEAVY_MAX_ATTEMPTS,
    )
    .await?;

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

    // The `m87 health` snapshot streams and folds ALL deploy_reports for the
    // device+revision server-side — the heaviest read in the API, and the one
    // most likely to exceed the default timeout on a busy device. Longer
    // timeout + retry (idempotent GET).
    let res = send_retrying(
        || client.get(&url).bearer_auth(token),
        HEAVY_REQUEST_TIMEOUT,
        HEAVY_MAX_ATTEMPTS,
    )
    .await?;

    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

// ---------------------------------------------------------------------------
// Lifecycle updates
// ---------------------------------------------------------------------------

pub async fn send_lifecycle_update(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    unit_id: &str,
    lifecycle: Lifecycle,
) -> Result<()> {
    let url = format!(
        "{}/device/{}/units/{}/lifecycle",
        api_url, device_id, unit_id
    );
    let client = get_client(trust_invalid_server_cert)?;
    let body = serde_json::json!({ "lifecycle": lifecycle });
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

// ---------------------------------------------------------------------------
// Job runs
// ---------------------------------------------------------------------------

pub async fn trigger_job(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    revision_id: &str,
    job_id: &str,
    body: TriggerJobBody,
) -> Result<JobRun> {
    let url = format!(
        "{}/device/{}/revisions/{}/jobs/{}/trigger",
        api_url, device_id, revision_id, job_id
    );
    let client = get_client(trust_invalid_server_cert)?;
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

pub async fn list_job_runs(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    job_id: Option<&str>,
) -> Result<Vec<JobRun>> {
    let mut url = format!("{}/device/{}/job-runs", api_url, device_id);
    if let Some(id) = job_id {
        url = format!("{}?job_id={}", url, id);
    }
    let client = get_client(trust_invalid_server_cert)?;
    let res = client.get(&url).bearer_auth(token).send().await?;
    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn get_job_run(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
    run_id: &str,
) -> Result<JobRun> {
    let url = format!("{}/device/{}/job-runs/{}", api_url, device_id, run_id);
    let client = get_client(trust_invalid_server_cert)?;
    let res = client.get(&url).bearer_auth(token).send().await?;
    match res.error_for_status() {
        Ok(r) => Ok(r.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

pub async fn rollback_device(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
    device_id: &str,
) -> Result<()> {
    let url = format!("{}/device/{}/rollback", api_url, device_id);
    let client = get_client(trust_invalid_server_cert)?;
    let res = client.post(&url).bearer_auth(token).send().await?;
    match res.error_for_status() {
        Ok(_) => Ok(()),
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
        Ok(_) => Ok(()),
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
        Ok(_) => Ok(()),
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
        Ok(_) => Ok(()),
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
        Ok(_) => Ok(()),
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
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Clone)]
    enum Resp {
        /// Accept the request but never respond (client times out).
        Stall,
        /// Respond with an HTTP status and empty body.
        Status(u16),
    }

    /// Minimal raw-TCP HTTP mock: the Nth connection gets `seq[N]` (or 204 once
    /// the sequence is exhausted). Returns the base URL and a per-connection hit
    /// counter.
    async fn spawn_http_mock(seq: Vec<Resp>) -> (String, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        tokio::spawn(async move {
            for _ in 0..16 {
                let (mut sock, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let idx = h.fetch_add(1, Ordering::SeqCst);
                let behavior = seq.get(idx).cloned().unwrap_or(Resp::Status(204));
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let _ = sock.read(&mut buf).await; // drain request
                    match behavior {
                        Resp::Stall => {
                            tokio::time::sleep(Duration::from_secs(10)).await;
                        }
                        Resp::Status(code) => {
                            let msg =
                                format!("HTTP/1.1 {code} X\r\nContent-Length: 0\r\n\r\n");
                            let _ = sock.write_all(msg.as_bytes()).await;
                        }
                    }
                });
            }
        });
        (format!("http://{addr}"), hits)
    }

    // Reproduces the customer's intermittent `deploy` timeout: the first request
    // stalls past the (test-short) timeout, but a retry succeeds. Without the
    // retry this errors out — exactly the "operation timed out" the customer had
    // to fix by re-running the command.
    #[tokio::test]
    async fn deploy_retries_on_timeout_then_succeeds() {
        let (base, hits) = spawn_http_mock(vec![Resp::Stall, Resp::Status(204)]).await;
        let client = get_client(true).unwrap();
        let body = serde_json::json!({ "revision": "yaml" });
        let url = format!("{base}/device/d/revisions/r");
        let r = post_json_retrying(&client, &url, "tok", &body, Duration::from_millis(300), 3).await;
        assert!(r.is_ok(), "deploy should succeed via retry after a timeout: {r:?}");
        assert_eq!(hits.load(Ordering::SeqCst), 2, "should have retried exactly once");
    }

    // A real server error is NOT a timeout — it must surface, not be retried into
    // a false success (a deterministic 500 like the earlier `$set` bug won't fix
    // itself on retry).
    #[tokio::test]
    async fn deploy_does_not_retry_on_server_error() {
        let (base, hits) = spawn_http_mock(vec![Resp::Status(500)]).await;
        let client = get_client(true).unwrap();
        let body = serde_json::json!({});
        let url = format!("{base}/device/d/revisions/r");
        let r = post_json_retrying(&client, &url, "tok", &body, Duration::from_millis(500), 3).await;
        assert!(r.is_err(), "a 500 must not be retried into success");
        assert_eq!(hits.load(Ordering::SeqCst), 1, "must not retry a server error");
    }
}
