//! E2E coverage for the two device-access-control fixes.
//!
//! Critical 1 — `short_id` collisions let an attacker hijack a victim's relay
//! tunnel. Enrollment must reject a device whose `short_id` collides with an
//! existing one.
//!
//! Critical 2 — `POST /device/{id}` only required Editor, yet let the payload
//! rewrite `owner_scope` / `allowed_scopes`. Rewriting the ACL must require
//! Admin, so an Editor cannot seize the device or share it with new scopes.
//!
//! Both tests drive the server's REST API directly. That is the only way to
//! exercise the exact fields (`device_id`, `owner_scope`) and a non-admin
//! Editor principal — the CLI in the harness authenticates with the admin key,
//! which bypasses all scope/role checks.

use super::containers::E2EInfra;
use super::helpers::E2EError;
use mongodb::bson::{doc, oid::ObjectId, Document};
use mongodb::Client;
use serde_json::json;
use std::time::Duration;

/// Same admin key the server container boots with (see `containers.rs`).
const ADMIN_KEY: &str = "e2e-admin-key";
const MONGO_DB: &str = "e2e-tests";

/// Reproduce `m87_shared::device::short_device_id` on the host: first 6 hex
/// chars of SHA256(device_id). Kept in lockstep with the canonical function
/// (which has its own unit test); if that ever changes, the collision search
/// below would stop matching and should be updated alongside it.
fn short_device_id(device_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(device_id.as_bytes());
    hex::encode(hasher.finalize())[..6].to_string()
}

/// Find two distinct, valid 24-hex device ids that share a `short_id`.
///
/// `short_id` is only 24 bits, so a collision is trivially discoverable by
/// scanning sequential ids — which is exactly why enrollment must enforce
/// uniqueness. Returns `(first_id, colliding_id, shared_short_id)`.
fn find_colliding_ids() -> (String, String, String) {
    use std::collections::HashMap;
    let mut seen: HashMap<String, String> = HashMap::new();
    for i in 0..2_000_000u64 {
        let device_id = format!("{:024x}", i);
        let short = short_device_id(&device_id);
        if let Some(prev) = seen.get(&short) {
            return (prev.clone(), device_id, short);
        }
        seen.insert(short, device_id);
    }
    panic!("no short_id collision found within scan budget (24-bit space)");
}

/// A fresh, valid 24-hex device id (no collision required).
fn fresh_device_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..24].to_string()
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .http1_only()
        .build()
        .expect("build reqwest client")
}

async fn mongo_client(infra: &E2EInfra) -> Result<Client, E2EError> {
    let port = infra
        .mongo
        .get_host_port_ipv4(27017)
        .await
        .map_err(|e| E2EError::Setup(e.to_string()))?;
    Client::with_uri_str(format!("mongodb://localhost:{port}"))
        .await
        .map_err(|e| E2EError::Setup(e.to_string()))
}

fn devices(client: &Client) -> mongodb::Collection<Document> {
    client.database(MONGO_DB).collection::<Document>("devices")
}

/// Run the full enrollment handshake against the server REST API for a chosen
/// `device_id` / `owner_scope`. Returns `Ok(Some(api_key))` if the device was
/// created (the API key is Editor-scoped on `device:<device_id>`), or
/// `Ok(None)` if the server refused to create it. Errors are reserved for
/// genuine transport/setup failures.
async fn enroll(
    http: &reqwest::Client,
    base_url: &str,
    device_id: &str,
    owner_scope: &str,
) -> Result<Option<String>, E2EError> {
    let device_info = json!({
        "hostname": format!("host-{}", &device_id[..8]),
        "username": "e2e",
        "operating_system": "linux",
        "architecture": "x86_64",
        "cpu_name": "e2e-cpu",
    });

    // 1) Unauthenticated enrollment request → returns the request UUID.
    let req_resp = http
        .post(format!("{base_url}/auth/request"))
        .json(&json!({
            "device_info": device_info,
            "owner_scope": owner_scope,
            "device_id": device_id,
        }))
        .send()
        .await
        .map_err(|e| E2EError::Exec(format!("POST /auth/request: {e}")))?;
    if !req_resp.status().is_success() {
        return Err(E2EError::Exec(format!(
            "enrollment request rejected: {}",
            req_resp.status()
        )));
    }
    let request_id: String = req_resp
        .json()
        .await
        .map_err(|e| E2EError::Parse(format!("request_id: {e}")))?;

    // 2) Admin approves the request (bearer = admin key).
    let approve_resp = http
        .post(format!("{base_url}/auth/request/approve"))
        .bearer_auth(ADMIN_KEY)
        .json(&json!({ "accept": true, "request_id": request_id }))
        .send()
        .await
        .map_err(|e| E2EError::Exec(format!("POST /auth/request/approve: {e}")))?;
    if !approve_resp.status().is_success() {
        return Err(E2EError::Exec(format!(
            "approve rejected: {}",
            approve_resp.status()
        )));
    }

    // 3) Poll check: once approved the server creates the device + key. A
    //    non-success status here is the Critical-1 rejection (create_from
    //    refuses the colliding short_id) → report as "not created".
    for _ in 0..15 {
        let check = http
            .post(format!("{base_url}/auth/request/check"))
            .json(&json!({ "request_id": request_id }))
            .send()
            .await
            .map_err(|e| E2EError::Exec(format!("POST /auth/request/check: {e}")))?;

        if !check.status().is_success() {
            return Ok(None);
        }

        let body: serde_json::Value = check
            .json()
            .await
            .map_err(|e| E2EError::Parse(format!("check response: {e}")))?;
        match body.get("state").and_then(|s| s.as_str()) {
            Some("approved") => {
                return Ok(body
                    .get("api_key")
                    .and_then(|k| k.as_str())
                    .map(|s| s.to_string()));
            }
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }

    Ok(None)
}

/// Critical 1: a second device whose `short_id` collides with an existing one
/// must be refused at enrollment, or it could hijack the incumbent's tunnel.
#[tokio::test]
async fn colliding_short_id_enrollment_is_rejected() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    let port = infra
        .server
        .get_host_port_ipv4(8084)
        .await
        .map_err(|e| E2EError::Setup(e.to_string()))?;
    let base_url = format!("https://localhost:{port}");
    let http = http_client();

    let (victim_id, attacker_id, short) = find_colliding_ids();
    assert_ne!(victim_id, attacker_id, "collision must be two distinct ids");
    tracing::info!("victim_id={victim_id} attacker_id={attacker_id} short_id={short}");

    // Victim enrolls first and must succeed.
    let victim_key = enroll(&http, &base_url, &victim_id, "user:victim@test.local").await?;
    assert!(
        victim_key.is_some(),
        "victim enrollment should succeed and mint an API key"
    );

    // Attacker (different owner) enrolls a device whose short_id collides. The
    // server must refuse to create it.
    let attacker_key = enroll(&http, &base_url, &attacker_id, "user:attacker@evil.com").await?;
    assert!(
        attacker_key.is_none(),
        "colliding-short_id enrollment must be rejected (would allow tunnel hijack)"
    );

    // Ground truth: exactly one device owns the shared short_id in the DB.
    let client = mongo_client(&infra).await?;
    let count = devices(&client)
        .count_documents(doc! { "short_id": &short })
        .await
        .map_err(|e| E2EError::Setup(format!("count devices: {e}")))?;
    assert_eq!(
        count, 1,
        "exactly one device may hold a short_id; found {count} (collision not enforced)"
    );

    tracing::info!("short_id collision enrollment correctly rejected");
    Ok(())
}

/// Critical 2: an Editor (a device's own enrollment key is Editor-scoped) may
/// update benign fields but must NOT rewrite `owner_scope` or `allowed_scopes`.
#[tokio::test]
async fn editor_cannot_rewrite_ownership_or_acl() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    let port = infra
        .server
        .get_host_port_ipv4(8084)
        .await
        .map_err(|e| E2EError::Setup(e.to_string()))?;
    let base_url = format!("https://localhost:{port}");
    let http = http_client();

    // Enroll a device; its returned API key is Editor on `device:<device_id>`.
    let device_id = fresh_device_id();
    let owner_scope = "user:victim@test.local";
    let editor_key = enroll(&http, &base_url, &device_id, owner_scope)
        .await?
        .ok_or_else(|| E2EError::Setup("enrollment did not return an API key".into()))?;

    let device_url = format!("{base_url}/device/{device_id}");

    // Sanity: the Editor key CAN update a benign field. This proves the later
    // rejection is about the privileged fields, not a broken key.
    let benign = http
        .post(&device_url)
        .bearer_auth(&editor_key)
        .json(&json!({ "target_version": "v9.9.9-e2e" }))
        .send()
        .await
        .map_err(|e| E2EError::Exec(format!("benign update: {e}")))?;
    assert!(
        benign.status().is_success(),
        "Editor should be able to update target_version, got {}",
        benign.status()
    );

    // Attack 1: try to seize ownership. Must be forbidden.
    let seize = http
        .post(&device_url)
        .bearer_auth(&editor_key)
        .json(&json!({ "owner_scope": "user:attacker@evil.com" }))
        .send()
        .await
        .map_err(|e| E2EError::Exec(format!("owner_scope update: {e}")))?;
    assert_eq!(
        seize.status().as_u16(),
        403,
        "Editor rewriting owner_scope must be forbidden, got {}",
        seize.status()
    );

    // Attack 2: try to share the device with a new scope. Must be forbidden.
    let share = http
        .post(&device_url)
        .bearer_auth(&editor_key)
        .json(&json!({ "allowed_scopes": ["org:evil", format!("device:{device_id}")] }))
        .send()
        .await
        .map_err(|e| E2EError::Exec(format!("allowed_scopes update: {e}")))?;
    assert_eq!(
        share.status().as_u16(),
        403,
        "Editor rewriting allowed_scopes must be forbidden, got {}",
        share.status()
    );

    // Ground truth in Mongo: ownership unchanged, evil scope never added.
    let client = mongo_client(&infra).await?;
    let oid = ObjectId::parse_str(&device_id)
        .map_err(|e| E2EError::Setup(format!("device id not an ObjectId: {e}")))?;
    let device = devices(&client)
        .find_one(doc! { "_id": oid })
        .await
        .map_err(|e| E2EError::Setup(format!("find device: {e}")))?
        .ok_or_else(|| E2EError::Setup("device missing after updates".into()))?;

    assert_eq!(
        device.get_str("owner_scope").unwrap_or_default(),
        owner_scope,
        "owner_scope must be unchanged after the forbidden update"
    );
    let allowed: Vec<String> = device
        .get_array("allowed_scopes")
        .map(|a| {
            a.iter()
                .filter_map(|b| b.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        !allowed.iter().any(|s| s == "org:evil"),
        "allowed_scopes must not contain the injected scope; have: {allowed:?}"
    );

    tracing::info!("Editor correctly blocked from rewriting ownership/ACL");
    Ok(())
}
