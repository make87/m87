//! iroh P2P signalling e2e tests.
//!
//! iroh runs as an opportunistic *direct* connection layer alongside the
//! existing quinn server relay. For a CLI to ever attempt a direct connection
//! it must first learn the device's iroh `EndpointAddr`, which the device
//! advertises in every heartbeat and the server caches and re-serves at
//! `GET /device/{id}/iroh-addr`.
//!
//! The unit tests cover the serialisation (`m87-shared`) and the relay-state
//! cache (`m87-server`) in isolation. This e2e test drives the whole
//! signalling path through real containers:
//!
//!   runtime control tunnel → heartbeat(iroh_node_addr) → server relay_state
//!     → GET /device/{id}/iroh-addr
//!
//! and asserts the advertised address is retrievable. It does not assert that
//! a direct hole-punched connection succeeds (that depends on the CI network
//! topology and the n0 relay); the relay fallback path is what the other e2e
//! tests already exercise.

use super::E2EInfra;
use super::fixtures::TestSetup;
use super::helpers::{E2EError, WaitConfig, wait_for_result};

/// Once the runtime's control tunnel is up, the device heartbeats its iroh
/// `EndpointAddr` and the server exposes it. Drive that path end-to-end.
#[tokio::test]
async fn test_device_advertises_iroh_addr_via_heartbeat() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // 1. The iroh-addr endpoint is keyed by the device ObjectId, not the
    //    short id, so resolve it from the device list.
    let list_json = setup.m87_cmd("devices list --json").await?;
    let parsed: serde_json::Value = serde_json::from_str(&list_json).map_err(|e| {
        E2EError::Exec(format!("`devices list --json` was not valid JSON: {e}\n{list_json}"))
    })?;
    let device_id = parsed["devices"]
        .as_array()
        .and_then(|devices| {
            devices
                .iter()
                .find(|d| d["short_id"] == setup.device.short_id)
        })
        .and_then(|d| d["id"].as_str())
        .ok_or_else(|| {
            E2EError::Exec(format!(
                "device {} not found in `devices list --json`: {list_json}",
                setup.device.short_id
            ))
        })?
        .to_string();

    // 2. Host-side HTTPS client. The e2e server uses a self-signed cert, same
    //    as the readiness probe in `wait_for_server`.
    let base = setup
        .infra
        .server_base_url()
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;
    let url = format!("{base}/device/{device_id}/iroh-addr");
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .http1_only()
        .build()
        .map_err(|e| E2EError::Exec(e.to_string()))?;

    // 3. Poll until the device has heartbeated its addr. A 404 ("iroh not
    //    available") just means no heartbeat has carried it yet — keep waiting.
    //    The runtime waits up to 10s for iroh relay registration at startup
    //    before the first heartbeat, so this can take ~15s.
    let addr_json = wait_for_result(
        WaitConfig::with_description("device to advertise its iroh addr"),
        || async {
            let resp = client
                .get(&url)
                .bearer_auth(E2EInfra::admin_key())
                .timeout(std::time::Duration::from_secs(5))
                .send()
                .await
                .map_err(|e| E2EError::Exec(e.to_string()))?;

            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !resp.status().is_success() {
                return Err(E2EError::Exec(format!(
                    "iroh-addr endpoint returned HTTP {}",
                    resp.status()
                )));
            }

            let text = resp
                .text()
                .await
                .map_err(|e| E2EError::Exec(e.to_string()))?;
            let body: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| E2EError::Exec(format!("iroh-addr body not JSON: {e}\n{text}")))?;
            Ok(body["iroh_node_addr"].as_str().map(str::to_string))
        },
    )
    .await?;

    // 4. The stored value is the device's serialised iroh `EndpointAddr`. It
    //    must be a non-empty JSON object (carrying at least the node id) — that
    //    proves the device produced a real addr, the server cached it, and the
    //    API served it back. We avoid asserting an exact field name so the test
    //    survives iroh version bumps.
    assert!(
        !addr_json.is_empty(),
        "stored iroh addr should not be empty"
    );
    let addr: serde_json::Value = serde_json::from_str(&addr_json).map_err(|e| {
        E2EError::Exec(format!("stored iroh addr is not valid JSON: {e}\n{addr_json}"))
    })?;
    assert!(
        addr.is_object(),
        "iroh addr should deserialise to a JSON object, got: {addr_json}"
    );

    tracing::info!("iroh addr signalling test passed: {addr_json}");
    Ok(())
}
