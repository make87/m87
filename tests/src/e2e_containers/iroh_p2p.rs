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
use super::helpers::{E2EError, WaitConfig, exec_background, exec_shell, wait_for_result};

/// Poll `exec` until it reports a direct iroh connection. Used as a readiness
/// gate: once a command takes the iroh path, the device's addr is advertised
/// and subsequent commands (e.g. forward) will take it too.
async fn wait_until_iroh_ready(setup: &TestSetup) -> Result<(), E2EError> {
    let dev = setup.device.name.clone();
    wait_for_result(
        WaitConfig::with_description("iroh data path to become ready"),
        || {
            let cli = &setup.infra.cli;
            let cmd = format!("m87 {dev} exec -- true 2>&1");
            async move {
                let out = exec_shell(cli, &cmd).await?;
                Ok(out.contains("via iroh").then_some(()))
            }
        },
    )
    .await
}

/// Once the runtime's control tunnel is up, the device heartbeats its iroh
/// `EndpointAddr` and the server exposes it. Drive that path end-to-end.
#[tokio::test]
async fn test_device_advertises_iroh_addr_via_heartbeat() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // 1. The iroh-addr endpoint is keyed by the device ObjectId, not the
    //    short id, so resolve it from the device list. The CLI is run with
    //    `2>&1`, so debug log lines are prepended to the JSON — slice from the
    //    first `{` (the log lines contain no braces).
    let list_out = setup.m87_cmd("devices list --json").await?;
    let json_str = list_out
        .find('{')
        .map(|i| &list_out[i..])
        .ok_or_else(|| {
            E2EError::Exec(format!("no JSON object in `devices list --json` output:\n{list_out}"))
        })?;
    let parsed: serde_json::Value = serde_json::from_str(json_str).map_err(|e| {
        E2EError::Exec(format!("`devices list --json` was not valid JSON: {e}\n{json_str}"))
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
                "device {} not found in `devices list --json`: {json_str}",
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

/// End-to-end data-plane test of the iroh layer and its relay fallback.
///
/// With the CLI dialer wired up, `m87 <device> exec` prefers a direct iroh
/// connection. The CLI and runtime share the e2e Docker network, so the direct
/// path should establish. The `M87_DISABLE_IROH` kill switch then forces the
/// server relay, proving the fallback path. Both must run the command
/// successfully. The chosen transport is asserted from `m87_client`'s debug log
/// (`RUST_LOG=m87_client=debug` in the CLI container), captured via `2>&1`.
#[tokio::test]
async fn test_exec_over_iroh_and_relay_fallback() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let dev = setup.device.name.clone();

    // (a) Default path: iroh-preferred. Poll, because the device's iroh addr is
    //     advertised on a heartbeat and may lag the control tunnel coming up;
    //     until it lands, exec transparently uses the relay. We require it to
    //     converge on the direct iroh path.
    let iroh_out = wait_for_result(
        WaitConfig::with_description("exec to use a direct iroh connection"),
        || {
            let cli = &setup.infra.cli;
            let cmd = format!("m87 {dev} exec -- echo iroh-hello 2>&1");
            async move {
                let out = exec_shell(cli, &cmd).await?;
                if !out.contains("iroh-hello") {
                    return Err(E2EError::Exec(format!("exec did not run command:\n{out}")));
                }
                // Converged once the direct path is chosen; otherwise keep polling.
                Ok(out.contains("via iroh").then_some(out))
            }
        },
    )
    .await?;
    assert!(
        iroh_out.contains("iroh-hello") && iroh_out.contains("via iroh"),
        "exec should run over a direct iroh connection:\n{iroh_out}"
    );

    // (b) Kill switch forces the server relay. The same command must still work,
    //     and must NOT take the iroh path. This is deterministic — single shot.
    let relay_out = exec_shell(
        &setup.infra.cli,
        &format!("M87_DISABLE_IROH=1 m87 {dev} exec -- echo relay-hello 2>&1"),
    )
    .await?;
    assert!(
        relay_out.contains("relay-hello"),
        "exec over the relay (iroh disabled) must succeed:\n{relay_out}"
    );
    assert!(
        relay_out.contains("via server relay"),
        "with M87_DISABLE_IROH set, exec must fall back to the relay:\n{relay_out}"
    );
    assert!(
        !relay_out.contains("via iroh"),
        "M87_DISABLE_IROH must suppress the iroh attempt entirely:\n{relay_out}"
    );

    tracing::info!("iroh data-plane + relay fallback test passed");
    Ok(())
}

/// UDP forwarding over a direct iroh connection. UDP rides on QUIC datagrams
/// (per-connection on iroh, vs. multiplexed over the relay), so it has its own
/// device-side plumbing worth exercising end-to-end.
///
/// Sends a datagram through `m87 <device> forward <local>:<remote>/udp` and
/// asserts it arrives at a `nc -u` listener on the device, while the forward
/// log confirms it took the direct iroh path.
#[tokio::test]
async fn test_udp_forward_over_iroh() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let dev = setup.device.name.clone();

    // Make sure iroh is the live path before forwarding (else it'd race into
    // the relay fallback while the device's addr is still propagating).
    wait_until_iroh_ready(&setup).await?;

    // Device: a UDP listener that records the first datagram it receives.
    // `-W 1` makes nc exit after one packet, flushing its (block-buffered)
    // stdout. `exec_background` already redirects stdout to the log file, so we
    // point that at the recv file directly and add no redirect of our own — an
    // extra `> file` in the command would be overridden and swallow the payload.
    exec_background(
        &setup.infra.runtime,
        "nc -u -l -p 9091 -W 1",
        "/tmp/udp_recv.txt",
    )
    .await
    .map_err(|e| E2EError::Exec(e.to_string()))?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // CLI: forward local udp/9090 → device 127.0.0.1:9091.
    exec_background(
        &setup.infra.cli,
        &format!("m87 {dev} forward 9090:127.0.0.1:9091/udp"),
        "/tmp/udp_forward.log",
    )
    .await
    .map_err(|e| E2EError::Exec(e.to_string()))?;

    // Wait for the forward to come up, and confirm it chose the iroh transport.
    let fwd_log = wait_for_result(
        WaitConfig::with_description("udp forward to establish"),
        || {
            let cli = &setup.infra.cli;
            async move {
                let log = exec_shell(cli, "cat /tmp/udp_forward.log 2>/dev/null || true").await?;
                Ok(log.contains("UDP forward:").then_some(log))
            }
        },
    )
    .await?;
    assert!(
        fwd_log.contains("via iroh"),
        "UDP forward should run over a direct iroh connection:\n{fwd_log}"
    );

    // Send the datagram a few times through the local forwarded port, polling
    // the device listener for it. UDP is unreliable and the first packet can be
    // lost while the path warms up, so we retry rather than send once.
    let marker = "UDP_OVER_IROH_4242";
    wait_for_result(
        WaitConfig::with_description("udp datagram to arrive on the device over iroh"),
        || {
            let cli = &setup.infra.cli;
            let rt = &setup.infra.runtime;
            async move {
                exec_shell(cli, &format!("printf '{marker}' | nc -u -w1 127.0.0.1 9090")).await?;
                let got = exec_shell(rt, "cat /tmp/udp_recv.txt 2>/dev/null || true").await?;
                Ok(got.contains(marker).then_some(()))
            }
        },
    )
    .await?;

    tracing::info!("UDP-over-iroh forward test passed");
    Ok(())
}
