//! Backward-compatibility e2e tests for the legacy → new deploy-spec format.
//!
//! The `feature/services-jobs-observers` branch replaced the old flat
//! `jobs: [{type, enabled, ...}]` revision format with the
//! `services` / `observers` / `job_defs` model. Deployments created under the
//! old format must keep working after the upgrade: the server auto-converts
//! them on read, and — critically — a device already running a converted unit
//! must NOT bounce it just because the wire format changed.
//!
//! These tests feed the system a legacy-format revision (exactly what an old
//! client/server produced) and assert:
//!   1. it auto-converts into the new `services` / lifecycle model, and
//!   2. re-applying the same logical deployment in the *new* format does not
//!      re-run the unit's startup steps (i.e. the service is not restarted).
//!
//! Harness limitation: the server and device both run the *current* binary, so
//! this exercises the format round-trip rather than a literal old→new binary
//! swap. The load-bearing guarantee is the same one a real upgrade relies on:
//! a unit's per-service hash is stable across the legacy→new conversion, so the
//! device's reconcile loop sees "no change" and leaves the running unit alone.

use serde_json::Value;
use std::time::Duration;

use super::fixtures::TestSetup;
use super::helpers::{exec_shell, wait_for_result, E2EError, WaitConfig};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write a file into the CLI container via heredoc (quotes/newlines survive a
/// single `sh -c`).
async fn write_cli_file(setup: &TestSetup, path: &str, content: &str) -> Result<(), E2EError> {
    let cmd = format!(
        "mkdir -p $(dirname {path}) && cat > {path} <<'M87_E2E_EOF'\n{content}\nM87_E2E_EOF"
    );
    exec_shell(&setup.infra.cli, &cmd).await?;
    Ok(())
}

/// `m87 <device> deploy <file>` (stderr folded into stdout for diagnostics).
async fn deploy(setup: &TestSetup, file: &str) -> Result<(), E2EError> {
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy {} 2>&1", setup.device.name, file),
    )
    .await?;
    Ok(())
}

/// Parsed `m87 <device> spec --json`. `RUST_LOG=error` keeps the client's
/// tracing output off stdout so the JSON payload parses cleanly.
async fn spec_json(setup: &TestSetup) -> Result<Value, E2EError> {
    let cmd = format!("RUST_LOG=error m87 {} spec --json", setup.device.name);
    let out = exec_shell(&setup.infra.cli, &cmd).await?;
    serde_json::from_str(out.trim())
        .map_err(|e| E2EError::Parse(format!("spec --json parse failed: {e}\n--- output ---\n{out}")))
}

/// Lifecycle of a unit found specifically under the `services` array of a
/// revision JSON. Returns None until the unit shows up there — which is itself
/// the assertion that a legacy `type: service` entry was converted into a
/// *service* (not left in some legacy bucket).
fn service_lifecycle(spec: &Value, id: &str) -> Option<String> {
    spec.get("services")?
        .as_array()?
        .iter()
        .find(|u| u.get("id").and_then(|v| v.as_str()) == Some(id))?
        .get("lifecycle")?
        .as_str()
        .map(str::to_string)
}

/// How many times the unit's startup steps ran, counted via a marker file the
/// step appends to. Missing file ⇒ 0. A restart appends another line, so this
/// is a direct "did the service get bounced?" probe.
async fn startup_count(setup: &TestSetup, marker: &str) -> Result<usize, E2EError> {
    let out = exec_shell(
        &setup.infra.runtime,
        &format!("if [ -f {marker} ]; then wc -l < {marker}; else echo 0; fi"),
    )
    .await?;
    Ok(out.trim().parse().unwrap_or(0))
}

/// Poll `spec --json` until the unit appears under `services` with a lifecycle.
async fn wait_for_service_lifecycle(
    setup: &TestSetup,
    id: &str,
    description: &'static str,
) -> Result<String, E2EError> {
    wait_for_result(
        WaitConfig::with_description(description)
            .max_attempts(45)
            .interval(Duration::from_secs(2)),
        || async { Ok(spec_json(setup).await.ok().and_then(|s| service_lifecycle(&s, id))) },
    )
    .await
}

/// Legacy (pre-split) revision: flat `jobs:` array carrying `type` + `enabled`.
/// The startup step appends one line to `marker` each time it runs.
fn legacy_service_yaml(id: &str, marker: &str, enabled: bool) -> String {
    format!(
        r#"jobs:
  - id: {id}
    type: service
    enabled: {enabled}
    steps:
      - name: mark
        run: "echo tick >> {marker}"
"#
    )
}

/// The same unit expressed in the new format (`services:` + `lifecycle`).
/// Must serialize to an identical `ServiceSpec` as the legacy-converted one.
fn new_service_yaml(id: &str, marker: &str) -> String {
    format!(
        r#"services:
  - id: {id}
    lifecycle: running
    steps:
      - name: mark
        run: "echo tick >> {marker}"
"#
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A legacy-format deployment auto-converts to a running service, and
/// re-applying the same logical deployment in the new format does NOT restart
/// that service. This is the upgrade-safety guarantee: existing deployments
/// keep running across the format change.
#[tokio::test]
async fn test_legacy_deploy_converts_and_survives_format_change() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let svc_id = "e2e-bc-svc";
    let marker = "/tmp/e2e-bc-marker.txt";

    // 1. Deploy the OLD format. The server must accept `jobs:[{type,enabled}]`.
    write_cli_file(&setup, "/tmp/legacy.yml", &legacy_service_yaml(svc_id, marker, true)).await?;
    deploy(&setup, "/tmp/legacy.yml").await?;

    // 2. Auto-conversion: the unit lands under `services` as lifecycle=running.
    let lifecycle = wait_for_service_lifecycle(
        &setup,
        svc_id,
        "legacy unit converted into services[]",
    )
    .await?;
    assert_eq!(
        lifecycle, "running",
        "legacy `type: service, enabled: true` must convert to a running service"
    );

    // 3. Wait for the startup step to run (marker appears), then settle and
    //    record the baseline. A freshly-deployed single service runs startup
    //    exactly once.
    wait_for_result(
        WaitConfig::with_description("legacy service startup ran")
            .max_attempts(45)
            .interval(Duration::from_secs(2)),
        || async {
            let n = startup_count(&setup, marker).await?;
            Ok((n >= 1).then_some(n))
        },
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let baseline = startup_count(&setup, marker).await?;
    assert_eq!(
        baseline, 1,
        "startup should have run exactly once for one converted service"
    );

    // 4. Re-apply the SAME deployment in the NEW format (same id + steps).
    //    This is the literal "old deployment, now stored in the new format"
    //    transition. Because the per-service hash is unchanged, the device must
    //    treat it as no-op and leave the running unit alone.
    write_cli_file(&setup, "/tmp/new.yml", &new_service_yaml(svc_id, marker)).await?;
    deploy(&setup, "/tmp/new.yml").await?;

    // Confirm the new-format spec is active and still running.
    let lifecycle2 =
        wait_for_service_lifecycle(&setup, svc_id, "new-format spec active").await?;
    assert_eq!(lifecycle2, "running");

    // 5. Give the device ample time to receive + reconcile the re-applied
    //    revision, then assert startup never re-ran. A restart would have
    //    appended a second `tick` line.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let after = startup_count(&setup, marker).await?;
    assert_eq!(
        after, baseline,
        "service startup re-ran ({after}x, was {baseline}x) after the legacy→new format change — \
         the conversion bounced a running service"
    );

    Ok(())
}

/// A legacy unit with `enabled: false` converts to lifecycle=stopped (not
/// running) and never runs its startup steps — the disabled→stopped mapping,
/// end-to-end. Mirrors the `legacy_disabled_becomes_stopped` unit test at the
/// full deploy surface.
#[tokio::test]
async fn test_legacy_disabled_converts_to_stopped() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let svc_id = "e2e-bc-disabled";
    let marker = "/tmp/e2e-bc-disabled-marker.txt";

    write_cli_file(
        &setup,
        "/tmp/disabled.yml",
        &legacy_service_yaml(svc_id, marker, false),
    )
    .await?;
    deploy(&setup, "/tmp/disabled.yml").await?;

    let lifecycle = wait_for_service_lifecycle(
        &setup,
        svc_id,
        "disabled legacy unit present in spec",
    )
    .await?;
    assert_eq!(
        lifecycle, "stopped",
        "legacy `enabled: false` must convert to lifecycle=stopped"
    );

    // A stopped unit must never run its startup steps.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let count = startup_count(&setup, marker).await?;
    assert_eq!(count, 0, "stopped service must not run startup steps");

    Ok(())
}
