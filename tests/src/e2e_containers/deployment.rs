//! Deployment tests for the services / observers / jobs split introduced in
//! the `feature/services-jobs-observers` branch.
//!
//! These exercise the user-facing CLI surface (`m87 <dev> deploy`, `service
//! pause/resume`, `job trigger`, `rollback`, …) and assert against the JSON
//! output of the matching inspect commands (`spec --json`, `units --json`,
//! `job status --json`, `deployment list --json`). Assertions look at parsed
//! JSON, not pretty-printed table text, so the table layout can change without
//! breaking the suite.

use serde_json::Value;
use std::time::Duration;

use super::fixtures::TestSetup;
use super::helpers::{exec_shell, wait_for_result, E2EError, WaitConfig};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Drop a file into the CLI container at `path` with the given content.
async fn write_cli_file(setup: &TestSetup, path: &str, content: &str) -> Result<(), E2EError> {
    // Write via a heredoc so embedded quotes/newlines survive a single
    // `sh -c` invocation.
    let cmd = format!(
        "mkdir -p $(dirname {path}) && cat > {path} <<'M87_E2E_EOF'\n{content}\nM87_E2E_EOF"
    );
    exec_shell(&setup.infra.cli, &cmd).await?;
    Ok(())
}

/// Run an `m87 <device> <args>` command and parse the stdout as JSON.
///
/// `RUST_LOG=error` silences `m87-client`'s tracing output, which otherwise
/// writes to stdout (default for `tracing_subscriber::fmt::layer()`) and
/// would mix with the JSON payload. The CLI container's baseline RUST_LOG
/// is `info,m87_client=debug` — too noisy to parse around. We also avoid
/// `cli_exec` because it appends `--verbose`, which raises the level
/// further.
async fn device_json(setup: &TestSetup, args: &str) -> Result<Value, E2EError> {
    let cmd = format!("RUST_LOG=error m87 {} {}", setup.device.name, args);
    let out = exec_shell(&setup.infra.cli, &cmd).await?;
    serde_json::from_str(out.trim()).map_err(|e| {
        E2EError::Parse(format!(
            "failed to parse JSON from `m87 {}`: {e}\n--- output ---\n{out}",
            args
        ))
    })
}

/// Poll a device JSON command until the predicate returns `Some(value)`.
async fn wait_for_device_json<F, T>(
    setup: &TestSetup,
    args: &str,
    description: &'static str,
    pred: F,
) -> Result<T, E2EError>
where
    F: Fn(&Value) -> Option<T>,
{
    wait_for_result(
        WaitConfig::with_description(description)
            .max_attempts(45)
            .interval(Duration::from_secs(2)),
        || async {
            let v = device_json(setup, args).await?;
            Ok(pred(&v))
        },
    )
    .await
}

/// Lifecycle string in the JSON output of a service / observer / job_def from
/// `units --json` or `spec --json`. Returns None if the unit isn't present.
fn unit_lifecycle<'a>(revision: &'a Value, unit_id: &str) -> Option<&'a str> {
    for section in ["services", "observers", "jobs"] {
        if let Some(arr) = revision.get(section).and_then(|v| v.as_array()) {
            for unit in arr {
                if unit.get("id").and_then(|v| v.as_str()) == Some(unit_id) {
                    return unit.get("lifecycle").and_then(|v| v.as_str());
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Test fixtures – minimal YAML specs that don't depend on any external state.
// ---------------------------------------------------------------------------

/// Single-step service that always succeeds. `true` is a POSIX builtin so it
/// works in the runtime container's alpine shell with no extra binaries.
fn service_yaml(id: &str) -> String {
    format!(
        r#"id: {id}
steps:
  - name: noop
    run: "true"
"#
    )
}

/// Single-step job wrapped in a full revision document. A bare ServiceSpec
/// and a bare JobDef are structurally indistinguishable (both have just
/// `id` + `steps`), so we have to disambiguate via the `job_defs:` section
/// of a DeploymentRevision rather than relying on auto-detect.
fn job_revision_yaml(id: &str) -> String {
    format!(
        r#"job_defs:
  - id: {id}
    steps:
      - name: noop
        run: "true"
"#
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `m87 <dev> deploy` adds a service that appears in the active revision.
///
/// Asserts on the parsed `spec --json` output — verifies the CLI→server wire
/// for `add_service` plus serde round-trip of `ServiceSpec`.
#[tokio::test]
async fn test_deploy_service_lands_in_spec() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let svc_id = "e2e-deploy-svc";
    write_cli_file(&setup, "/tmp/svc.yml", &service_yaml(svc_id)).await?;

    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/svc.yml 2>&1", setup.device.name),
    )
    .await?;

    let spec = wait_for_device_json(&setup, "spec --json", "service in spec", |spec| {
        unit_lifecycle(spec, svc_id).map(|lc| (spec.clone(), lc.to_string()))
    })
    .await?;

    let (_spec_value, lifecycle) = spec;
    assert_eq!(
        lifecycle, "running",
        "newly-deployed service should default to lifecycle=running"
    );

    Ok(())
}

/// `m87 <dev> stop <id>` then `start <id>` round-trips through the lifecycle
/// queue → heartbeat → device state → status snapshot.
///
/// Asserts via `health --json` where `runs[].enabled` flips false on stop and
/// back to true on start. The server's stored `DeploymentRevision.lifecycle`
/// is NOT updated by lifecycle commands (only the device's local state is),
/// so we read the device-reported snapshot rather than the spec.
///
/// TODO: pause/resume — observable surface for "paused vs running" is not
/// directly carried in `RunStatus`; needs a richer snapshot field or a
/// device-side log-tail check.
#[tokio::test]
async fn test_service_stop_start_via_snapshot() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let svc_id = "e2e-lifecycle-svc";
    write_cli_file(&setup, "/tmp/svc.yml", &service_yaml(svc_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/svc.yml 2>&1", setup.device.name),
    )
    .await?;

    // Wait for the device to report the unit at all.
    wait_for_device_json(&setup, "health --json", "unit visible in snapshot", |s| {
        run_enabled(s, svc_id).map(|_| ())
    })
    .await?;

    // Stop.
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} stop {} 2>&1", setup.device.name, svc_id),
    )
    .await?;
    wait_for_device_json(&setup, "health --json", "unit enabled=false", |s| {
        (run_enabled(s, svc_id) == Some(false)).then(|| ())
    })
    .await?;

    // Start.
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} start {} 2>&1", setup.device.name, svc_id),
    )
    .await?;
    wait_for_device_json(&setup, "health --json", "unit enabled=true", |s| {
        (run_enabled(s, svc_id) == Some(true)).then(|| ())
    })
    .await?;

    Ok(())
}

/// Look up `runs[].enabled` for a given unit id in a `DeploymentStatusSnapshot`
/// JSON. Returns None if the unit isn't reported yet.
fn run_enabled(snapshot: &Value, unit_id: &str) -> Option<bool> {
    snapshot
        .get("runs")?
        .as_array()?
        .iter()
        .find(|r| r.get("run_id").and_then(|v| v.as_str()) == Some(unit_id))
        .and_then(|r| r.get("enabled").and_then(|v| v.as_bool()))
}

/// `m87 <dev> job trigger <id>` creates a JobRun whose status eventually
/// reaches Success.
///
/// Asserts via `job trigger --json` (typed JobRun: run_id, job_def_id, status)
/// and `job status --json` polling. Validates the new job_runs collection +
/// heartbeat-queue path end-to-end.
#[tokio::test]
async fn test_job_trigger_completes() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let job_id = "e2e-trigger-job";
    write_cli_file(&setup, "/tmp/job.yml", &job_revision_yaml(job_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/job.yml 2>&1", setup.device.name),
    )
    .await?;

    // Wait for the JobDef to show in `job defs --json` before triggering.
    wait_for_device_json(&setup, "job defs --json", "job def visible", |v| {
        v.as_array()?.iter().find_map(|j| {
            (j.get("id").and_then(|x| x.as_str()) == Some(job_id)).then(|| ())
        })
    })
    .await?;

    let trigger_out = device_json(&setup, &format!("job trigger {} --json", job_id)).await?;
    let run_id = trigger_out
        .get("run_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| E2EError::Parse(format!("no run_id in trigger output: {trigger_out}")))?
        .to_string();
    assert_eq!(
        trigger_out.get("job_def_id").and_then(|v| v.as_str()),
        Some(job_id),
        "trigger response job_def_id must match"
    );

    let final_status = wait_for_device_json(
        &setup,
        format!("job status {} --json", run_id).leak_static(),
        "job run terminal status",
        |v| match v.get("status").and_then(|s| s.as_str()) {
            Some(s @ ("success" | "failed")) => Some(s.to_string()),
            _ => None,
        },
    )
    .await?;

    assert_eq!(final_status, "success", "job run should reach Success");
    Ok(())
}

/// Successive `m87 <dev> deploy` calls on the same device accumulate into
/// the *same* spec — they do NOT create separate revisions to flip between.
///
/// This is the load-bearing guarantee of the single-revision model. If the
/// server reverts to the old multi-revision behaviour (each deploy creates
/// a new "active" revision), the second deploy would silently shadow the
/// first instead of adding to it, and this test catches it.
#[tokio::test]
async fn test_deploys_accumulate_in_single_spec() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let svc_a = "e2e-accum-a";
    let svc_b = "e2e-accum-b";

    // First deploy.
    write_cli_file(&setup, "/tmp/a.yml", &service_yaml(svc_a)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/a.yml 2>&1", setup.device.name),
    )
    .await?;
    wait_for_device_json(&setup, "spec --json", "service-a in spec", |s| {
        unit_lifecycle(s, svc_a).map(|_| ())
    })
    .await?;

    // Capture the revision id; it MUST stay the same after the second deploy.
    let spec_after_a = device_json(&setup, "spec --json").await?;
    let rev_id_after_a = spec_after_a
        .get("id")
        .and_then(|v| v.as_str())
        .expect("spec has id")
        .to_string();

    // Second deploy of a different unit (no --replace-all).
    write_cli_file(&setup, "/tmp/b.yml", &service_yaml(svc_b)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/b.yml 2>&1", setup.device.name),
    )
    .await?;
    wait_for_device_json(&setup, "spec --json", "both services in spec", |s| {
        let has_a = unit_lifecycle(s, svc_a).is_some();
        let has_b = unit_lifecycle(s, svc_b).is_some();
        (has_a && has_b).then(|| ())
    })
    .await?;

    // The revision id must be unchanged — there's only ever one revision.
    let spec_after_b = device_json(&setup, "spec --json").await?;
    let rev_id_after_b = spec_after_b
        .get("id")
        .and_then(|v| v.as_str())
        .expect("spec has id");
    assert_eq!(
        rev_id_after_b, rev_id_after_a,
        "second deploy must not create a new revision; expected revision id {rev_id_after_a} \
         but got {rev_id_after_b}. This indicates the multi-revision flip is back."
    );

    // `deployment list --json` would have shown 2 revisions under the old
    // model. The command is gone, so we instead probe via `spec --json` which
    // returns *the* revision — there can only be one.
    Ok(())
}

/// `m87 <dev> deploy --replace-all` atomically swaps the device's spec to
/// match the given revision file. Verifies the "revert by redeploy" flow
/// that replaced the old rollback command under the single-revision model.
#[tokio::test]
async fn test_deploy_replace_all_swaps_spec() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    // First deploy: service-a.
    let svc_a = "e2e-replace-a";
    write_cli_file(&setup, "/tmp/a.yml", &service_yaml(svc_a)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/a.yml 2>&1", setup.device.name),
    )
    .await?;
    wait_for_device_json(&setup, "spec --json", "service-a present", |s| {
        unit_lifecycle(s, svc_a).map(|_| ())
    })
    .await?;

    // Replace-all with a new revision containing only service-b.
    let svc_b = "e2e-replace-b";
    let revision_yaml = format!(
        r#"services:
  - id: {svc_b}
    steps:
      - name: noop
        run: "true"
"#
    );
    write_cli_file(&setup, "/tmp/b.yml", &revision_yaml).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/b.yml --replace-all 2>&1", setup.device.name),
    )
    .await?;

    wait_for_device_json(&setup, "spec --json", "spec swapped to service-b", |s| {
        let has_a = unit_lifecycle(s, svc_a).is_some();
        let has_b = unit_lifecycle(s, svc_b).is_some();
        (!has_a && has_b).then(|| ())
    })
    .await?;

    Ok(())
}

/// True if a job with `id` is present under the canonical `job_defs` array of a
/// `spec --json` revision.
fn job_in_defs(revision: &Value, id: &str) -> bool {
    revision
        .get("job_defs")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .any(|u| u.get("id").and_then(|v| v.as_str()) == Some(id))
        })
        .unwrap_or(false)
}

/// `m87 <dev> deploy <jobdef.yml> --type job` must land the job in the spec's
/// `job_defs` array — the `add_job` path, which is API/UI-only until now (a bare
/// unit YAML always resolves to a service). Exercises CLI -> server `add_job` ->
/// Mongo -> read-back end to end: if `add_job` writes to the wrong field, the
/// deploy 500s on the audit-log read-back and the job never appears.
#[tokio::test]
async fn test_deploy_job_via_cli_lands_in_job_defs() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let job_id = "e2e-migrate-job";
    let job_yaml = format!(
        r#"id: {job_id}
steps:
  - name: run
    run: "true"
"#
    );
    write_cli_file(&setup, "/tmp/job.yml", &job_yaml).await?;
    let out = exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/job.yml --type job 2>&1", setup.device.name),
    )
    .await?;
    assert!(
        !out.contains("failed to add run spec") && !out.contains("500"),
        "deploy --type job must not error (add_job wrote the wrong field?): {out}"
    );

    wait_for_device_json(&setup, "spec --json", "job in job_defs", |s| {
        job_in_defs(s, job_id).then(|| ())
    })
    .await?;

    Ok(())
}

/// A device with MORE THAN ONE `active: true` revision (which a check-then-
/// insert race in create_deployment can produce) must still resolve to ONE
/// stable revision. An unsorted `find_one({active:true})` returns an arbitrary
/// one per call, so the deployment hash flaps, the device never reads
/// `up_to_date`, and the server sends a new target on every heartbeat — a
/// heartbeat/reconcile storm. The server must resolve deterministically to the
/// newest active revision.
#[tokio::test]
async fn test_multiple_active_revisions_resolve_to_newest_deterministically() -> Result<(), E2EError>
{
    use mongodb::bson::{doc, oid::ObjectId, Document};
    use mongodb::Client;

    let setup = TestSetup::init().await?;

    // Connect to the same Mongo the server uses (DB "e2e-tests").
    let port = setup
        .infra
        .mongo
        .get_host_port_ipv4(27017)
        .await
        .map_err(|e| E2EError::Setup(e.to_string()))?;
    let client = Client::with_uri_str(format!("mongodb://localhost:{port}"))
        .await
        .map_err(|e| E2EError::Setup(e.to_string()))?;
    let db = client.database("e2e-tests");

    // Look up the device's Mongo _id by its short_id.
    let dev: Document = db
        .collection::<Document>("devices")
        .find_one(doc! { "short_id": &setup.device.short_id })
        .await
        .map_err(|e| E2EError::Setup(e.to_string()))?
        .ok_or_else(|| E2EError::Setup("device not found in mongo".into()))?;
    let device_oid: ObjectId = dev
        .get_object_id("_id")
        .map_err(|e| E2EError::Setup(e.to_string()))?;

    // Inject two active revisions — an older (index 0) and a newer (index 1).
    let rev_old = format!("rev-old-{}", setup.device.short_id);
    let rev_new = format!("rev-new-{}", setup.device.short_id);
    let revisions = db.collection::<Document>("deploy_revisions");
    for (rid, index) in [(&rev_old, 0i32), (&rev_new, 1i32)] {
        revisions
            .insert_one(doc! {
                "revision": { "id": rid },
                "device_id": device_oid,
                "active": true,
                "dirty": false,
                "index": index,
                "owner_scope": "test",
                "allowed_scopes": [],
            })
            .await
            .map_err(|e| E2EError::Setup(e.to_string()))?;
    }

    // `spec --json` resolves the active revision via the server. It must return
    // the NEWEST consistently across repeated calls, not flap between the two.
    for _ in 0..6 {
        let v = device_json(&setup, "spec --json").await?;
        let id = v.get("id").and_then(|x| x.as_str());
        assert_eq!(
            id,
            Some(rev_new.as_str()),
            "active revision must resolve deterministically to the newest; got {id:?}"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Trait shim: `&str -> &'static str` for the static-lifetime description
// fields used by `WaitConfig`. We need this because the polling helpers
// require `&'static str` and we build descriptions with format!() above.
// ---------------------------------------------------------------------------

trait LeakStaticExt {
    fn leak_static(self) -> &'static str;
}

impl LeakStaticExt for String {
    fn leak_static(self) -> &'static str {
        Box::leak(self.into_boxed_str())
    }
}

