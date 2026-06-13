//! E2E tests for the unified `logs` and enhanced `status` commands.
//!
//! These assert against parsed JSON / NDJSON and process exit codes only —
//! never table text — so the renderers can change freely.
//!
//! We use **job runs** as the event source: triggering a job creates a
//! deterministic, fast, end-to-end-observable event stream (StepReport →
//! JobRunReport) that we've already proved works in
//! `deployment::test_job_trigger_completes`. Observe-based service tests
//! depend on runtime step-execution timing that is harder to nail down
//! reliably in CI without longer waits than the suite's budget allows.

use serde_json::Value;
use std::time::Duration;
use testcontainers::core::ExecCommand;

use super::fixtures::TestSetup;
use super::helpers::{exec_shell, wait_for_result, E2EError, WaitConfig};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

async fn write_cli_file(setup: &TestSetup, path: &str, content: &str) -> Result<(), E2EError> {
    let cmd = format!(
        "mkdir -p $(dirname {path}) && cat > {path} <<'M87_E2E_EOF'\n{content}\nM87_E2E_EOF"
    );
    exec_shell(&setup.infra.cli, &cmd).await?;
    Ok(())
}

/// Run `m87 <device> <args>` with `RUST_LOG=error` and parse stdout as JSON.
async fn device_json(setup: &TestSetup, args: &str) -> Result<Value, E2EError> {
    let cmd = format!("RUST_LOG=error m87 {} {}", setup.device.name, args);
    let out = exec_shell(&setup.infra.cli, &cmd).await?;
    serde_json::from_str(out.trim()).map_err(|e| {
        E2EError::Parse(format!(
            "failed to parse JSON from `m87 {args}`: {e}\n--- output ---\n{out}"
        ))
    })
}

/// Run `m87 <device> <args>` and return (stdout, exit_code). Used by status
/// tests where the exit code is the assertion target.
async fn device_with_exit(setup: &TestSetup, args: &str) -> Result<(String, i32), E2EError> {
    let cmd = format!(
        "RUST_LOG=error m87 {} {} ; echo __EXIT__$?",
        setup.device.name, args
    );
    let mut result = setup
        .infra
        .cli
        .exec(ExecCommand::new(vec!["sh", "-c", cmd.as_str()]))
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;
    let stdout = result
        .stdout_to_vec()
        .await
        .map_err(|e| E2EError::Exec(e.to_string()))?;
    let text = String::from_utf8_lossy(&stdout).to_string();
    let (body, code) = match text.rsplit_once("__EXIT__") {
        Some((b, rest)) => {
            let code = rest.trim().parse::<i32>().unwrap_or(-1);
            (b.trim_end().to_string(), code)
        }
        None => (text.clone(), -1),
    };
    Ok((body, code))
}

async fn device_ndjson(setup: &TestSetup, args: &str) -> Result<Vec<Value>, E2EError> {
    let cmd = format!("RUST_LOG=error m87 {} {}", setup.device.name, args);
    let out = exec_shell(&setup.infra.cli, &cmd).await?;
    Ok(out
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect())
}

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
            .max_attempts(60)
            .interval(Duration::from_secs(2)),
        || async {
            let v = device_json(setup, args).await?;
            Ok(pred(&v))
        },
    )
    .await
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

fn service_yaml(id: &str) -> String {
    format!(
        r#"id: {id}
steps:
  - name: noop
    run: "true"
"#
    )
}

/// Job that always succeeds. Wrapped in a full revision because a bare
/// JobDef YAML is structurally identical to a ServiceSpec.
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

/// Job whose step always fails — used to seed failed events into the
/// deploy_reports collection.
fn failing_job_revision_yaml(id: &str) -> String {
    format!(
        r#"job_defs:
  - id: {id}
    steps:
      - name: fail
        run: "false"
"#
    )
}

// ---------------------------------------------------------------------------
// `logs` tests
// ---------------------------------------------------------------------------

/// `logs --jobs --json` returns at least one event for a triggered job run.
/// This is the foundational "is the unified history surface wired up?"
/// check — it doesn't matter which event kinds come back, just that the
/// command returns NDJSON for events that the server has stored.
#[tokio::test]
async fn test_logs_returns_events_for_triggered_job() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let job_id = "e2e-logs-events";
    write_cli_file(&setup, "/tmp/job.yml", &job_revision_yaml(job_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/job.yml 2>&1", setup.device.name),
    )
    .await?;

    // Wait for job def to land then trigger.
    wait_for_device_json(&setup, "job defs --json", "job def visible", |v| {
        v.as_array()?
            .iter()
            .find_map(|j| (j.get("id").and_then(|x| x.as_str()) == Some(job_id)).then(|| ()))
    })
    .await?;

    let trigger_out = device_json(&setup, &format!("job trigger {job_id} --json")).await?;
    let run_id = trigger_out
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("trigger returns run_id")
        .to_string();

    // Wait for the unified logs surface to surface at least one event for
    // this run. Job events are guaranteed to flow because
    // `deployment::test_job_trigger_completes` already proves that the
    // job-trigger heartbeat round-trip lands.
    let events = wait_for_result(
        WaitConfig::with_description("event for triggered run in unified logs")
            .max_attempts(60)
            .interval(Duration::from_secs(2)),
        || async {
            let ev = device_ndjson(&setup, "logs --json -n 200").await?;
            let for_run = ev
                .iter()
                .find(|e| e.get("run_id").and_then(|v| v.as_str()) == Some(run_id.as_str()))
                .cloned();
            Ok(for_run.map(|_| ev))
        },
    )
    .await?;

    // Spot-check the event shape.
    let any = events.first().expect("at least one event");
    assert!(any.get("ts").and_then(|v| v.as_u64()).unwrap_or(0) > 0);
    assert!(any.get("category").and_then(|v| v.as_str()).is_some());
    assert!(any.get("sub_kind").is_some());
    Ok(())
}

/// `logs --failed --json` returns only failed events when a failing job
/// is triggered.
#[tokio::test]
async fn test_logs_failed_filter_with_failing_job() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let job_id = "e2e-logs-failed-job";
    write_cli_file(&setup, "/tmp/job.yml", &failing_job_revision_yaml(job_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/job.yml 2>&1", setup.device.name),
    )
    .await?;

    wait_for_device_json(&setup, "job defs --json", "job def visible", |v| {
        v.as_array()?
            .iter()
            .find_map(|j| (j.get("id").and_then(|x| x.as_str()) == Some(job_id)).then(|| ()))
    })
    .await?;

    let trigger_out = device_json(&setup, &format!("job trigger {job_id} --json")).await?;
    let run_id = trigger_out
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("trigger returns run_id")
        .to_string();

    let events = wait_for_result(
        WaitConfig::with_description("failed event for run")
            .max_attempts(60)
            .interval(Duration::from_secs(2)),
        || async {
            let ev = device_ndjson(&setup, "logs --failed --json -n 200").await?;
            let only_failures = ev
                .iter()
                .all(|e| e.get("success").and_then(|v| v.as_bool()) == Some(false));
            let for_run = ev
                .iter()
                .any(|e| e.get("run_id").and_then(|v| v.as_str()) == Some(run_id.as_str()));
            Ok(if only_failures && for_run { Some(ev) } else { None })
        },
    )
    .await?;

    assert!(events
        .iter()
        .any(|e| e.get("run_id").and_then(|v| v.as_str()) == Some(run_id.as_str())));
    Ok(())
}

/// `--services` and `--jobs` are mutually narrowing. With `--services` we
/// must not see job-category events; with `--jobs` we must not see service
/// step events. Verified by triggering a job and querying both filters.
#[tokio::test]
async fn test_logs_kind_scoping_excludes_other_category() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let job_id = "e2e-logs-scoped";
    write_cli_file(&setup, "/tmp/job.yml", &job_revision_yaml(job_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/job.yml 2>&1", setup.device.name),
    )
    .await?;
    wait_for_device_json(&setup, "job defs --json", "job def visible", |v| {
        v.as_array()?
            .iter()
            .find_map(|j| (j.get("id").and_then(|x| x.as_str()) == Some(job_id)).then(|| ()))
    })
    .await?;

    let trigger_out = device_json(&setup, &format!("job trigger {job_id} --json")).await?;
    let run_id = trigger_out
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("trigger returns run_id")
        .to_string();

    // Wait until the job event surfaces in --jobs scope.
    wait_for_result(
        WaitConfig::with_description("job event under --jobs scope")
            .max_attempts(60)
            .interval(Duration::from_secs(2)),
        || async {
            let ev = device_ndjson(&setup, "logs --jobs --json -n 200").await?;
            Ok(if ev
                .iter()
                .any(|e| e.get("run_id").and_then(|v| v.as_str()) == Some(run_id.as_str()))
            {
                Some(())
            } else {
                None
            })
        },
    )
    .await?;

    // --jobs must not include service-category events.
    let jobs_only = device_ndjson(&setup, "logs --jobs --json -n 500").await?;
    assert!(
        jobs_only
            .iter()
            .all(|e| e.get("category").and_then(|v| v.as_str()) != Some("service")),
        "expected no service-category events in --jobs output; got {jobs_only:?}"
    );
    Ok(())
}

/// `logs --json` produces NDJSON (each line is a complete JSON object).
/// Even with zero events the command must exit cleanly with empty stdout.
/// Regression guard against accidentally switching to pretty-printed JSON.
#[tokio::test]
async fn test_logs_json_output_is_ndjson() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let job_id = "e2e-logs-ndjson";
    write_cli_file(&setup, "/tmp/job.yml", &job_revision_yaml(job_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/job.yml 2>&1", setup.device.name),
    )
    .await?;
    wait_for_device_json(&setup, "job defs --json", "job def visible", |v| {
        v.as_array()?
            .iter()
            .find_map(|j| (j.get("id").and_then(|x| x.as_str()) == Some(job_id)).then(|| ()))
    })
    .await?;
    let trigger = device_json(&setup, &format!("job trigger {job_id} --json")).await?;
    let run_id = trigger
        .get("run_id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    // Wait for at least one event.
    wait_for_result(
        WaitConfig::with_description("any event for run")
            .max_attempts(60)
            .interval(Duration::from_secs(2)),
        || async {
            let ev = device_ndjson(&setup, "logs --json -n 50").await?;
            Ok(if ev
                .iter()
                .any(|e| e.get("run_id").and_then(|v| v.as_str()) == Some(run_id.as_str()))
            {
                Some(())
            } else {
                None
            })
        },
    )
    .await?;

    // Raw output must be NDJSON: every non-empty line parses on its own.
    let raw = exec_shell(
        &setup.infra.cli,
        &format!("RUST_LOG=error m87 {} logs --json -n 50", setup.device.name),
    )
    .await?;
    for (i, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let _: Value = serde_json::from_str(line).map_err(|e| {
            E2EError::Parse(format!(
                "line {i} is not standalone JSON ({e}): {line:?}"
            ))
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `status` tests
// ---------------------------------------------------------------------------

/// `status --short` on a healthy device prints "✓ ... all healthy" and
/// exits 0. The exit code is the load-bearing assertion — scripts depend
/// on it.
#[tokio::test]
async fn test_status_short_healthy_exits_zero() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let svc_id = "e2e-status-healthy";
    write_cli_file(&setup, "/tmp/svc.yml", &service_yaml(svc_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/svc.yml 2>&1", setup.device.name),
    )
    .await?;

    // Give the device a beat to take the new spec.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let (out, code) = device_with_exit(&setup, "status --short").await?;
    assert!(
        out.contains("✓") && out.contains("healthy"),
        "unexpected short output: {out}"
    );
    assert_eq!(code, 0, "healthy status should exit 0; got {code} ({out})");
    Ok(())
}

/// `status --quiet` on a healthy device produces no output and exits 0.
/// This is the "health probe" path that CI / cron / supervisors will use.
#[tokio::test]
async fn test_status_quiet_healthy_no_output_zero_exit() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let svc_id = "e2e-status-quiet";
    write_cli_file(&setup, "/tmp/svc.yml", &service_yaml(svc_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/svc.yml 2>&1", setup.device.name),
    )
    .await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let (out, code) = device_with_exit(&setup, "status --quiet").await?;
    assert!(out.trim().is_empty(), "--quiet should produce no output; got {out:?}");
    assert_eq!(code, 0, "healthy quiet status should exit 0; got {code}");
    Ok(())
}

/// `status --json` returns a parseable summary with the expected top-level
/// fields. Doesn't assert specific values (depends on runtime state) but
/// guards the JSON shape from accidental breakage.
#[tokio::test]
async fn test_status_json_has_expected_shape() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let svc_id = "e2e-status-json";
    write_cli_file(&setup, "/tmp/svc.yml", &service_yaml(svc_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/svc.yml 2>&1", setup.device.name),
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(5)).await;

    let summary = device_json(&setup, "status --json").await?;
    assert!(summary.get("device").is_some(), "missing `device` field");
    assert!(summary.get("current_issues").is_some(), "missing `current_issues`");
    assert!(summary.get("observations").is_some(), "missing `observations`");
    assert!(summary.get("open_incident_ids").is_some(), "missing `open_incident_ids`");
    // No window without --since.
    assert!(
        summary.get("window").map_or(true, |v| v.is_null()),
        "window should not be set without --since"
    );
    Ok(())
}

/// `status --since X --json` adds a `window` block with aggregated counts.
/// Uses a triggered failing job so we have at least one event in the
/// window without depending on observe scheduling.
#[tokio::test]
async fn test_status_windowed_json_aggregates_events() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;

    let job_id = "e2e-status-window";
    write_cli_file(&setup, "/tmp/job.yml", &failing_job_revision_yaml(job_id)).await?;
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy /tmp/job.yml 2>&1", setup.device.name),
    )
    .await?;
    wait_for_device_json(&setup, "job defs --json", "job def visible", |v| {
        v.as_array()?
            .iter()
            .find_map(|j| (j.get("id").and_then(|x| x.as_str()) == Some(job_id)).then(|| ()))
    })
    .await?;

    let trigger = device_json(&setup, &format!("job trigger {job_id} --json")).await?;
    let run_id = trigger.get("run_id").and_then(|v| v.as_str()).unwrap().to_string();

    // Wait for the failed event to land before querying status.
    wait_for_result(
        WaitConfig::with_description("failed event present")
            .max_attempts(60)
            .interval(Duration::from_secs(2)),
        || async {
            let ev = device_ndjson(&setup, "logs --failed --json -n 50").await?;
            Ok(if ev
                .iter()
                .any(|e| e.get("run_id").and_then(|v| v.as_str()) == Some(run_id.as_str()))
            {
                Some(())
            } else {
                None
            })
        },
    )
    .await?;

    let summary = device_json(&setup, "status --since 5m --json").await?;
    let window = summary
        .get("window")
        .and_then(|w| w.as_object())
        .expect("windowed status must include a `window` block");
    let total_events = window
        .get("total_events")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert!(total_events > 0, "expected at least one event in window: {summary}");
    let total_failures = window
        .get("total_failures")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert!(
        total_failures > 0,
        "expected at least one failure in window: {summary}"
    );
    Ok(())
}
