//! End-to-end tests for the *class of services that launch real Docker
//! containers* — the shape our customers actually deploy (a service whose
//! steps run `docker run` / `docker compose up`, with a `stop:` block that
//! tears the container down again).
//!
//! Unlike `deployment.rs` (which only asserts what lands in the *spec* using
//! `run: "true"` no-ops), these tests start **real containers on the host
//! docker** (the runtime container has the docker CLI and the host socket
//! mounted) and assert on `docker ps`. That's the only way to prove the
//! invariant that matters on a device with exclusive hardware (camera / LTE):
//! **at most one container of a given service-class may run at a time.**
//!
//! We model "the camera" as a label: every container this suite starts carries
//! `m87e2ecam=<device-short-id>` (unique per test, so parallel tests don't
//! collide on the shared host docker) plus `m87e2ever=<version>`. Counting
//! running containers with that label tells us directly whether m87 ever left
//! two alive at once — which is what bricked the customer's device.
//!
//! What each test proves is called out inline. `docker_crash_before_stop_*`
//! pins the customer's real bug — a power cut in the window after reconcile
//! decides to stop the old unit but before its stop runs, which a restart would
//! otherwise forget (the teardown intent is now rebuilt on boot).

use std::time::Duration;

use super::fixtures::{RuntimeRunner, TestSetup};
use super::helpers::{exec_shell, wait_for, E2EError, WaitConfig};

// ---------------------------------------------------------------------------
// Docker helpers — all run directly on the device (runtime container).
// ---------------------------------------------------------------------------

/// The container image used for the fake "camera" workload. `busybox sleep`
/// is a tiny, long-lived process; we pre-pull it so the first `docker run`
/// step can't time out on a cold pull.
const CAM_IMAGE: &str = "busybox";

/// Label key that scopes every container this suite starts to one test run.
fn cam_label(short: &str) -> String {
    format!("m87e2ecam={short}")
}

fn container_name(short: &str, ver: &str) -> String {
    format!("m87e2e_{short}_{ver}")
}

/// Pre-pull the workload image on the device so `docker run` steps are fast.
async fn prepull(setup: &TestSetup) -> Result<(), E2EError> {
    exec_shell(
        &setup.infra.runtime,
        &format!("docker pull {CAM_IMAGE} >/dev/null 2>&1 || true"),
    )
    .await?;
    Ok(())
}

/// Number of *running* containers carrying this run's camera label.
async fn running_count(setup: &TestSetup, short: &str) -> Result<usize, E2EError> {
    let out = exec_shell(
        &setup.infra.runtime,
        &format!(
            "docker ps -q --filter label={} | wc -l | tr -d '[:space:]'",
            cam_label(short)
        ),
    )
    .await?;
    out.trim()
        .parse::<usize>()
        .map_err(|e| E2EError::Parse(format!("running_count: bad `wc -l` output {out:?}: {e}")))
}

/// Number of running containers for a specific version label.
async fn running_count_ver(setup: &TestSetup, short: &str, ver: &str) -> Result<usize, E2EError> {
    let out = exec_shell(
        &setup.infra.runtime,
        &format!(
            "docker ps -q --filter label={} --filter label=m87e2ever={ver} | wc -l | tr -d '[:space:]'",
            cam_label(short)
        ),
    )
    .await?;
    out.trim().parse::<usize>().map_err(|e| {
        E2EError::Parse(format!("running_count_ver: bad `wc -l` output {out:?}: {e}"))
    })
}

/// Force-remove every container this run started (best-effort cleanup).
async fn cleanup(setup: &TestSetup, short: &str) {
    let _ = exec_shell(
        &setup.infra.runtime,
        &format!(
            "docker rm -f $(docker ps -aq --filter label={}) >/dev/null 2>&1 || true",
            cam_label(short)
        ),
    )
    .await;
}

/// Poll until exactly `expected` containers with this run's label are running.
async fn wait_running_total(
    setup: &TestSetup,
    short: &str,
    expected: usize,
    description: &'static str,
) -> Result<(), E2EError> {
    wait_for(
        WaitConfig::with_description(description)
            .max_attempts(45)
            .interval(Duration::from_secs(2)),
        || async { running_count(setup, short).await.map(|n| n == expected).unwrap_or(false) },
    )
    .await
}

// ---------------------------------------------------------------------------
// Spec builders — a service whose start launches a real container.
// ---------------------------------------------------------------------------

/// Build one `services:`-list entry that starts a real busybox container.
///
/// * `start` `docker rm -f` of its own name first (idempotent across the
///   once-per-hash retries m87 may do), then `docker run -d`.
/// * when `with_stop`, a `stop:` block that removes the container again — the
///   correct way to let m87 tear the unit down on removal/rename.
fn cam_service_entry(id: &str, short: &str, ver: &str, with_stop: bool) -> String {
    let name = container_name(short, ver);
    let label = cam_label(short);
    let mut entry = format!(
        r#"  - id: {id}
    workdir:
      mode: persistent
    steps:
      - name: up
        timeout: 60s
        run: "docker rm -f {name} >/dev/null 2>&1 || true; docker run -d --label {label} --label m87e2ever={ver} --name {name} {CAM_IMAGE} sleep 3600"
"#
    );
    if with_stop {
        entry.push_str(&format!(
            r#"    stop:
      steps:
        - name: down
          timeout: 60s
          run: "docker rm -f {name}"
"#
        ));
    }
    entry
}

fn revision(entries: &[String]) -> String {
    format!("services:\n{}", entries.concat())
}

/// Drop a spec file into the CLI container.
async fn write_spec(setup: &TestSetup, path: &str, content: &str) -> Result<(), E2EError> {
    let cmd = format!(
        "mkdir -p $(dirname {path}) && cat > {path} <<'M87_E2E_EOF'\n{content}\nM87_E2E_EOF"
    );
    exec_shell(&setup.infra.cli, &cmd).await?;
    Ok(())
}

async fn deploy(setup: &TestSetup, path: &str, _replace_all: bool) -> Result<(), E2EError> {
    // Always `--replace-all`: the non-replace-all full-revision deploy path has
    // a separate server bug (Mongo rejects the `$set` with "FieldPath field
    // names may not contain '.'"). `--replace-all` replaces the whole spec,
    // which is the semantics every test here wants anyway.
    exec_shell(
        &setup.infra.cli,
        &format!("m87 {} deploy {path} --replace-all 2>&1", setup.device.name),
    )
    .await?;
    Ok(())
}

/// Simulate a machine/agent restart: kill the running agent and start a fresh
/// one. The log is truncated first so `wait_for_control_tunnel` observes the
/// *new* agent's startup rather than a stale line.
async fn restart_agent(setup: &TestSetup) -> Result<(), E2EError> {
    // Clear any crash trigger so the recovered agent runs to completion.
    let _ = exec_shell(
        &setup.infra.runtime,
        "rm -f /root/.local/share/m87/.crash_at /root/.local/share/m87/.reached_*; pkill -9 -x m87 >/dev/null 2>&1 || true; : > /tmp/runtime-run.log",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    RuntimeRunner::new(&setup.infra).start_with_tunnel().await
}

/// Wait for an update to fully converge: exactly the `new_ver` container runs,
/// the `old_ver` one is gone, and the total is 1. Needed because the old
/// container lingers until the ~30s heartbeat delivers the new revision, so a
/// plain "total == 1" check would pass on the stale state.
async fn wait_transition(
    setup: &TestSetup,
    short: &str,
    new_ver: &str,
    old_ver: &str,
    description: &'static str,
) -> Result<(), E2EError> {
    wait_for(
        WaitConfig::with_description(description)
            .max_attempts(45)
            .interval(Duration::from_secs(2)),
        || async {
            let nv = running_count_ver(setup, short, new_ver).await.unwrap_or(0);
            let ov = running_count_ver(setup, short, old_ver).await.unwrap_or(99);
            let total = running_count(setup, short).await.unwrap_or(99);
            nv == 1 && ov == 0 && total == 1
        },
    )
    .await
}

/// Kill the running agent and start a fresh one with `M87_CRASH_AT=<point>` so
/// it hard-exits (emulated power cut) the first time it reaches that reconcile
/// point. Does NOT wait for the tunnel — the agent is meant to die.
async fn start_agent_with_crash(setup: &TestSetup, point: &str) -> Result<(), E2EError> {
    // Arm the crash via a file the agent reads at each reconcile point (robust
    // to plumb through a container exec), then start the agent and wait for its
    // tunnel so it will actually reconcile — and crash at `point`.
    let _ = exec_shell(
        &setup.infra.runtime,
        &format!(
            "mkdir -p /root/.local/share/m87; rm -f /root/.local/share/m87/.reached_*; \
             printf '{point}' > /root/.local/share/m87/.crash_at; \
             pkill -9 -x m87 >/dev/null 2>&1 || true; : > /tmp/runtime-run.log"
        ),
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    RuntimeRunner::new(&setup.infra).start_with_tunnel().await
}

/// Wait until reconcile has frozen at `point`, then SIGKILL the whole `m87`
/// process tree — a real power cut at exactly that spot (a single exit would be
/// respawned by the supervisor).
async fn crash_at_point(setup: &TestSetup, point: &str) -> Result<(), E2EError> {
    wait_for(
        WaitConfig::with_description("reconcile reached crash point")
            .max_attempts(45)
            .interval(Duration::from_secs(2)),
        || async {
            exec_shell(
                &setup.infra.runtime,
                &format!("test -f /root/.local/share/m87/.reached_{point} && echo y || echo n"),
            )
            .await
            .map(|o| o.trim() == "y")
            .unwrap_or(false)
        },
    )
    .await?;
    // Cut power to the entire tree.
    let _ = exec_shell(&setup.infra.runtime, "pkill -9 -x m87 >/dev/null 2>&1 || true").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

/// Assert that no more than `max` containers of `ver` run over `samples`
/// polls — used for negative ("must never appear") invariants.
async fn assert_stays_at_most(
    setup: &TestSetup,
    short: &str,
    ver: &str,
    max: usize,
    samples: u32,
) -> Result<(), E2EError> {
    for _ in 0..samples {
        let n = running_count_ver(setup, short, ver).await?;
        assert!(
            n <= max,
            "version {ver} had {n} running (expected <= {max}) — a concurrent container appeared"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Baseline: updating a service **in place** (same id, changed start command)
/// must never leave the old and new container running together. Proves the
/// stop-old-before-start-new ordering works against *real* docker, and that
/// the server-side supersede-by-id keeps it a single revision.
#[tokio::test]
async fn docker_update_same_id_keeps_single_container() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let short = setup.device.short_id.clone();
    cleanup(&setup, &short).await;
    prepull(&setup).await?;

    // v1
    let v1 = revision(&[cam_service_entry("cam", &short, "v1", true)]);
    write_spec(&setup, "/tmp/cam_v1.yml", &v1).await?;
    deploy(&setup, "/tmp/cam_v1.yml", false).await?;
    wait_running_total(&setup, &short, 1, "cam v1 running").await?;
    assert_eq!(running_count_ver(&setup, &short, "v1").await?, 1);

    // v2 — same id, different start command (a new hash → stop v1, start v2).
    let v2 = revision(&[cam_service_entry("cam", &short, "v2", true)]);
    write_spec(&setup, "/tmp/cam_v2.yml", &v2).await?;
    deploy(&setup, "/tmp/cam_v2.yml", false).await?;

    wait_transition(&setup, &short, "v2", "v1", "cam updated to v2, v1 torn down").await?;

    cleanup(&setup, &short).await;
    Ok(())
}

/// `--replace-all` to a **renamed** unit that declares proper `stop:` steps
/// must tear down the old container. Proves reconcile runs the removed unit's
/// stop steps against real docker.
#[tokio::test]
async fn docker_replace_all_tears_down_removed_unit() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let short = setup.device.short_id.clone();
    cleanup(&setup, &short).await;
    prepull(&setup).await?;

    let v1 = revision(&[cam_service_entry("cam-a", &short, "v1", true)]);
    write_spec(&setup, "/tmp/a.yml", &v1).await?;
    deploy(&setup, "/tmp/a.yml", false).await?;
    wait_running_total(&setup, &short, 1, "cam-a running").await?;

    // Rename cam-a -> cam-b via replace-all. cam-a is removed; its stop steps
    // must run and remove the old container.
    let v2 = revision(&[cam_service_entry("cam-b", &short, "v2", true)]);
    write_spec(&setup, "/tmp/b.yml", &v2).await?;
    deploy(&setup, "/tmp/b.yml", true).await?;

    wait_transition(&setup, &short, "v2", "v1", "renamed to cam-b, cam-a torn down").await?;

    cleanup(&setup, &short).await;
    Ok(())
}

/// Sibling isolation against real docker: a service whose start **fails**
/// (image not present, `--pull=never`) must not stop a healthy sibling in the
/// same revision from starting. Validates that reconcile no longer aborts the
/// whole pass on the first failing unit.
#[tokio::test]
async fn docker_failed_start_does_not_block_healthy_sibling() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let short = setup.device.short_id.clone();
    cleanup(&setup, &short).await;
    prepull(&setup).await?;

    // A broken unit (start can never succeed) alongside a healthy camera unit.
    let broken = format!(
        r#"  - id: broken
    steps:
      - name: up
        timeout: 30s
        run: "docker run -d --pull=never m87-no-such-image-{short} true"
"#
    );
    let healthy = cam_service_entry("cam", &short, "v1", true);
    let rev = revision(&[broken, healthy]);
    write_spec(&setup, "/tmp/mixed.yml", &rev).await?;
    deploy(&setup, "/tmp/mixed.yml", false).await?;

    // The healthy sibling must come up despite the broken unit failing.
    wait_running_total(&setup, &short, 1, "healthy sibling running despite broken unit").await?;
    assert_eq!(running_count_ver(&setup, &short, "v1").await?, 1);

    cleanup(&setup, &short).await;
    Ok(())
}

/// A **slow / hanging stop** (models `docker stop` on a container that won't
/// release quickly, or a wedged compose down) must not cause concurrency. When
/// the stop step exceeds its timeout, reconcile must NOT start the replacement
/// — worst case is a stalled update (old stays up), never two on the camera.
#[tokio::test]
async fn docker_slow_stop_defers_start_no_concurrency() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let short = setup.device.short_id.clone();
    cleanup(&setup, &short).await;
    prepull(&setup).await?;

    // v1 with a stop step that hangs far longer than its 3s timeout.
    let name_v1 = container_name(&short, "v1");
    let label = cam_label(&short);
    let v1 = format!(
        r#"services:
  - id: cam
    workdir:
      mode: persistent
    steps:
      - name: up
        timeout: 60s
        run: "docker rm -f {name_v1} >/dev/null 2>&1 || true; docker run -d --label {label} --label m87e2ever=v1 --name {name_v1} {CAM_IMAGE} sleep 3600"
    stop:
      steps:
        - name: down
          timeout: 3s
          run: "sleep 30; docker rm -f {name_v1}"
"#
    );
    write_spec(&setup, "/tmp/slow_v1.yml", &v1).await?;
    deploy(&setup, "/tmp/slow_v1.yml", false).await?;
    wait_running_total(&setup, &short, 1, "cam v1 running").await?;

    // Update in place → reconcile must stop v1 (times out) before starting v2.
    let v2 = revision(&[cam_service_entry("cam", &short, "v2", true)]);
    write_spec(&setup, "/tmp/slow_v2.yml", &v2).await?;
    deploy(&setup, "/tmp/slow_v2.yml", false).await?;

    // v2 must never come up while v1's stop keeps timing out — no two-on-camera.
    // The window (30 x 2s = 60s) comfortably exceeds the ~30s heartbeat delay
    // before v2 is even delivered, so if m87 were going to start v2 it would
    // have done so well within it.
    assert_stays_at_most(&setup, &short, "v2", 0, 30).await?;
    assert_eq!(
        running_count_ver(&setup, &short, "v1").await?,
        1,
        "old unit stays up (safe stall) while its stop can't complete"
    );

    cleanup(&setup, &short).await;
    Ok(())
}

/// A **flaky pull** (fails a few times, then succeeds) must self-heal via
/// step-level `retry:` rather than leaving the unit stuck. Documents the
/// resilient way to configure a pull for a bad link.
#[tokio::test]
async fn docker_flaky_pull_recovers_with_retry() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let short = setup.device.short_id.clone();
    cleanup(&setup, &short).await;
    prepull(&setup).await?;

    // Reset the attempt counter on the device.
    let counter = format!("/tmp/pc_{short}");
    let _ = exec_shell(&setup.infra.runtime, &format!("rm -f {counter}")).await;

    let name = container_name(&short, "v1");
    let label = cam_label(&short);
    // First two attempts exit 1 (pull "fails"); the third runs the container.
    let spec = format!(
        r#"services:
  - id: cam
    workdir:
      mode: persistent
    steps:
      - name: up
        timeout: 60s
        retry:
          attempts: 5
          backoff: 1s
        run: "n=$(cat {counter} 2>/dev/null || echo 0); n=$((n+1)); echo $n > {counter}; if [ \"$n\" -lt 3 ]; then echo flaky-pull-fail-$n; exit 1; fi; docker rm -f {name} >/dev/null 2>&1 || true; docker run -d --label {label} --label m87e2ever=v1 --name {name} {CAM_IMAGE} sleep 3600"
"#
    );
    write_spec(&setup, "/tmp/flaky.yml", &spec).await?;
    deploy(&setup, "/tmp/flaky.yml", false).await?;

    // Despite the first two failures, the retry must eventually land the container.
    wait_running_total(&setup, &short, 1, "flaky pull recovers to a running container").await?;

    cleanup(&setup, &short).await;
    Ok(())
}

/// **Crash / machine restart mid-update.** With v1 up and a rename-to-v2 in
/// flight, kill the agent (interrupting reconcile) and bring it back — the way
/// a Pi loses power mid-deploy. After recovery there must be exactly one
/// container (the new one); the old one must not linger alongside it.
#[tokio::test]
async fn docker_crash_mid_update_recovers_to_single_container() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let short = setup.device.short_id.clone();
    cleanup(&setup, &short).await;
    prepull(&setup).await?;

    // v1 cam-a with a deliberately slow stop, so the kill lands mid-teardown.
    let name_a = container_name(&short, "v1");
    let label = cam_label(&short);
    let v1 = format!(
        r#"services:
  - id: cam-a
    workdir:
      mode: persistent
    steps:
      - name: up
        timeout: 60s
        run: "docker rm -f {name_a} >/dev/null 2>&1 || true; docker run -d --label {label} --label m87e2ever=v1 --name {name_a} {CAM_IMAGE} sleep 3600"
    stop:
      steps:
        - name: down
          timeout: 60s
          run: "sleep 5; docker rm -f {name_a}"
"#
    );
    write_spec(&setup, "/tmp/crash_a.yml", &v1).await?;
    deploy(&setup, "/tmp/crash_a.yml", false).await?;
    wait_running_total(&setup, &short, 1, "cam-a running").await?;

    // Rename to cam-b via replace-all, then crash mid-reconcile.
    let v2 = revision(&[cam_service_entry("cam-b", &short, "v2", true)]);
    write_spec(&setup, "/tmp/crash_b.yml", &v2).await?;
    deploy(&setup, "/tmp/crash_b.yml", true).await?;
    // Let reconcile enter the slow stop, then kill the agent.
    tokio::time::sleep(Duration::from_secs(2)).await;
    restart_agent(&setup).await?;

    // After recovery, reconcile must converge to exactly one container (cam-b).
    wait_for(
        WaitConfig::with_description("recovered to a single container")
            .max_attempts(60)
            .interval(Duration::from_secs(2)),
        || async {
            let total = running_count(&setup, &short).await.unwrap_or(99);
            let v2 = running_count_ver(&setup, &short, "v2").await.unwrap_or(0);
            total == 1 && v2 == 1
        },
    )
    .await?;
    assert_eq!(
        running_count_ver(&setup, &short, "v1").await?,
        0,
        "old container must not linger after a crash-interrupted rename"
    );

    cleanup(&setup, &short).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Deterministic crash-injection matrix (M87_CRASH_AT hard-exits the runtime at
// a named reconcile point). These cut power at exact spots instead of racing a
// timer, so they actually prove what survives a mid-update power loss.
// ---------------------------------------------------------------------------

/// Power cut AFTER the start steps run (container up) but BEFORE success is
/// recorded. On restart the (idempotent) start re-runs and must converge to
/// exactly one container — not wedge on "name already in use".
#[tokio::test]
async fn docker_crash_after_start_recovers_idempotent() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let short = setup.device.short_id.clone();
    cleanup(&setup, &short).await;
    prepull(&setup).await?;

    start_agent_with_crash(&setup, "after_start_steps").await?;

    let v1 = revision(&[cam_service_entry("cam", &short, "v1", true)]);
    write_spec(&setup, "/tmp/ca.yml", &v1).await?;
    deploy(&setup, "/tmp/ca.yml", false).await?;
    crash_at_point(&setup, "after_start_steps").await?;

    restart_agent(&setup).await?;
    // Converge to exactly one v1 container. Check total AND version in a single
    // poll: the idempotent start step is `docker rm -f {name}; docker run`, so a
    // separate `running_count_ver` query can land in the down-then-up gap while
    // the step re-executes and read 0 even though total momentarily reads 1.
    wait_for(
        WaitConfig::with_description("single v1 container after crash recovery")
            .max_attempts(45)
            .interval(Duration::from_secs(2)),
        || async {
            let total = running_count(&setup, &short).await.unwrap_or(99);
            let v1 = running_count_ver(&setup, &short, "v1").await.unwrap_or(0);
            total == 1 && v1 == 1
        },
    )
    .await?;

    cleanup(&setup, &short).await;
    Ok(())
}

/// Power cut mid-transition — AFTER the old unit is stopped, BEFORE the new one
/// starts. On restart reconcile must converge to exactly the new container.
#[tokio::test]
async fn docker_crash_mid_transition_converges_to_new() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let short = setup.device.short_id.clone();
    cleanup(&setup, &short).await;
    prepull(&setup).await?;

    let v1 = revision(&[cam_service_entry("cam-a", &short, "v1", true)]);
    write_spec(&setup, "/tmp/ta.yml", &v1).await?;
    deploy(&setup, "/tmp/ta.yml", false).await?;
    wait_running_total(&setup, &short, 1, "cam-a running").await?;

    start_agent_with_crash(&setup, "after_stop_before_start").await?;
    let v2 = revision(&[cam_service_entry("cam-b", &short, "v2", true)]);
    write_spec(&setup, "/tmp/tb.yml", &v2).await?;
    deploy(&setup, "/tmp/tb.yml", true).await?;
    crash_at_point(&setup, "after_stop_before_start").await?;

    restart_agent(&setup).await?;
    wait_transition(&setup, &short, "v2", "v1", "converged to cam-b after crash").await?;

    cleanup(&setup, &short).await;
    Ok(())
}

/// THE customer bug (expected to FAIL until B lands). A power cut lands in the
/// window after reconcile has decided to stop the old unit but *before* it runs
/// the stop — the wider that window (a slow stop), the likelier a reboot hits
/// it. cam-a has a perfectly CORRECT, working stop; it just never gets to run,
/// because the crash is at `before_stops`. On restart m87 rebuilds its dirty
/// set from the DESIRED revision only, so the renamed-away cam-a is forgotten:
/// its stop is never re-run and it stays alive alongside cam-b — two on the
/// camera. The teardown intent must survive the restart (B). Un-ignore once it
/// does.
#[tokio::test]
async fn docker_crash_before_stop_orphans_old_unit() -> Result<(), E2EError> {
    let setup = TestSetup::init().await?;
    let short = setup.device.short_id.clone();
    cleanup(&setup, &short).await;
    prepull(&setup).await?;

    // cam-a with a correct, working stop (`docker rm -f`) — it WOULD tear down
    // cleanly if m87 ever ran it.
    let v1 = revision(&[cam_service_entry("cam-a", &short, "v1", true)]);
    write_spec(&setup, "/tmp/ba.yml", &v1).await?;
    deploy(&setup, "/tmp/ba.yml", false).await?;
    wait_running_total(&setup, &short, 1, "cam-a running").await?;

    // Rename to cam-b; cut power BEFORE reconcile runs cam-a's stop.
    start_agent_with_crash(&setup, "before_stops").await?;
    let v2 = revision(&[cam_service_entry("cam-b", &short, "v2", true)]);
    write_spec(&setup, "/tmp/bb.yml", &v2).await?;
    deploy(&setup, "/tmp/bb.yml", true).await?;
    crash_at_point(&setup, "before_stops").await?;

    // Recover, let cam-b come up, then check we did NOT leave two on the camera.
    restart_agent(&setup).await?;
    wait_for(
        WaitConfig::with_description("cam-b started after crash")
            .max_attempts(45)
            .interval(Duration::from_secs(2)),
        || async { running_count_ver(&setup, &short, "v2").await.map(|n| n == 1).unwrap_or(false) },
    )
    .await?;
    let total = running_count(&setup, &short).await?;
    cleanup(&setup, &short).await;
    assert_eq!(
        total, 1,
        "old container orphaned across the crash (found {total}): the pending teardown of a \
         removed unit must survive a restart and be re-run before the update is done"
    );
    Ok(())
}

