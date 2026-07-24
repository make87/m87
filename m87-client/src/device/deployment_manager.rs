use anyhow::{Context, Result, anyhow};
use m87_shared::deploy_spec::{
    DeployReportKind, DeploymentRevision, DeploymentRevisionReport, JobDef, JobRun, JobRunReport,
    JobRunStatus, Lifecycle, LifecycleUpdate, ObserveHooks, OnFailure, Outcome, RestartPolicy,
    RunReport, RunState, ServiceSpec, Step, StepReport, UndoMode, WorkdirMode,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt::Display,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::{fs, io::AsyncWriteExt, sync::RwLock, time::sleep};

use crate::{
    device::log_manager::LogManager,
    util::{
        command::{RunCommandError, run_command},
        shutdown::SHUTDOWN,
    },
};

const MAX_TAIL_BYTES: usize = 4 * 1024; // 4 KB

// ---------------------------------------------------------------------------
// Directory helpers
// ---------------------------------------------------------------------------

fn data_dir(dir_path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = dir_path {
        return Ok(path);
    }
    Ok(dirs::data_dir().context("data_dir missing")?.join("m87"))
}

fn events_dir(dir_path: Option<PathBuf>) -> Result<PathBuf> {
    Ok(data_dir(dir_path)?.join("events"))
}

fn pending_dir(dir_path: Option<PathBuf>) -> Result<PathBuf> {
    Ok(events_dir(dir_path)?.join("pending"))
}

fn inflight_dir(dir_path: Option<PathBuf>) -> Result<PathBuf> {
    Ok(events_dir(dir_path)?.join("inflight"))
}

async fn ensure_dirs(dir_path: Option<PathBuf>) -> Result<()> {
    fs::create_dir_all(pending_dir(dir_path.clone())?).await?;
    fs::create_dir_all(inflight_dir(dir_path)?).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Deploy-report event retention
//
// Deploy-report events live as files under `events/pending` until the server
// acks them. If a device is offline, flaky, or churning, that queue can grow to
// thousands of stale files that are then replayed forever as "catch-up" on every
// reconnect — pinning a core and spamming heartbeats. Two backstops keep the
// folder bounded regardless of connectivity:
//   * age  — drop events older than `EVENT_RETENTION_SECS` (a stale deploy
//            report is worthless; the newest RunState already supersedes it).
//   * count — never keep more than `EVENT_MAX_COUNT` files; drop oldest first.
// Both are enforced lazily (throttled to once per `PRUNE_MIN_INTERVAL_SECS`) on
// the enqueue and idle-poll paths, plus once unconditionally at startup.
// ---------------------------------------------------------------------------

/// Default max age for a queued deploy-report event (2 days). Overridable via
/// the `deploy_report_retention_secs` insector config value.
const DEFAULT_EVENT_RETENTION_SECS: u64 = 172_800;
/// Hard cap on queued event files, enforced even when the age bound would keep
/// them, so a burst can never pollute the folder with thousands of entries.
const EVENT_MAX_COUNT: usize = 5_000;
/// Minimum wall-clock gap between prune scans on the hot paths.
const PRUNE_MIN_INTERVAL_SECS: u64 = 60;

static EVENT_RETENTION_SECS: AtomicU64 = AtomicU64::new(DEFAULT_EVENT_RETENTION_SECS);
/// Unix-seconds of the last prune scan; 0 means "never". Guards the throttle.
static LAST_PRUNE_UNIX: AtomicU64 = AtomicU64::new(0);

/// Set the retention window for queued deploy-report events. Called once at
/// runtime startup from the loaded [`Config`]; a value of 0 disables the age
/// bound (the count cap still applies).
pub fn set_event_retention_secs(secs: u64) {
    EVENT_RETENTION_SECS.store(secs, Ordering::Relaxed);
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Remove stale/excess `*.json` event files from a single directory.
///
/// Deterministic core (no globals, no throttle) so it can be unit-tested with a
/// fixed `now`: first drops every file whose mtime is older than `max_age`
/// (when `max_age` is non-zero), then — if more than `max_count` survive —
/// drops the oldest by mtime until `max_count` remain. Returns the number of
/// files removed. Files with unreadable metadata are left untouched.
async fn prune_events_dir(
    dir: &Path,
    max_age: Duration,
    max_count: usize,
    now: SystemTime,
) -> Result<usize> {
    let mut rd = match fs::read_dir(dir).await {
        Ok(d) => d,
        Err(_) => return Ok(0),
    };
    // (path, mtime) for every json event file.
    let mut files: Vec<(PathBuf, SystemTime)> = Vec::new();
    while let Ok(Some(entry)) = rd.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let mtime = match entry.metadata().await.and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(_) => continue,
        };
        files.push((path, mtime));
    }

    let mut removed = 0usize;
    // Age bound.
    if !max_age.is_zero() {
        let mut survivors = Vec::with_capacity(files.len());
        for (path, mtime) in files {
            let age = now.duration_since(mtime).unwrap_or(Duration::ZERO);
            if age > max_age {
                if fs::remove_file(&path).await.is_ok() {
                    removed += 1;
                }
            } else {
                survivors.push((path, mtime));
            }
        }
        files = survivors;
    }
    // Count cap: drop oldest first.
    if files.len() > max_count {
        files.sort_by_key(|(_, mtime)| *mtime);
        let overflow = files.len() - max_count;
        for (path, _) in files.into_iter().take(overflow) {
            if fs::remove_file(&path).await.is_ok() {
                removed += 1;
            }
        }
    }
    Ok(removed)
}

/// Prune both the pending and inflight event dirs using the configured
/// retention window and the hard count cap. Unconditional (no throttle) — use
/// [`maybe_prune_events`] on hot paths.
pub async fn prune_events(dir_path: Option<PathBuf>) -> Result<usize> {
    LAST_PRUNE_UNIX.store(now_unix(), Ordering::Relaxed);
    let max_age = Duration::from_secs(EVENT_RETENTION_SECS.load(Ordering::Relaxed));
    let now = SystemTime::now();
    let mut removed = 0;
    for dir in [pending_dir(dir_path.clone())?, inflight_dir(dir_path)?] {
        removed += prune_events_dir(&dir, max_age, EVENT_MAX_COUNT, now).await?;
    }
    if removed > 0 {
        tracing::info!("pruned {removed} stale/excess deploy-report event(s)");
    }
    Ok(removed)
}

/// Throttled prune: runs [`prune_events`] at most once per
/// `PRUNE_MIN_INTERVAL_SECS`, so it can be called freely from the enqueue and
/// idle-poll paths without rescanning the directory on every event.
async fn maybe_prune_events(dir_path: Option<PathBuf>) {
    let now = now_unix();
    let last = LAST_PRUNE_UNIX.load(Ordering::Relaxed);
    if now.saturating_sub(last) < PRUNE_MIN_INTERVAL_SECS {
        return;
    }
    // Claim the slot before doing IO so concurrent callers don't all scan.
    if LAST_PRUNE_UNIX
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    if let Err(e) = prune_events(dir_path).await {
        tracing::warn!("event prune failed: {e}");
    }
}

// ---------------------------------------------------------------------------
// LocalRunState – per-unit persistent state stored in the workdir
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct LocalRunState {
    /// Hash of the `ServiceSpec` whose startup steps last ran successfully.
    /// `None`  → startup has not yet completed for the current spec.
    /// `Some(h)` → startup ran successfully for spec with hash `h`.
    #[serde(default)]
    pub last_run_hash: Option<String>,

    /// How many times startup has failed consecutively (drives backoff).
    #[serde(default)]
    pub startup_failures: u32,

    /// Timestamp (ms since epoch) of the last startup attempt.
    #[serde(default)]
    pub last_attempt_ms: Option<u64>,

    // Observe tracking -------------------------------------------------------
    #[serde(default)]
    pub consecutive_health_failures: u32,
    #[serde(default)]
    pub consecutive_alive_failures: u32,
    #[serde(default)]
    pub reported_health_once: bool,
    #[serde(default)]
    pub reported_alive_once: bool,
    #[serde(default)]
    pub last_health: bool,
    #[serde(default)]
    pub last_alive: bool,

    /// Runtime lifecycle override sent from the server via heartbeat.
    #[serde(default)]
    pub lifecycle: Lifecycle,
}

impl LocalRunState {
    fn state_file_path(work_dir: &Path) -> PathBuf {
        work_dir.join("run_state.json")
    }

    pub fn load(work_dir: &Path) -> Result<Self> {
        let path = Self::state_file_path(work_dir);
        if !path.exists() {
            return Ok(Self::default());
        }
        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("read run_state.json in {}", work_dir.display()))?;
        match serde_json::from_str(&contents) {
            Ok(st) => Ok(st),
            Err(e) => {
                // A corrupt / invalid run_state.json (a truncated write after a
                // power cut, or an incompatible-schema file from an older build)
                // must not wedge us. `load` is on the hot path — every reconcile
                // tick and every heartbeat handshake calls it — so returning Err
                // here makes the runtime fail at very high frequency. Self-heal:
                // discard the bad file and start from defaults; the next `save`
                // rewrites it cleanly.
                tracing::warn!(
                    "run_state.json in {} is invalid ({e}); deleting and resetting to defaults",
                    work_dir.display()
                );
                let _ = std::fs::remove_file(&path);
                Ok(Self::default())
            }
        }
    }

    pub fn save(work_dir: &Path, st: &Self) -> Result<()> {
        let path = Self::state_file_path(work_dir);
        let contents = serde_json::to_string_pretty(st).context("serialize LocalRunState")?;
        std::fs::write(&path, contents)
            .with_context(|| format!("write run_state.json in {}", work_dir.display()))
    }

    pub fn delete(work_dir: &Path) -> Result<()> {
        let path = Self::state_file_path(work_dir);
        if path.exists() {
            std::fs::remove_file(&path)
                .with_context(|| format!("delete run_state.json in {}", work_dir.display()))?;
        }
        Ok(())
    }

    /// Clear `last_run_hash` so the next reconcile re-runs startup steps.
    pub fn clear_run_hash(work_dir: &Path) -> Result<()> {
        let mut st = Self::load(work_dir)?;
        st.last_run_hash = None;
        Self::save(work_dir, &st)
    }

    fn failures_mut(&mut self, kind: ObserveKind) -> &mut u32 {
        match kind {
            ObserveKind::Liveness => &mut self.consecutive_alive_failures,
            ObserveKind::Health => &mut self.consecutive_health_failures,
        }
    }
}

// ---------------------------------------------------------------------------
// ObserveKind helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum ObserveKind {
    Liveness,
    Health,
}

impl Display for ObserveKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObserveKind::Liveness => write!(f, "liveness"),
            ObserveKind::Health => write!(f, "health"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ObserveDecision {
    is_failure: bool,
    needs_send: bool,
    consecutive: u32,
}

/// Test-only fault injection. If `M87_CRASH_AT` matches `point` and this point
/// hasn't fired before (one-shot via a marker file under `root_dir`), hard-exit
/// the process — emulating a power cut at that exact spot with no cleanup or
/// destructors, the way a Pi loses power mid-reconcile. No-op in production
/// (env unset). One-shot so a restarted runtime makes forward progress instead
/// of crash-looping on the same point.
fn maybe_crash(root_dir: &Path, point: &str) {
    // Trigger via env var OR a `<root>/.crash_at` file (the file is far more
    // robust to plumb through a container exec than an env var).
    let want = std::env::var("M87_CRASH_AT").ok().or_else(|| {
        std::fs::read_to_string(root_dir.join(".crash_at"))
            .ok()
            .map(|s| s.trim().to_string())
    });
    let want = match want {
        Some(v) if !v.is_empty() => v,
        _ => return,
    };
    if want != point {
        return;
    }
    // Emulate power loss. `m87 runtime run` is a supervised, multi-process tree,
    // so a single `process::exit` is just respawned — it does NOT take the
    // device down. Instead, signal that reconcile reached this point and then
    // FREEZE, so the test can SIGKILL the whole tree at exactly this spot (a
    // true power cut). Recovery clears `.crash_at`, so the restarted runtime
    // does not freeze again.
    let _ = std::fs::write(root_dir.join(format!(".reached_{point}")), b"1");
    tracing::error!("M87_CRASH_AT={point}: frozen at reconcile point for power-loss emulation");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

fn now_ms_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl ObserveKind {
    fn default_timeout(self) -> Duration {
        Duration::from_secs(5)
    }

    fn decide_on_success(&self, st: &LocalRunState) -> bool {
        match self {
            ObserveKind::Liveness => {
                !st.reported_alive_once || !st.last_alive || st.consecutive_alive_failures > 0
            }
            ObserveKind::Health => {
                !st.reported_health_once || !st.last_health || st.consecutive_health_failures > 0
            }
        }
    }

    fn decide_on_error(
        &self,
        st: &LocalRunState,
        hooks: &ObserveHooks,
        consecutive: u32,
    ) -> ObserveDecision {
        let fails_after = hooks.fails_after.unwrap_or(1);
        let is_failure = consecutive > 0 && fails_after > 0 && (consecutive % fails_after == 0);
        let consecutive_out = if fails_after == 0 {
            0
        } else {
            consecutive / fails_after
        };
        // Edge-trigger the report: only emit on a healthy -> unhealthy
        // transition, not on every `fails_after`-th consecutive failure. Once
        // we've reported the unit unhealthy we stay silent until it recovers
        // (the success path is likewise edge-triggered). Without this, a
        // persistently-failing observe — e.g. an error line stuck inside a
        // `logs --tail | grep` health check — re-enqueues a fresh RunState
        // every `fails_after * every` seconds for as long as the line stays in
        // the window. Because each RunState carries a unique `report_time` +
        // `log_tail`, the `enqueue_event` content-hash dedup can't collapse
        // them, so the pending-event queue grows unbounded and the 40/s drain
        // pins a core. `is_failure`/`consecutive` are left untouched so the
        // restart policy still fires on its own cadence.
        let previously_healthy = match self {
            ObserveKind::Liveness => !st.reported_alive_once || st.last_alive,
            ObserveKind::Health => !st.reported_health_once || st.last_health,
        };
        ObserveDecision {
            is_failure,
            needs_send: is_failure && previously_healthy,
            consecutive: consecutive_out,
        }
    }

    fn build_runstate_event(
        &self,
        run_id: &str,
        revision_id: &str,
        ok: bool,
        log_tail: Option<String>,
    ) -> RunState {
        let t = now_ms_u64();
        match (self, ok) {
            (ObserveKind::Liveness, true) => RunState {
                run_id: run_id.to_string(),
                revision_id: revision_id.to_string(),
                healthy: None,
                alive: Some(true),
                report_time: t,
                log_tail: None,
            },
            (ObserveKind::Liveness, false) => RunState {
                run_id: run_id.to_string(),
                revision_id: revision_id.to_string(),
                healthy: Some(false),
                alive: Some(false),
                report_time: t,
                log_tail,
            },
            (ObserveKind::Health, true) => RunState {
                run_id: run_id.to_string(),
                revision_id: revision_id.to_string(),
                healthy: Some(true),
                alive: Some(true),
                report_time: t,
                log_tail: None,
            },
            (ObserveKind::Health, false) => RunState {
                run_id: run_id.to_string(),
                revision_id: revision_id.to_string(),
                healthy: Some(false),
                alive: None,
                report_time: t,
                log_tail,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// RevisionStore – current + previous revision on disk
// ---------------------------------------------------------------------------

pub struct RevisionStore;

impl RevisionStore {
    fn desired_path(dir_path: Option<PathBuf>) -> Result<PathBuf> {
        Ok(data_dir(dir_path)?.join("desired_units.json"))
    }

    fn previous_path(dir_path: Option<PathBuf>) -> Result<PathBuf> {
        Ok(data_dir(dir_path)?.join("previous_units.json"))
    }

    pub fn get_previous_config(dir_path: Option<PathBuf>) -> Result<Option<DeploymentRevision>> {
        Self::read_revision_file(&Self::previous_path(dir_path)?)
    }

    pub fn get_desired_config(dir_path: Option<PathBuf>) -> Result<Option<DeploymentRevision>> {
        Self::read_revision_file(&Self::desired_path(dir_path)?)
    }

    /// Read and parse a stored revision file. A corrupt or incompatible-schema
    /// file (e.g. written by an older client) is NOT propagated as an error —
    /// that would wedge every reconcile cycle forever, forcing a manual file
    /// deletion. Instead we quarantine the bad file (`<name>.corrupt`) and
    /// return `None`, so the loop makes progress and the server can re-push a
    /// fresh revision on the next heartbeat.
    fn read_revision_file(path: &Path) -> Result<Option<DeploymentRevision>> {
        if !path.exists() {
            return Ok(None);
        }
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("read {}", path.display()))?;
        match serde_json::from_str::<DeploymentRevision>(&s) {
            Ok(c) => Ok(Some(c)),
            Err(e) => {
                let quarantine = path.with_file_name(format!(
                    "{}.corrupt",
                    path.file_name().and_then(|n| n.to_str()).unwrap_or("revision")
                ));
                tracing::warn!(
                    "failed to parse {}, quarantining to {} and continuing: {e}",
                    path.display(),
                    quarantine.display()
                );
                let _ = std::fs::rename(path, &quarantine);
                Ok(None)
            }
        }
    }

    /// Write a new desired config, backing up the current one to previous.
    pub fn set_config(config: &DeploymentRevision, dir_path: Option<PathBuf>) -> Result<()> {
        let desired = Self::desired_path(dir_path.clone())?;
        let previous = Self::previous_path(dir_path)?;
        if desired.exists() {
            std::fs::copy(&desired, &previous).context("backup desired → previous")?;
        }
        let s = serde_json::to_string_pretty(config).context("serialize DeploymentRevision")?;
        std::fs::write(&desired, s).context("write desired_units.json")
    }
}

// ---------------------------------------------------------------------------
// DeploymentManager
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct DeploymentManager {
    root_dir: PathBuf,
    /// Hashes of `ServiceSpec` items that need to be reconciled.
    dirty_services: Arc<RwLock<HashSet<String>>>,
    /// Hashes of observer `ServiceSpec` items that need scheduling updates.
    dirty_observers: Arc<RwLock<HashSet<String>>>,
    /// Queue of `JobRun` items waiting to be executed.
    pending_job_runs: Arc<RwLock<VecDeque<JobRun>>>,
    log_manager: LogManager,
}

impl DeploymentManager {
    pub async fn new(data_dir_path: Option<PathBuf>) -> Result<Self> {
        ensure_dirs(data_dir_path.clone()).await?;
        recover_inflight(data_dir_path.clone()).await?;
        let root_dir = data_dir(data_dir_path.clone())?;
        let log_manager = LogManager::start();
        Ok(Self {
            root_dir,
            dirty_services: Arc::new(RwLock::new(HashSet::new())),
            dirty_observers: Arc::new(RwLock::new(HashSet::new())),
            pending_job_runs: Arc::new(RwLock::new(VecDeque::new())),
            log_manager,
        })
    }

    pub fn get_current_deploy_hash(data_dir_path: Option<PathBuf>) -> String {
        RevisionStore::get_desired_config(data_dir_path)
            .ok()
            .flatten()
            .map(|c| c.get_hash())
            .unwrap_or_default()
    }

    pub async fn start_log_follow(&self) -> Result<()> {
        if let Some(spec) = RevisionStore::get_desired_config(Some(self.root_dir.clone()))? {
            for svc in spec.services.iter().chain(spec.observers.iter()) {
                if let Some(obs) = &svc.observe {
                    if let Some(log_spec) = &obs.logs {
                        let wd = self
                            .resolve_workdir_for(&svc.id, svc.workdir.as_ref())
                            .await?;
                        self.log_manager
                            .follow_start(svc.id.clone(), log_spec, svc.env.clone(), wd)
                            .await;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn stop_log_follow(&self, unit_id: &str) -> Result<()> {
        self.log_manager.follow_stop(unit_id.to_string()).await;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // set_desired_units – called when a new revision arrives via heartbeat
    // -----------------------------------------------------------------------

    pub async fn set_desired_units(
        &self,
        config: DeploymentRevision,
        lifecycle_updates: Vec<LifecycleUpdate>,
    ) -> Result<()> {
        let old = RevisionStore::get_desired_config(Some(self.root_dir.clone()))?;

        // Skip if nothing changed AND no lifecycle updates.
        if lifecycle_updates.is_empty() {
            if let Some(ref oc) = old {
                if oc.get_hash() == config.get_hash() {
                    return Ok(());
                }
            }
        }

        let new_svc_map = config.get_service_map();
        let new_obs_map = config.get_observer_map();

        let old_svc_map = old
            .as_ref()
            .map(|c| c.get_service_map())
            .unwrap_or_default();
        let old_obs_map = old
            .as_ref()
            .map(|c| c.get_observer_map())
            .unwrap_or_default();
        let old_svc_by_id: HashMap<String, String> = old_svc_map
            .iter()
            .map(|(h, s)| (s.id.clone(), h.clone()))
            .collect();
        let new_svc_by_id: HashMap<String, String> = new_svc_map
            .iter()
            .map(|(h, s)| (s.id.clone(), h.clone()))
            .collect();
        let old_obs_by_id: HashMap<String, String> = old_obs_map
            .iter()
            .map(|(h, o)| (o.id.clone(), h.clone()))
            .collect();
        let new_obs_by_id: HashMap<String, String> = new_obs_map
            .iter()
            .map(|(h, o)| (o.id.clone(), h.clone()))
            .collect();

        RevisionStore::set_config(&config, Some(self.root_dir.clone()))?;

        let mut ds = self.dirty_services.write().await;
        let mut dobs = self.dirty_observers.write().await;

        // --- Services -------------------------------------------------------
        // Added / changed
        for (h, svc) in &new_svc_map {
            if !old_svc_map.contains_key(h) {
                // Check if startup already ran for this hash.
                let wd = self
                    .resolve_workdir_for(&svc.id, svc.workdir.as_ref())
                    .await?;
                if let Ok(st) = LocalRunState::load(&wd) {
                    if st.last_run_hash.as_deref() != Some(h) {
                        ds.insert(h.clone());
                    }
                } else {
                    ds.insert(h.clone());
                }
            }
        }
        // Removed
        for (old_h, _) in &old_svc_map {
            if !new_svc_map.contains_key(old_h) {
                ds.insert(old_h.clone());
            }
        }
        // Same id, different hash (spec changed)
        for (id, old_h) in &old_svc_by_id {
            if let Some(new_h) = new_svc_by_id.get(id) {
                if new_h != old_h {
                    ds.insert(old_h.clone());
                    ds.insert(new_h.clone());
                }
            }
        }

        // --- Observers -------------------------------------------------------
        for (h, _) in &new_obs_map {
            if !old_obs_map.contains_key(h) {
                dobs.insert(h.clone());
            }
        }
        for (old_h, _) in &old_obs_map {
            if !new_obs_map.contains_key(old_h) {
                dobs.insert(old_h.clone());
            }
        }
        for (id, old_h) in &old_obs_by_id {
            if let Some(new_h) = new_obs_by_id.get(id) {
                if new_h != old_h {
                    dobs.insert(old_h.clone());
                    dobs.insert(new_h.clone());
                }
            }
        }

        drop(ds);
        drop(dobs);

        // Apply lifecycle updates
        if !lifecycle_updates.is_empty() {
            self.apply_lifecycle_updates_inner(&config, lifecycle_updates)
                .await?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // apply_lifecycle_updates – public entry-point (e.g. from control_tunnel)
    // -----------------------------------------------------------------------

    pub async fn apply_lifecycle_updates(&self, updates: Vec<LifecycleUpdate>) -> Result<()> {
        let rev =
            RevisionStore::get_desired_config(Some(self.root_dir.clone()))?.unwrap_or_default();
        self.apply_lifecycle_updates_inner(&rev, updates).await
    }

    async fn apply_lifecycle_updates_inner(
        &self,
        rev: &DeploymentRevision,
        updates: Vec<LifecycleUpdate>,
    ) -> Result<()> {
        let mut ds = self.dirty_services.write().await;

        for upd in updates {
            // Find this unit in services or observers
            let spec_opt = rev
                .get_service_by_id(&upd.unit_id)
                .or_else(|| rev.get_observer_by_id(&upd.unit_id));

            let Some(spec) = spec_opt else {
                tracing::warn!(
                    "lifecycle_update: unit '{}' not found in revision",
                    upd.unit_id
                );
                continue;
            };

            let wd = self
                .resolve_workdir_for(&spec.id, spec.workdir.as_ref())
                .await?;
            let mut st = LocalRunState::load(&wd)?;
            let prev_lifecycle = st.lifecycle.clone();
            st.lifecycle = upd.lifecycle.clone();
            LocalRunState::save(&wd, &st)?;

            let hash = spec.get_hash();
            match &upd.lifecycle {
                Lifecycle::Stopped => {
                    // Trigger stop reconcile
                    ds.insert(hash);
                }
                Lifecycle::Running => {
                    // If was stopped, need to start again
                    if prev_lifecycle.is_stopped() {
                        st.last_run_hash = None;
                        LocalRunState::save(&wd, &st)?;
                        ds.insert(hash);
                    }
                }
                Lifecycle::Paused => {
                    // No stop steps — just suspend observe. Nothing to reconcile.
                }
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // set_dirty_services – called on daemon restart to re-queue pending work
    // -----------------------------------------------------------------------

    async fn set_dirty_services(&self) -> Result<()> {
        let desired = match RevisionStore::get_desired_config(Some(self.root_dir.clone()))? {
            Some(c) => c,
            None => return Ok(()),
        };
        let desired_svcs = desired.get_service_map();
        let mut ds = self.dirty_services.write().await;
        for (hash, svc) in &desired_svcs {
            let wd = self
                .resolve_workdir_for(&svc.id, svc.workdir.as_ref())
                .await?;
            let st = LocalRunState::load(&wd).unwrap_or_default();
            if st.last_run_hash.as_deref() != Some(hash.as_str()) {
                ds.insert(hash.clone());
            }
        }

        // Re-queue pending teardowns a restart would otherwise forget. The dirty
        // set is in-memory, so a crash between "reconcile decided to stop a
        // removed unit" and "that stop confirmed" loses the intent — on restart
        // we'd rebuild work from the DESIRED revision only and start the
        // replacement while the old unit is still alive (two on the camera).
        // For every unit that was in the PREVIOUS revision, is gone from desired,
        // and whose stop was never confirmed (`last_run_hash` still set — and by
        // the stop-only-deletes-on-success invariant its workspace + stop steps
        // are therefore still on disk), re-queue it so reconcile's
        // `(Some(old), None) -> to_stop` path runs its stop before the update is
        // considered done.
        let desired_ids: HashSet<String> = desired_svcs.values().map(|s| s.id.clone()).collect();
        if let Some(prev) = RevisionStore::get_previous_config(Some(self.root_dir.clone()))? {
            for (hash, svc) in prev.get_service_map() {
                if desired_ids.contains(&svc.id) {
                    // Still desired (e.g. a same-id spec change) — handled by the
                    // normal dirty path above, not a teardown.
                    continue;
                }
                let wd = self
                    .resolve_workdir_for(&svc.id, svc.workdir.as_ref())
                    .await?;
                let st = LocalRunState::load(&wd).unwrap_or_default();
                if st.last_run_hash.is_some() {
                    ds.insert(hash);
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // enqueue_pending_job_runs – add incoming job runs to the local queue
    // -----------------------------------------------------------------------

    pub async fn enqueue_job_runs(&self, runs: Vec<JobRun>) {
        let mut q = self.pending_job_runs.write().await;
        for r in runs {
            q.push_back(r);
        }
    }

    /// Durably record how to stop a unit, next to the unit it starts. Written
    /// write-ahead (before the unit starts) so a reap can find its stop steps
    /// even after it's renamed away and dropped out of `previous`. Best-effort —
    /// a missing manifest only weakens multi-hop reap, never blocks a deploy.
    fn save_unit_manifest(wd: &Path, spec: &ServiceSpec) {
        if let Ok(json) = serde_json::to_string_pretty(spec) {
            let _ = std::fs::write(wd.join("unit.json"), json);
        }
    }

    /// Tear down any orphan no longer reachable through the desired/previous
    /// revisions. `set_dirty_services` (+`previous`) covers a single-hop rename,
    /// but `previous` only holds one revision back — after two quick renames an
    /// interrupted teardown is in NEITHER previous nor desired. For every
    /// workspace whose unit still looks running (`last_run_hash` set = stop never
    /// confirmed) and whose id is in neither revision, run the stop steps
    /// recorded in its `unit.json` ledger. Units still in `previous` are left to
    /// `set_dirty_services` (avoids a double stop). Fully agnostic — it just
    /// re-runs the unit's own recorded stop.
    pub(crate) async fn reap_orphaned_units(&self) -> Result<()> {
        // Only reap when we actually know the desired state. If the config is
        // absent/unreadable, `keep` would be empty and we'd tear down every
        // workspace — a transient read miss must not nuke running units.
        let desired = match RevisionStore::get_desired_config(Some(self.root_dir.clone()))? {
            Some(d) => Some(d),
            None => return Ok(()),
        };
        let previous = RevisionStore::get_previous_config(Some(self.root_dir.clone()))?;
        let keep: HashSet<String> = desired
            .iter()
            .chain(previous.iter())
            .flat_map(|r| r.services.iter().map(|s| s.id.clone()))
            .collect();
        let rev = desired.as_ref().and_then(|d| d.id.clone()).unwrap_or_default();

        let ws_root = self.root_dir.join("workspaces");
        let mut rd = match tokio::fs::read_dir(&ws_root).await {
            Ok(rd) => rd,
            Err(_) => return Ok(()),
        };
        while let Some(entry) = rd.next_entry().await? {
            if !entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let id = match entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };
            if keep.contains(&id) {
                continue;
            }
            let dir = entry.path();
            // Only reap a unit whose stop was never confirmed.
            if LocalRunState::load(&dir)
                .unwrap_or_default()
                .last_run_hash
                .is_none()
            {
                continue;
            }
            let spec: ServiceSpec = match std::fs::read_to_string(dir.join("unit.json"))
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
            {
                Some(s) => s,
                None => continue, // no ledger -> nothing to run (agnostic)
            };
            tracing::info!("reaping orphaned unit '{id}' from teardown ledger");
            if let Err(e) = self.stop_service(&spec, &rev, &dir).await {
                tracing::error!("reap of orphaned unit '{id}' failed: {e:#}");
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // start – the main supervisor loop
    // -----------------------------------------------------------------------

    /// One-time cleanup of pre-0.8 (0.7.x) deployment workspaces.
    ///
    /// 0.7.x ran compose services under `<root>/jobs/<id>` (persistent) and
    /// `<root>/tmp/jobs/<hash>` (ephemeral), so a unit's docker-compose project
    /// name was that directory's basename. 0.8.x instead uses
    /// `<root>/workspaces/<id>`, so after an upgrade the containers 0.7.x started
    /// are orphaned under project names the new client never references: they
    /// keep running (and docker's restart policy resurrects them on reboot),
    /// fighting the 0.8.x containers for resources.
    ///
    /// These two directories are NEVER created by 0.8.x, so reaping them is
    /// safe: for each leftover workspace we run `docker compose down` from inside
    /// it (using the compose file 0.7.x materialized there, hence the same
    /// project name it created the containers with), then remove the directory.
    /// Best-effort throughout — a device with no leftovers is a no-op, and after
    /// the first successful pass the directories are gone so it stays a no-op.
    pub(crate) async fn reap_legacy_workspaces(&self) {
        for sub in ["jobs", "tmp/jobs"] {
            let base = self.root_dir.join(sub);
            let mut rd = match tokio::fs::read_dir(&base).await {
                Ok(rd) => rd,
                Err(_) => continue, // directory absent → nothing to reap
            };
            while let Ok(Some(entry)) = rd.next_entry().await {
                let path = entry.path();
                let is_dir = entry
                    .file_type()
                    .await
                    .map(|t| t.is_dir())
                    .unwrap_or(false);
                if !is_dir {
                    continue;
                }
                tracing::warn!(
                    "reaping pre-0.8 deployment workspace {} (orphaned by the 0.7.x→0.8.x upgrade)",
                    path.display()
                );
                // Tear the old compose project down. Running `down` from the
                // leftover workspace picks up its materialized compose file and
                // the same (basename-derived) project name 0.7.x used, so this
                // targets exactly those containers. Best-effort: no compose file,
                // no docker, or an already-gone project all no-op.
                let cmd = format!(
                    "cd '{}' && docker compose down --remove-orphans",
                    path.display()
                );
                let _ = tokio::process::Command::new("/bin/sh")
                    .arg("-lc")
                    .arg(&cmd)
                    .output()
                    .await;
                let _ = tokio::fs::remove_dir_all(&path).await;
            }
            // Remove the now-empty legacy parent dir so the reap is a clean no-op next boot.
            let _ = tokio::fs::remove_dir(&base).await;
        }
    }

    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut next_health: HashMap<String, Instant> = HashMap::new();
            let mut next_liveness: HashMap<String, Instant> = HashMap::new();
            let tick = Duration::from_millis(250);

            // Reap 0.7.x-era orphaned compose projects before reconciling, so an
            // upgraded device doesn't run duplicate containers fighting for
            // resources. Safe/no-op on a device that was always on 0.8.x.
            self.reap_legacy_workspaces().await;

            // Tear down multi-hop orphans (an interrupted teardown that has since
            // fallen out of `previous`) from the durable per-unit ledger, before
            // reconciling — otherwise they'd run alongside the current revision.
            if let Err(e) = self.reap_orphaned_units().await {
                tracing::error!("reap_orphaned_units failed: {e:#}");
            }

            let _ = self.set_dirty_services().await;

            loop {
                if SHUTDOWN.is_cancelled() {
                    break;
                }

                // 1) Reconcile dirty services
                if let Err(e) = self.reconcile_dirty().await {
                    tracing::error!("reconcile error: {e}");
                    if let Ok(Some(desired)) =
                        RevisionStore::get_desired_config(Some(self.root_dir.clone()))
                    {
                        let _ = enqueue_event(
                            DeployReportKind::DeploymentRevisionReport(DeploymentRevisionReport {
                                revision_id: desired.id.clone().unwrap_or_default(),
                                outcome: Outcome::Failed,
                                dirty: true,
                                error: Some(format!("reconcile error: {e}")),
                            }),
                            Some(self.root_dir.clone()),
                        )
                        .await;
                    }
                }

                // 2) Drain pending job runs (each gets its own task)
                {
                    let mut q = self.pending_job_runs.write().await;
                    while let Some(job_run) = q.pop_front() {
                        let desired =
                            RevisionStore::get_desired_config(Some(self.root_dir.clone()))
                                .ok()
                                .flatten();
                        if let Some(def) = desired
                            .as_ref()
                            .and_then(|r| r.get_job_by_id(&job_run.job_def_id))
                        {
                            let mgr = self.clone();
                            tokio::spawn(async move {
                                let _ = mgr.execute_job_run(job_run, &def).await;
                            });
                        } else {
                            tracing::warn!(
                                "job_run: job def '{}' not found in revision",
                                job_run.job_def_id
                            );
                        }
                    }
                }

                // 3) Schedule observe checks for services + observers
                let now = Instant::now();
                let desired_spec =
                    match RevisionStore::get_desired_config(Some(self.root_dir.clone())) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!("failed to read desired config: {e}");
                            sleep(tick).await;
                            continue;
                        }
                    };

                if let Some(spec) = desired_spec {
                    let revision_id = spec.id.clone().unwrap_or_default();

                    // Iterate services + observers that have observe hooks
                    for svc in spec.services.iter().chain(spec.observers.iter()) {
                        let Some(obs) = &svc.observe else {
                            continue;
                        };
                        // Check runtime lifecycle override
                        let wd_res = self
                            .resolve_workdir_for(&svc.id, svc.workdir.as_ref())
                            .await;
                        let is_paused = match wd_res {
                            Ok(ref wd) => LocalRunState::load(wd)
                                .map(|st| st.lifecycle.is_paused())
                                .unwrap_or(false),
                            Err(_) => false,
                        };
                        if is_paused || svc.lifecycle.is_paused() {
                            continue;
                        }
                        // Spec-level stopped → don't poll
                        if svc.lifecycle.is_stopped() {
                            continue;
                        }

                        let hash = svc.get_hash();

                        if let Some(liveness) = &obs.liveness {
                            let due = next_liveness.get(&hash).copied().unwrap_or(now);
                            if now >= due {
                                next_liveness.insert(hash.clone(), now + liveness.every);
                                let _ = self
                                    .run_observe_check(
                                        ObserveKind::Liveness,
                                        &svc.id,
                                        &revision_id,
                                        svc,
                                        liveness,
                                    )
                                    .await;
                            }
                        }
                        if let Some(health) = &obs.health {
                            let due = next_health.get(&hash).copied().unwrap_or(now);
                            if now >= due {
                                next_health.insert(hash.clone(), now + health.every);
                                let _ = self
                                    .run_observe_check(
                                        ObserveKind::Health,
                                        &svc.id,
                                        &revision_id,
                                        svc,
                                        health,
                                    )
                                    .await;
                            }
                        }
                    }
                }

                sleep(tick).await;
            }
        });
    }

    // -----------------------------------------------------------------------
    // reconcile_dirty – services only
    // -----------------------------------------------------------------------

    pub(crate) async fn reconcile_dirty(&self) -> Result<()> {
        let dirty_hashes: Vec<String> = {
            let d = self.dirty_services.read().await;
            if d.is_empty() {
                return Ok(());
            }
            d.iter().cloned().collect()
        };

        let desired_cfg = RevisionStore::get_desired_config(Some(self.root_dir.clone()))?
            .ok_or_else(|| anyhow!("no desired config"))?;
        let desired_rev = desired_cfg.id.clone().unwrap_or_default();

        let prev_cfg = RevisionStore::get_previous_config(Some(self.root_dir.clone()))?;
        let prev_rev = prev_cfg.as_ref().and_then(|c| c.id.clone());

        let mut to_stop: Vec<ServiceSpec> = Vec::new();
        let mut to_start: Vec<ServiceSpec> = Vec::new();

        for h in &dirty_hashes {
            let new_spec = desired_cfg.get_service_by_hash(h);
            let old_spec = prev_cfg.as_ref().and_then(|c| c.get_service_by_hash(h));

            match (old_spec, new_spec) {
                // Removed from desired
                (Some(old), None) => {
                    to_stop.push(old);
                }
                // Present in both
                (Some(old), Some(new)) => {
                    // Stopped lifecycle → stop
                    let wd = self
                        .resolve_workdir_for(&new.id, new.workdir.as_ref())
                        .await?;
                    let st = LocalRunState::load(&wd).unwrap_or_default();
                    let effective_stopped = new.lifecycle.is_stopped() || st.lifecycle.is_stopped();

                    if effective_stopped {
                        to_stop.push(new.clone());
                    } else if old.get_hash() != new.get_hash() {
                        // Spec changed → stop old, start new
                        to_stop.push(old);
                        to_start.push(new);
                    } else {
                        // Same hash, same id — may need startup if hash never ran
                        to_start.push(new);
                    }
                }
                // Added
                (None, Some(new)) => {
                    let wd = self
                        .resolve_workdir_for(&new.id, new.workdir.as_ref())
                        .await?;
                    let st = LocalRunState::load(&wd).unwrap_or_default();
                    let effective_stopped = new.lifecycle.is_stopped() || st.lifecycle.is_stopped();
                    if effective_stopped {
                        // Was previously started (hash exists) but now needs to stop
                        if st.last_run_hash.is_some() {
                            to_stop.push(new);
                        }
                    } else {
                        to_start.push(new);
                    }
                }
                (None, None) => {}
            }
        }

        // Dedup by id
        let mut stop_map: HashMap<String, ServiceSpec> = HashMap::new();
        for s in to_stop {
            stop_map.insert(s.id.clone(), s);
        }
        let mut start_map: HashMap<String, ServiceSpec> = HashMap::new();
        for s in to_start {
            start_map.insert(s.id.clone(), s);
        }

        // Reconcile has decided a unit must be stopped but hasn't run its stop
        // yet. Crash here to test that the teardown intent survives a restart
        // (today it does not — the dirty set is in-memory and rebuilt from the
        // desired revision only, so a removed unit's pending stop is forgotten).
        maybe_crash(&self.root_dir, "before_stops");

        // Phase 2a: stop first. A failing stop step must NOT be silently
        // swallowed — it leaves a half-torn-down unit. Record the failure so the
        // service stays dirty (and is retried) and the error is surfaced to the
        // caller, rather than reconcile reporting success.
        let mut failed_hashes: HashSet<String> = HashSet::new();
        for (_, spec) in &stop_map {
            let wd = self
                .resolve_workdir_for(&spec.id, spec.workdir.as_ref())
                .await?;
            let rev = prev_rev.clone().unwrap_or_else(|| desired_rev.clone());
            if let Err(e) = self.stop_service(spec, &rev, &wd).await {
                tracing::error!("stop_service for '{}' failed: {e:#}", spec.id);
                failed_hashes.insert(spec.get_hash());
            }
        }

        // Old units have been stopped; new ones not yet started. Crash here to
        // test that a power cut mid-transition converges to a single unit.
        maybe_crash(&self.root_dir, "after_stop_before_start");

        // Phase 2b: start — but ONLY if every stop this pass succeeded. A unit
        // whose stop failed is still running; bringing the new revision up on
        // top of it means two containers run at once, contending for the same
        // exclusive hardware (camera / LTE / …) and taking the device down. When
        // any stop failed we skip ALL starts, keep the to-start work dirty, and
        // retry on the next reconcile once the old unit is actually gone. Leaving
        // the old unit running is stable; running both is the fatal state.
        if failed_hashes.is_empty() {
            // A failing start must NOT abort the whole reconcile via `?`: every
            // sibling not yet processed would be skipped and left "pending" with
            // no error of its own, and since `start_map` iteration order is
            // nondeterministic the skipped set varies per run (the flaky
            // "re-deploy sometimes fixes it" behaviour). Collect failures per
            // unit instead, mirroring the stop phase.
            for (_, spec) in &start_map {
                let wd = self
                    .resolve_workdir_for(&spec.id, spec.workdir.as_ref())
                    .await?;
                if let Err(e) = self.apply_service(spec, &desired_rev, &wd).await {
                    tracing::error!("apply_service for '{}' failed: {e:#}", spec.id);
                    failed_hashes.insert(spec.get_hash());
                }
            }
        } else {
            tracing::warn!(
                "{} stop step(s) failed; deferring {} start(s) to avoid running \
                 old and new units concurrently",
                failed_hashes.len(),
                start_map.len()
            );
            // Keep the deferred starts dirty so they are applied once the stops
            // succeed on a later pass.
            for (_, spec) in &start_map {
                failed_hashes.insert(spec.get_hash());
            }
        }

        // Clear processed hashes, but keep any that failed (stop or start) or
        // that were deferred because a stop blocked their start, so they are
        // retried on the next reconcile instead of being dropped.
        let mut ds = self.dirty_services.write().await;
        for h in dirty_hashes {
            if !failed_hashes.contains(&h) {
                ds.remove(&h);
            }
        }
        drop(ds);

        if !failed_hashes.is_empty() {
            return Err(anyhow!(
                "{} unit(s) failed or were deferred during reconcile",
                failed_hashes.len()
            ));
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // apply_service – hash-based idempotency with exponential backoff
    // -----------------------------------------------------------------------

    pub(crate) async fn apply_service(
        &self,
        spec: &ServiceSpec,
        revision_id: &str,
        wd: &Path,
    ) -> Result<()> {
        let mut st = LocalRunState::load(wd)?;

        // Already ran for this exact spec version
        if st.last_run_hash.as_deref() == Some(&spec.get_hash()) {
            return Ok(());
        }

        // Exponential backoff after failures (cap at 64 s)
        if st.startup_failures > 0 {
            let backoff_ms = 1000 * 2u64.pow(st.startup_failures.min(6));
            if let Some(last) = st.last_attempt_ms {
                let elapsed = now_ms_u64().saturating_sub(last);
                if elapsed < backoff_ms {
                    return Ok(());
                }
            }
        }

        st.last_attempt_ms = Some(now_ms_u64());
        LocalRunState::save(wd, &st)?;

        self.materialize_files_svc(spec, wd).await?;

        // Record this unit's teardown ledger BEFORE starting it (write-ahead), so
        // that even if it's later renamed away and falls out of `previous`, a
        // boot-time reap can still find its stop steps and tear it down. See
        // `reap_orphaned_units`.
        Self::save_unit_manifest(wd, spec);

        let result = self
            .execute_steps(
                &spec.id,
                revision_id,
                wd,
                &spec.env,
                &spec.steps,
                spec.on_failure.as_ref(),
            )
            .await;

        match result {
            Ok(()) => {
                // Start steps ran (container is up) but success is NOT yet
                // recorded — crash here to test idempotent re-run on restart.
                maybe_crash(&self.root_dir, "after_start_steps");
                let mut st2 = LocalRunState::load(wd)?;
                st2.last_run_hash = Some(spec.get_hash());
                st2.startup_failures = 0;
                st2.last_attempt_ms = None;
                LocalRunState::save(wd, &st2)?;
                let _ = enqueue_event(
                    DeployReportKind::RunReport(RunReport {
                        run_id: spec.id.clone(),
                        revision_id: revision_id.to_string(),
                        outcome: Outcome::Success,
                        report_time: now_ms_u64(),
                        error: None,
                    }),
                    Some(self.root_dir.clone()),
                )
                .await;
                Ok(())
            }
            Err(e) => {
                let mut st2 = LocalRunState::load(wd)?;
                st2.startup_failures += 1;
                st2.last_attempt_ms = Some(now_ms_u64());
                LocalRunState::save(wd, &st2)?;
                let _ = enqueue_event(
                    DeployReportKind::RunReport(RunReport {
                        run_id: spec.id.clone(),
                        revision_id: revision_id.to_string(),
                        outcome: Outcome::Failed,
                        report_time: now_ms_u64(),
                        error: Some(e.to_string()),
                    }),
                    Some(self.root_dir.clone()),
                )
                .await;
                Err(e)
            }
        }
    }

    // -----------------------------------------------------------------------
    // stop_service
    // -----------------------------------------------------------------------

    pub(crate) async fn stop_service(
        &self,
        spec: &ServiceSpec,
        revision_id: &str,
        wd: &Path,
        // force=true skips stop steps
    ) -> Result<()> {
        self.stop_log_follow(&spec.id).await?;

        if let Some(stop) = &spec.stop {
            self.execute_steps(
                &spec.id,
                revision_id,
                wd,
                &spec.env,
                &stop.steps,
                spec.on_failure.as_ref(),
            )
            .await?;
        }

        // Clear the run hash so startup re-runs if the service is later resumed.
        // We do NOT delete the state file for persistent workdirs because the
        // `lifecycle` field must survive the stop so that `apply_lifecycle_updates`
        // can detect the transition correctly on resume.
        let mut st = LocalRunState::load(wd).unwrap_or_default();
        st.last_run_hash = None;
        st.startup_failures = 0;
        LocalRunState::save(wd, &st)?;

        if let Some(workdir) = &spec.workdir {
            let ws = self.get_workspace_path_for(&spec.id, spec.workdir.as_ref())?;
            match workdir.mode {
                WorkdirMode::Persistent => {
                    // State file is preserved so that lifecycle tracking survives the stop.
                    // last_run_hash was already cleared above.
                }
                WorkdirMode::Ephemeral => {
                    let _ = tokio::fs::remove_dir_all(&ws).await;
                }
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // execute_job_run – runs a triggered JobRun
    // -----------------------------------------------------------------------

    pub(crate) async fn execute_job_run(&self, run: JobRun, def: &JobDef) -> Result<()> {
        // Merge env: def env overridden by run env_overrides
        let mut env = def.env.clone();
        for (k, v) in &run.env_overrides {
            env.insert(k.clone(), v.clone());
        }

        let wd = self
            .resolve_workdir_for(&def.id, def.workdir.as_ref())
            .await?;

        // Materialize files from the job definition
        self.materialize_files_job(def, &wd).await?;

        let result = self
            .execute_steps(
                &run.run_id,
                &run.revision_id,
                &wd,
                &env,
                &def.steps,
                def.on_failure.as_ref(),
            )
            .await;

        let (status, error) = match &result {
            Ok(()) => (JobRunStatus::Success, None),
            Err(e) => (JobRunStatus::Failed, Some(e.to_string())),
        };

        let _ = enqueue_event(
            DeployReportKind::JobRunReport(JobRunReport {
                run_id: run.run_id.clone(),
                job_def_id: run.job_def_id.clone(),
                revision_id: run.revision_id.clone(),
                status,
                report_time: now_ms_u64(),
                error,
            }),
            Some(self.root_dir.clone()),
        )
        .await;

        result
    }

    // -----------------------------------------------------------------------
    // Observe checks
    // -----------------------------------------------------------------------

    async fn run_observe_check(
        &self,
        kind: ObserveKind,
        run_id: &str,
        revision_id: &str,
        spec: &ServiceSpec,
        hooks: &ObserveHooks,
    ) -> Result<()> {
        let r = self
            .run_observe(kind, run_id, revision_id, spec, hooks)
            .await;

        match r {
            Ok(d) if d.consecutive > 0 => {
                tracing::info!(
                    "{} check had {} consecutive failures for '{}'",
                    kind,
                    d.consecutive,
                    run_id
                );
                let _ = self
                    .check_rollback_on_observe_failure(kind, revision_id, Some(d.consecutive), spec)
                    .await;
                Ok(())
            }
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn run_observe(
        &self,
        kind: ObserveKind,
        run_id: &str,
        revision_id: &str,
        spec: &ServiceSpec,
        hooks: &ObserveHooks,
    ) -> Result<ObserveDecision> {
        let wd = self
            .resolve_workdir_for(&spec.id, spec.workdir.as_ref())
            .await?;
        let mut st = LocalRunState::load(&wd)?;

        let timeout = hooks
            .observe_timeout
            .unwrap_or_else(|| kind.default_timeout());

        let res = run_command(
            run_id,
            &wd,
            &spec.env,
            &hooks.observe,
            Some(timeout),
            MAX_TAIL_BYTES,
        )
        .await;

        match res {
            Ok(_) => {
                *st.failures_mut(kind) = 0;
                let needs_send = kind.decide_on_success(&st);
                LocalRunState::save(&wd, &st)?;

                if needs_send {
                    let _ = enqueue_event(
                        DeployReportKind::RunState(kind.build_runstate_event(
                            run_id,
                            revision_id,
                            true,
                            None,
                        )),
                        Some(self.root_dir.clone()),
                    )
                    .await;
                    let mut st2 = LocalRunState::load(&wd)?;
                    match kind {
                        ObserveKind::Health => {
                            st2.reported_health_once = true;
                            st2.last_health = true;
                            st2.consecutive_health_failures = 0;
                        }
                        ObserveKind::Liveness => {
                            st2.reported_alive_once = true;
                            st2.last_alive = true;
                            st2.consecutive_alive_failures = 0;
                        }
                    }
                    LocalRunState::save(&wd, &st2)?;
                }

                Ok(ObserveDecision {
                    is_failure: false,
                    needs_send,
                    consecutive: 0,
                })
            }
            Err(e) => {
                let log_tail = match &e {
                    RunCommandError::Failed(f) => Some(f.combined_tail.clone()),
                    _ => None,
                };
                *st.failures_mut(kind) += 1;
                let consecutive = *st.failures_mut(kind);
                let decision = kind.decide_on_error(&st, hooks, consecutive);

                LocalRunState::save(&wd, &st)?;

                if decision.needs_send {
                    let _ = enqueue_event(
                        DeployReportKind::RunState(kind.build_runstate_event(
                            run_id,
                            revision_id,
                            false,
                            log_tail,
                        )),
                        Some(self.root_dir.clone()),
                    )
                    .await;
                    let mut st2 = LocalRunState::load(&wd)?;
                    match kind {
                        ObserveKind::Health => {
                            st2.last_health = false;
                            st2.reported_health_once = true;
                        }
                        ObserveKind::Liveness => {
                            st2.last_alive = false;
                            st2.reported_alive_once = true;
                        }
                    }
                    LocalRunState::save(&wd, &st2)?;
                }

                Ok(decision)
            }
        }
    }

    // -----------------------------------------------------------------------
    // Restart policy
    // -----------------------------------------------------------------------
    //
    // Auto-rollback-on-observe-failure was removed when we collapsed to a
    // single-revision model. The remaining recovery path is per-service
    // `RestartPolicy` — re-runs startup steps in place, no revision swap.

    async fn check_rollback_on_observe_failure(
        &self,
        _kind: ObserveKind,
        _revision_id: &str,
        _consecutive: Option<u32>,
        spec: &ServiceSpec,
    ) -> Result<()> {
        self.maybe_restart_service(spec).await
    }

    async fn maybe_restart_service(&self, spec: &ServiceSpec) -> Result<()> {
        match spec.restart {
            RestartPolicy::Never => {}
            RestartPolicy::OnFailure | RestartPolicy::Always => {
                let wd = self
                    .resolve_workdir_for(&spec.id, spec.workdir.as_ref())
                    .await?;
                LocalRunState::clear_run_hash(&wd)?;
                self.dirty_services.write().await.insert(spec.get_hash());
                tracing::info!("restart policy: re-queuing '{}'", spec.id);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Step execution
    // -----------------------------------------------------------------------

    async fn execute_steps(
        &self,
        run_id: &str,
        revision_id: &str,
        wd: &Path,
        env: &BTreeMap<String, String>,
        steps: &[Step],
        on_failure: Option<&OnFailure>,
    ) -> Result<()> {
        let undo_mode = on_failure.map(|f| f.undo.clone()).unwrap_or(UndoMode::None);
        let continue_on_failure = on_failure.map(|f| f.continue_on_failure).unwrap_or(false);

        let mut executed: Vec<&Step> = Vec::new();
        let mut any_failed = false;

        for step in steps {
            let res = self
                .run_step_with_retry(run_id, revision_id, wd, env, step, false)
                .await;
            match res {
                Ok(()) => {
                    executed.push(step);
                }
                Err(e) => {
                    any_failed = true;
                    tracing::error!(
                        "step '{}' failed: {e}",
                        step.name.as_deref().unwrap_or("unnamed")
                    );
                    if !continue_on_failure {
                        if matches!(undo_mode, UndoMode::ExecutedSteps) {
                            self.undo_steps(run_id, revision_id, wd, env, &executed)
                                .await;
                        }
                        return Err(e);
                    }
                }
            }
        }

        if any_failed {
            return Err(anyhow!("one or more steps failed"));
        }
        Ok(())
    }

    async fn undo_steps(
        &self,
        run_id: &str,
        revision_id: &str,
        wd: &Path,
        env: &BTreeMap<String, String>,
        steps: &[&Step],
    ) {
        for step in steps.iter().rev() {
            if let Some(undo) = &step.undo {
                let undo_step = Step {
                    name: step.name.as_ref().map(|n| format!("{} (undo)", n)),
                    run: undo.run.clone(),
                    timeout: undo.timeout,
                    retry: None,
                    undo: None,
                };
                let _ = self
                    .run_step_with_retry(run_id, revision_id, wd, env, &undo_step, true)
                    .await;
            }
        }
    }

    async fn run_step_with_retry(
        &self,
        run_id: &str,
        revision_id: &str,
        wd: &Path,
        env: &BTreeMap<String, String>,
        step: &Step,
        is_undo: bool,
    ) -> Result<()> {
        let max_attempts = step.retry.as_ref().map(|r| r.attempts.max(1)).unwrap_or(1);
        let backoff = step
            .retry
            .as_ref()
            .and_then(|r| r.backoff)
            .unwrap_or(Duration::from_secs(1));

        let mut last_err = None;
        for attempt in 1..=max_attempts {
            match self
                .run_step(run_id, revision_id, wd, env, step, attempt, is_undo)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < max_attempts {
                        sleep(backoff).await;
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("step failed")))
    }

    async fn run_step(
        &self,
        run_id: &str,
        revision_id: &str,
        wd: &Path,
        env: &BTreeMap<String, String>,
        step: &Step,
        attempt: u32,
        is_undo: bool,
    ) -> Result<()> {
        // Always pass a timeout to run_command. `step.timeout = None` would
        // otherwise mean "wait forever", and a hung user command (e.g. a
        // setup script blocked on an unreachable network resource) blocks
        // `execute_steps` → blocks `reconcile_dirty` → blocks the entire
        // supervisor loop, which is the classic "runtime stuck" signature.
        // 1 hour is a generous upper bound that won't cut off legitimate
        // long-running setup work while still bounding the worst case.
        let effective_timeout = step
            .timeout
            .unwrap_or_else(|| Duration::from_secs(3600));
        let res = run_command(
            run_id,
            wd,
            env,
            &step.run,
            Some(effective_timeout),
            MAX_TAIL_BYTES,
        )
        .await;

        let (success, exit_code, error_msg, log_tail) = match &res {
            Ok(out) => (true, Some(0i32), None::<String>, out.clone()),
            Err(RunCommandError::Failed(f)) => (
                false,
                f.exit_code,
                Some(
                    f.error
                        .clone()
                        .unwrap_or_else(|| format!("exit code {:?}", f.exit_code)),
                ),
                f.combined_tail.clone(),
            ),
            Err(e) => (false, None, Some(e.to_string()), String::new()),
        };

        let tail = merge_log_tails("", &log_tail, MAX_TAIL_BYTES);

        let _ = enqueue_event(
            DeployReportKind::StepReport(StepReport {
                revision_id: revision_id.to_string(),
                run_id: run_id.to_string(),
                name: step.name.clone(),
                attempts: attempt,
                exit_code,
                report_time: now_ms_u64(),
                success,
                is_undo,
                error: error_msg.clone(),
                log_tail: if tail.is_empty() { None } else { Some(tail) },
            }),
            Some(self.root_dir.clone()),
        )
        .await;

        if success {
            Ok(())
        } else {
            Err(anyhow!(
                error_msg.unwrap_or_else(|| "step failed".to_string())
            ))
        }
    }

    // -----------------------------------------------------------------------
    // Workspace helpers
    // -----------------------------------------------------------------------

    pub(crate) fn get_workspace_path_for(
        &self,
        id: &str,
        workdir: Option<&m87_shared::deploy_spec::Workdir>,
    ) -> Result<PathBuf> {
        if let Some(wd) = workdir {
            if let Some(path) = &wd.path {
                return Ok(PathBuf::from(path));
            }
        }
        Ok(self.root_dir.join("workspaces").join(id))
    }

    pub(crate) async fn resolve_workdir_for(
        &self,
        id: &str,
        workdir: Option<&m87_shared::deploy_spec::Workdir>,
    ) -> Result<PathBuf> {
        let path = self.get_workspace_path_for(id, workdir)?;
        fs::create_dir_all(&path).await?;
        Ok(path)
    }

    async fn materialize_files_svc(&self, spec: &ServiceSpec, wd: &Path) -> Result<()> {
        for (name, content) in &spec.files {
            let file_path = wd.join(name);
            let mut f = fs::File::create(&file_path).await?;
            f.write_all(content.as_bytes()).await?;
        }
        Ok(())
    }

    async fn materialize_files_job(&self, def: &JobDef, wd: &Path) -> Result<()> {
        for (name, content) in &def.files {
            let file_path = wd.join(name);
            let mut f = fs::File::create(&file_path).await?;
            f.write_all(content.as_bytes()).await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Event queue (file-based, crash-safe)
// ---------------------------------------------------------------------------

pub async fn enqueue_event(kind: DeployReportKind, dir_path: Option<PathBuf>) -> Result<()> {
    // Trim stale/excess events before adding another, so the folder can never
    // grow without bound even under a write storm (throttled internally).
    maybe_prune_events(dir_path.clone()).await;
    let pending = pending_dir(dir_path.clone())?;
    let hash = kind.get_hash();
    let path = pending.join(format!("{}.json", hash));
    if path.exists() {
        return Ok(());
    }
    let s = serde_json::to_string(&kind).context("serialize event")?;
    fs::write(&path, s).await.context("write pending event")?;
    Ok(())
}

pub struct ClaimedEvent {
    pub path: PathBuf,
    pub report: DeployReportKind,
}

pub async fn recover_inflight(dir_path: Option<PathBuf>) -> Result<()> {
    let inflight = inflight_dir(dir_path.clone())?;
    let pending = pending_dir(dir_path)?;
    let mut dir = match fs::read_dir(&inflight).await {
        Ok(d) => d,
        Err(_) => return Ok(()),
    };
    while let Ok(Some(entry)) = dir.next_entry().await {
        let src = entry.path();
        let dst = pending.join(entry.file_name());
        let _ = fs::rename(&src, &dst).await;
    }
    Ok(())
}

pub async fn claim_next_event(dir_path: Option<PathBuf>) -> Result<Option<ClaimedEvent>> {
    let pending = pending_dir(dir_path.clone())?;
    let inflight = inflight_dir(dir_path)?;
    let mut dir = match fs::read_dir(&pending).await {
        Ok(d) => d,
        Err(_) => return Ok(None),
    };
    while let Ok(Some(entry)) = dir.next_entry().await {
        let src = entry.path();
        if src.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let dst = inflight.join(entry.file_name());
        if fs::rename(&src, &dst).await.is_err() {
            continue;
        }
        let s = match fs::read_to_string(&dst).await {
            Ok(s) => s,
            Err(e) => {
                // Unreadable / non-UTF-8 event: drop it rather than leaving it
                // to be replayed from inflight on every restart.
                tracing::warn!("failed to read event {dst:?}, removing: {e}");
                let _ = fs::remove_file(&dst).await;
                continue;
            }
        };
        match serde_json::from_str::<DeployReportKind>(&s) {
            Ok(report) => {
                return Ok(Some(ClaimedEvent { path: dst, report }));
            }
            Err(e) => {
                tracing::warn!("failed to parse event {dst:?}, removing: {e}");
                let _ = fs::remove_file(&dst).await;
            }
        }
    }
    Ok(None)
}

pub async fn ack_event(hash: &str, dir_path: Option<PathBuf>) -> Result<()> {
    let inflight = inflight_dir(dir_path)?;
    let path = inflight.join(format!("{}.json", hash));
    if path.exists() {
        fs::remove_file(&path).await?;
    }
    Ok(())
}

pub async fn on_new_event(dir_path: Option<PathBuf>) -> Option<ClaimedEvent> {
    loop {
        match claim_next_event(dir_path.clone()).await {
            Ok(Some(ev)) => return Some(ev),
            Ok(None) => {
                // Idle: opportunistically trim the queue (throttled) so an
                // offline device still ages out its stale backlog.
                maybe_prune_events(dir_path.clone()).await;
                sleep(Duration::from_millis(200)).await;
            }
            Err(e) => {
                tracing::error!("claim_next_event error: {e}");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Log tail merging
// ---------------------------------------------------------------------------

fn merge_log_tails(existing: &str, new: &str, max_bytes: usize) -> String {
    let combined = if existing.is_empty() {
        new.to_string()
    } else {
        format!("{}\n{}", existing, new)
    };
    if combined.len() <= max_bytes {
        combined
    } else {
        combined[combined.len() - max_bytes..].to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use m87_shared::deploy_spec::{
        CommandSpec, ObserveHooks, ObserveSpec, RebootMode, StopSpec, Workdir,
    };
    use std::time::Duration;
    use tempfile::TempDir;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn sh(s: impl Into<String>) -> CommandSpec {
        CommandSpec::Sh(s.into())
    }

    fn mk_step(name: &str, cmd: CommandSpec) -> Step {
        Step {
            name: Some(name.to_string()),
            run: cmd,
            timeout: Some(Duration::from_secs(5)),
            retry: None,
            undo: None,
        }
    }

    fn mk_svc(id: &str, start: CommandSpec, stop: CommandSpec) -> ServiceSpec {
        ServiceSpec {
            id: id.to_string(),
            lifecycle: Lifecycle::Running,
            workdir: Some(Workdir {
                mode: WorkdirMode::Persistent,
                path: None,
            }),
            files: BTreeMap::new(),
            env: BTreeMap::new(),
            steps: vec![mk_step("start", start)],
            on_failure: None,
            observe: None,
            stop: Some(StopSpec {
                steps: vec![mk_step("stop", stop)],
            }),
            reboot: RebootMode::None,
            restart: RestartPolicy::OnFailure,
        }
    }

    fn mk_observer_spec(id: &str) -> ServiceSpec {
        ServiceSpec {
            id: id.to_string(),
            lifecycle: Lifecycle::Running,
            workdir: Some(Workdir {
                mode: WorkdirMode::Persistent,
                path: None,
            }),
            files: BTreeMap::new(),
            env: BTreeMap::new(),
            steps: vec![],
            on_failure: None,
            observe: Some(ObserveSpec {
                logs: None,
                liveness: None,
                health: Some(ObserveHooks {
                    every: Duration::from_secs(60),
                    observe: sh("echo ok"),
                    observe_timeout: None,
                    record: None,
                    record_timeout: None,
                    report: None,
                    report_timeout: None,
                    fails_after: None,
                }),
            }),
            stop: None,
            reboot: RebootMode::None,
            restart: RestartPolicy::OnFailure,
        }
    }

    fn mk_job_def(id: &str, cmd: CommandSpec) -> JobDef {
        JobDef {
            id: id.to_string(),
            lifecycle: Lifecycle::Running,
            workdir: Some(Workdir {
                mode: WorkdirMode::Persistent,
                path: None,
            }),
            files: BTreeMap::new(),
            env: BTreeMap::new(),
            steps: vec![mk_step("run", cmd)],
            on_failure: None,
            reboot: RebootMode::None,
        }
    }

    fn mk_rev(
        id: &str,
        svcs: Vec<ServiceSpec>,
        obs: Vec<ServiceSpec>,
        jobs: Vec<JobDef>,
    ) -> DeploymentRevision {
        DeploymentRevision {
            id: Some(id.to_string()),
            services: svcs,
            observers: obs,
            jobs,
            rollback: None,
        }
    }

    async fn make_mgr(td: &TempDir) -> DeploymentManager {
        let base = td.path().join("m87");
        DeploymentManager::new(Some(base)).await.unwrap()
    }

    // ── Service lifecycle tests ───────────────────────────────────────────────

    #[tokio::test]
    async fn service_starts_on_deploy() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker = td.path().join("started");
        let marker_s = marker.display().to_string();

        let rev = mk_rev(
            "r1",
            vec![mk_svc("svc", sh(format!("touch {marker_s}")), sh("true"))],
            vec![],
            vec![],
        );
        mgr.set_desired_units(rev, vec![]).await?;
        mgr.reconcile_dirty().await?;

        assert!(marker.exists(), "startup command should have run");
        Ok(())
    }

    #[tokio::test]
    async fn service_skips_restart_same_hash() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let counter = td.path().join("count");
        let counter_s = counter.display().to_string();

        let svc = mk_svc("svc", sh(format!("echo x >> {counter_s}")), sh("true"));
        let rev = mk_rev("r1", vec![svc.clone()], vec![], vec![]);

        mgr.set_desired_units(rev.clone(), vec![]).await?;
        mgr.reconcile_dirty().await?;
        // Force the hash into dirty again (simulating a second deploy with same spec)
        mgr.dirty_services.write().await.insert(svc.get_hash());
        mgr.reconcile_dirty().await?;

        let lines = std::fs::read_to_string(&counter)?.lines().count();
        assert_eq!(lines, 1, "startup should run only once for the same hash");
        Ok(())
    }

    #[tokio::test]
    async fn service_restarts_on_spec_change() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let order = td.path().join("order");
        let order_s = order.display().to_string();

        let v1 = mk_rev(
            "r1",
            vec![mk_svc(
                "svc",
                sh(format!("echo start_v1 >> {order_s}")),
                sh(format!("echo stop_v1 >> {order_s}")),
            )],
            vec![],
            vec![],
        );
        let v2 = mk_rev(
            "r2",
            vec![mk_svc(
                "svc",
                sh(format!("echo start_v2 >> {order_s}")),
                sh(format!("echo stop_v2 >> {order_s}")),
            )],
            vec![],
            vec![],
        );

        mgr.set_desired_units(v1, vec![]).await?;
        mgr.reconcile_dirty().await?;
        mgr.set_desired_units(v2, vec![]).await?;
        mgr.reconcile_dirty().await?;

        let text = std::fs::read_to_string(&order)?;
        assert!(text.contains("start_v1"), "v1 should have started");
        assert!(text.contains("start_v2"), "v2 should have started");
        Ok(())
    }

    #[tokio::test]
    async fn service_stop_before_start_on_change() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let order = td.path().join("order");
        let order_s = order.display().to_string();
        let marker = td.path().join("marker");
        let marker_s = marker.display().to_string();

        let v1 = mk_rev(
            "r1",
            vec![mk_svc(
                "svc",
                sh(format!("echo start_v1 >> {order_s}; touch {marker_s}")),
                sh(format!("echo stop_v1 >> {order_s}; rm -f {marker_s}")),
            )],
            vec![],
            vec![],
        );
        let v2 = mk_rev(
            "r2",
            vec![mk_svc(
                "svc",
                sh(format!("echo start_v2 >> {order_s}; touch {marker_s}")),
                sh(format!("echo stop_v2 >> {order_s}; rm -f {marker_s}")),
            )],
            vec![],
            vec![],
        );

        mgr.set_desired_units(v1, vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker.exists());

        mgr.set_desired_units(v2, vec![]).await?;
        mgr.reconcile_dirty().await?;

        let text = std::fs::read_to_string(&order)?;
        let lines: Vec<&str> = text.lines().collect();
        let pos_stop = lines.iter().position(|l| *l == "stop_v1").unwrap();
        let pos_start = lines.iter().position(|l| *l == "start_v2").unwrap();
        assert!(
            pos_stop < pos_start,
            "stop_v1 ({pos_stop}) must precede start_v2 ({pos_start})"
        );
        assert!(marker.exists(), "v2 marker should exist");
        Ok(())
    }

    #[tokio::test]
    async fn service_removed_runs_stop() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker = td.path().join("marker");
        let marker_s = marker.display().to_string();

        let v1 = mk_rev(
            "r1",
            vec![mk_svc(
                "svc",
                sh(format!("touch {marker_s}")),
                sh(format!("rm -f {marker_s}")),
            )],
            vec![],
            vec![],
        );
        mgr.set_desired_units(v1, vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker.exists());

        let v2 = mk_rev("r2", vec![], vec![], vec![]);
        mgr.set_desired_units(v2, vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(!marker.exists(), "stop cmd should have removed marker");
        Ok(())
    }

    // Reproduces the reporting/handling gap on the teardown path (Fix 2).
    //
    // When a service is removed from the desired revision, reconcile runs its
    // stop steps. Today the stop is invoked as `let _ = self.stop_service(...)`
    // (reconcile_dirty), so a FAILING stop step is silently discarded: reconcile
    // returns Ok and clears the dirty flag, leaving a half-torn-down unit with no
    // error surfaced and no retry. A failing stop must not be swallowed.
    // Reaps 0.7.x-era leftover workspaces (which orphaned containers on upgrade)
    // while leaving 0.8.x's own `workspaces/` untouched.
    #[tokio::test]
    async fn reaps_legacy_workspaces_but_not_new_ones() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let root = mgr.root_dir.clone();

        // 0.7.x leftovers: persistent `jobs/<id>` and ephemeral `tmp/jobs/<hash>`.
        std::fs::create_dir_all(root.join("jobs/old-svc"))?;
        std::fs::write(
            root.join("jobs/old-svc/docker-compose.yml"),
            "services: {}\n",
        )?;
        std::fs::create_dir_all(root.join("tmp/jobs/deadbeefcafef00d"))?;

        // 0.8.x's own workspace — must survive the reap.
        std::fs::create_dir_all(root.join("workspaces/live-svc"))?;
        std::fs::write(root.join("workspaces/live-svc/marker"), "keep")?;

        mgr.reap_legacy_workspaces().await;

        assert!(
            !root.join("jobs").exists(),
            "legacy jobs/ dir must be reaped"
        );
        assert!(
            !root.join("tmp/jobs").exists(),
            "legacy tmp/jobs/ dir must be reaped"
        );
        assert!(
            root.join("workspaces/live-svc/marker").exists(),
            "0.8.x workspace must be left untouched"
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_stop_step_is_not_silently_swallowed() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;

        // Service whose stop step always fails.
        let v1 = mk_rev(
            "r1",
            vec![mk_svc("svc", sh("true"), sh("exit 1"))],
            vec![],
            vec![],
        );
        mgr.set_desired_units(v1, vec![]).await?;
        mgr.reconcile_dirty().await?;

        // Remove the service → its (failing) stop steps get scheduled.
        let v2 = mk_rev("r2", vec![], vec![], vec![]);
        mgr.set_desired_units(v2, vec![]).await?;

        let res = mgr.reconcile_dirty().await;
        assert!(
            res.is_err(),
            "a failing stop step must surface as an error from reconcile, not be \
             swallowed by `let _ = stop_service(...)`"
        );
        Ok(())
    }

    #[tokio::test]
    async fn service_stopped_via_lifecycle_runs_stop() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker = td.path().join("marker");
        let marker_s = marker.display().to_string();

        let svc = mk_svc(
            "svc",
            sh(format!("touch {marker_s}")),
            sh(format!("rm -f {marker_s}")),
        );
        let rev = mk_rev("r1", vec![svc.clone()], vec![], vec![]);
        mgr.set_desired_units(rev.clone(), vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker.exists());

        // Apply lifecycle=Stopped → should trigger stop reconcile
        mgr.apply_lifecycle_updates(vec![LifecycleUpdate {
            unit_id: "svc".to_string(),
            lifecycle: Lifecycle::Stopped,
        }])
        .await?;
        mgr.reconcile_dirty().await?;
        assert!(!marker.exists(), "stop steps should have removed marker");
        Ok(())
    }

    #[tokio::test]
    async fn service_paused_lifecycle_does_not_stop() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker = td.path().join("marker");
        let stop_marker = td.path().join("stopped");
        let marker_s = marker.display().to_string();
        let stop_s = stop_marker.display().to_string();

        let svc = mk_svc(
            "svc",
            sh(format!("touch {marker_s}")),
            sh(format!("touch {stop_s}")),
        );
        let rev = mk_rev("r1", vec![svc], vec![], vec![]);
        mgr.set_desired_units(rev, vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker.exists());

        // Pause — should NOT run stop steps
        mgr.apply_lifecycle_updates(vec![LifecycleUpdate {
            unit_id: "svc".to_string(),
            lifecycle: Lifecycle::Paused,
        }])
        .await?;
        // dirty_services should be empty (pause doesn't dirty)
        assert!(
            mgr.dirty_services.read().await.is_empty(),
            "pause should not mark service dirty"
        );
        assert!(!stop_marker.exists(), "stop steps must NOT run on pause");
        Ok(())
    }

    #[tokio::test]
    async fn service_resume_from_stopped() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker = td.path().join("marker");
        let marker_s = marker.display().to_string();

        let svc = mk_svc(
            "svc",
            sh(format!("touch {marker_s}")),
            sh(format!("rm -f {marker_s}")),
        );
        let rev = mk_rev("r1", vec![svc.clone()], vec![], vec![]);
        mgr.set_desired_units(rev.clone(), vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker.exists());

        // Stop it
        mgr.apply_lifecycle_updates(vec![LifecycleUpdate {
            unit_id: "svc".to_string(),
            lifecycle: Lifecycle::Stopped,
        }])
        .await?;
        mgr.reconcile_dirty().await?;
        assert!(!marker.exists());

        // Resume
        mgr.apply_lifecycle_updates(vec![LifecycleUpdate {
            unit_id: "svc".to_string(),
            lifecycle: Lifecycle::Running,
        }])
        .await?;
        mgr.reconcile_dirty().await?;
        assert!(marker.exists(), "service should restart after resume");
        Ok(())
    }

    #[tokio::test]
    async fn startup_backoff_respected() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let counter = td.path().join("count");
        let counter_s = counter.display().to_string();

        // A service whose startup always fails
        let svc = mk_svc("svc", sh("exit 1"), sh("true"));
        let rev = mk_rev("r1", vec![svc.clone()], vec![], vec![]);
        mgr.set_desired_units(rev, vec![]).await?;
        // First reconcile: startup attempt, fails, startup_failures=1
        let _ = mgr.reconcile_dirty().await;

        // Manually re-dirty and reconcile again immediately — backoff should prevent re-run
        mgr.dirty_services.write().await.insert(svc.get_hash());

        // Set up a counter to verify the startup step is NOT called again immediately
        let svc2 = ServiceSpec {
            steps: vec![mk_step("start", sh(format!("echo x >> {counter_s}")))],
            ..svc.clone()
        };
        let wd = mgr
            .resolve_workdir_for(&svc2.id, svc2.workdir.as_ref())
            .await?;
        // Manually set startup_failures=1 with last_attempt_ms = now (so backoff is in effect)
        let mut st = LocalRunState::load(&wd)?;
        st.startup_failures = 1;
        st.last_attempt_ms = Some(now_ms_u64());
        LocalRunState::save(&wd, &st)?;

        let _ = mgr.apply_service(&svc2, "r1", &wd).await;
        // Counter file should NOT exist because backoff is in effect
        assert!(
            !counter.exists(),
            "startup must not run during backoff window"
        );
        Ok(())
    }

    // ── Observer tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn observer_appears_in_observer_map() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let obs = mk_observer_spec("obs1");
        let rev = mk_rev("r1", vec![], vec![obs], vec![]);
        mgr.set_desired_units(rev.clone(), vec![]).await?;
        let loaded = RevisionStore::get_desired_config(Some(mgr.root_dir.clone()))?;
        let map = loaded.unwrap().get_observer_map();
        assert_eq!(map.len(), 1);
        assert!(map.values().any(|o| o.id == "obs1"));
        Ok(())
    }

    #[tokio::test]
    async fn observer_removed_from_map() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let obs = mk_observer_spec("obs1");
        let rev1 = mk_rev("r1", vec![], vec![obs], vec![]);
        mgr.set_desired_units(rev1, vec![]).await?;

        let rev2 = mk_rev("r2", vec![], vec![], vec![]);
        mgr.set_desired_units(rev2, vec![]).await?;

        let loaded = RevisionStore::get_desired_config(Some(mgr.root_dir.clone()))?;
        assert!(loaded.unwrap().get_observer_map().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_desired_config_is_quarantined_not_stuck() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let root = mgr.root_dir.clone();

        // Establish a valid desired config, then corrupt it on disk to simulate
        // an old / incompatible-schema `desired_units.json`.
        mgr.set_desired_units(mk_rev("r1", vec![], vec![], vec![]), vec![])
            .await?;
        let path = RevisionStore::desired_path(Some(root.clone()))?;
        std::fs::write(&path, "{ not a valid DeploymentRevision")?;

        // Must NOT return Err (that would wedge every reconcile cycle forever,
        // requiring a manual file deletion). It recovers: returns None and moves
        // the bad file aside so the server can re-push a fresh revision.
        let loaded = RevisionStore::get_desired_config(Some(root.clone()))?;
        assert!(loaded.is_none(), "corrupt config should read as None, not error");
        assert!(!path.exists(), "corrupt config should be moved aside");
        assert!(
            path.with_file_name("desired_units.json.corrupt").exists(),
            "corrupt config should be quarantined for inspection"
        );

        // A subsequent read is clean — no repeated error, file already handled.
        assert!(RevisionStore::get_desired_config(Some(root))?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn observer_paused_skips_observe_check() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let mut obs = mk_observer_spec("obs1");
        // Paused at spec level
        obs.lifecycle = Lifecycle::Paused;
        let rev = mk_rev("r1", vec![], vec![obs.clone()], vec![]);
        mgr.set_desired_units(rev, vec![]).await?;

        let loaded = RevisionStore::get_desired_config(Some(mgr.root_dir.clone()))?.unwrap();
        // All observers with paused lifecycle should be filtered in the start loop
        for o in &loaded.observers {
            assert!(
                o.lifecycle.is_paused(),
                "observer should be paused in saved revision"
            );
        }
        Ok(())
    }

    // ── Job tests ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn job_def_not_run_on_deploy() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker = td.path().join("job_ran");
        let marker_s = marker.display().to_string();

        let jd = mk_job_def("migrate", sh(format!("touch {marker_s}")));
        let rev = mk_rev("r1", vec![], vec![], vec![jd]);
        mgr.set_desired_units(rev, vec![]).await?;
        mgr.reconcile_dirty().await?;

        assert!(
            !marker.exists(),
            "job definition must not auto-run on deploy"
        );
        Ok(())
    }

    #[tokio::test]
    async fn job_run_executes_steps() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker = td.path().join("job_done");
        let marker_s = marker.display().to_string();

        let jd = mk_job_def("migrate", sh(format!("touch {marker_s}")));
        let rev = mk_rev("r1", vec![], vec![], vec![jd.clone()]);
        mgr.set_desired_units(rev, vec![]).await?;

        let run = JobRun {
            run_id: "run-1".to_string(),
            job_def_id: "migrate".to_string(),
            revision_id: "r1".to_string(),
            env_overrides: BTreeMap::new(),
            status: JobRunStatus::Queued,
            enqueued_at: now_ms_u64(),
            started_at: None,
            completed_at: None,
            error: None,
        };
        mgr.execute_job_run(run, &jd).await?;
        assert!(marker.exists(), "job run should have created marker");
        Ok(())
    }

    #[tokio::test]
    async fn job_run_env_override_applied() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let out = td.path().join("env_out");
        let out_s = out.display().to_string();

        let mut jd = mk_job_def("env-job", sh(format!("echo $TARGET > {out_s}")));
        jd.env.insert("TARGET".to_string(), "default".to_string());

        let mut overrides = BTreeMap::new();
        overrides.insert("TARGET".to_string(), "production".to_string());

        let run = JobRun {
            run_id: "run-2".to_string(),
            job_def_id: "env-job".to_string(),
            revision_id: "r1".to_string(),
            env_overrides: overrides,
            status: JobRunStatus::Queued,
            enqueued_at: now_ms_u64(),
            started_at: None,
            completed_at: None,
            error: None,
        };
        mgr.execute_job_run(run, &jd).await?;
        let content = std::fs::read_to_string(&out)?.trim().to_string();
        assert_eq!(content, "production", "env override must take effect");
        Ok(())
    }

    #[tokio::test]
    async fn job_run_reports_success() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let jd = mk_job_def("ok-job", sh("true"));
        let run = JobRun {
            run_id: "run-ok".to_string(),
            job_def_id: "ok-job".to_string(),
            revision_id: "r1".to_string(),
            env_overrides: BTreeMap::new(),
            status: JobRunStatus::Queued,
            enqueued_at: now_ms_u64(),
            started_at: None,
            completed_at: None,
            error: None,
        };
        let result = mgr.execute_job_run(run, &jd).await;
        assert!(result.is_ok(), "successful job should return Ok");

        // Drain all enqueued events; find the JobRunReport among them (StepReports come first)
        let mut found = false;
        loop {
            let ev = claim_next_event(Some(mgr.root_dir.clone())).await?;
            match ev {
                None => break,
                Some(ev) => {
                    if let DeployReportKind::JobRunReport(r) = ev.report {
                        assert_eq!(r.status, JobRunStatus::Success);
                        assert_eq!(r.run_id, "run-ok");
                        found = true;
                    }
                }
            }
        }
        assert!(found, "JobRunReport with Success must be enqueued");
        Ok(())
    }
    #[tokio::test]
    async fn job_run_reports_failure() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let jd = mk_job_def("fail-job", sh("exit 1"));
        let run = JobRun {
            run_id: "run-fail".to_string(),
            job_def_id: "fail-job".to_string(),
            revision_id: "r1".to_string(),
            env_overrides: BTreeMap::new(),
            status: JobRunStatus::Queued,
            enqueued_at: now_ms_u64(),
            started_at: None,
            completed_at: None,
            error: None,
        };
        let result = mgr.execute_job_run(run, &jd).await;
        assert!(result.is_err(), "failed job should return Err");

        // Drain all enqueued events; find the JobRunReport among them (StepReports come first)
        let mut found = false;
        loop {
            let ev = claim_next_event(Some(mgr.root_dir.clone())).await?;
            match ev {
                None => break,
                Some(ev) => {
                    if let DeployReportKind::JobRunReport(r) = ev.report {
                        assert_eq!(r.status, JobRunStatus::Failed);
                        assert_eq!(r.run_id, "run-fail");
                        found = true;
                    }
                }
            }
        }
        assert!(found, "JobRunReport with Failed must be enqueued");
        Ok(())
    }

    #[tokio::test]
    async fn multiple_job_runs_sequential() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let order = td.path().join("order");
        let order_s = order.display().to_string();

        let jd = mk_job_def("seq-job", sh(format!("echo run >> {order_s}")));
        for i in 0..3u32 {
            let run = JobRun {
                run_id: format!("run-{i}"),
                job_def_id: "seq-job".to_string(),
                revision_id: "r1".to_string(),
                env_overrides: BTreeMap::new(),
                status: JobRunStatus::Queued,
                enqueued_at: now_ms_u64(),
                started_at: None,
                completed_at: None,
                error: None,
            };
            mgr.execute_job_run(run, &jd).await?;
        }
        let count = std::fs::read_to_string(&order)?.lines().count();
        assert_eq!(count, 3, "three runs should produce three lines");
        Ok(())
    }

    // ── Restart policy tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn restart_on_failure_marks_service_dirty() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;

        let svc = mk_svc("svc", sh("true"), sh("true"));
        let rev = mk_rev("r1", vec![svc.clone()], vec![], vec![]);
        mgr.set_desired_units(rev, vec![]).await?;
        mgr.reconcile_dirty().await?;

        // Clear dirty set to start clean
        mgr.dirty_services.write().await.clear();

        // Simulate restart policy triggering
        mgr.maybe_restart_service(&svc).await?;

        let wd = mgr
            .resolve_workdir_for(&svc.id, svc.workdir.as_ref())
            .await?;
        let st = LocalRunState::load(&wd)?;
        assert!(
            st.last_run_hash.is_none(),
            "clear_run_hash should have cleared hash"
        );
        assert!(
            mgr.dirty_services.read().await.contains(&svc.get_hash()),
            "service should be re-queued in dirty set"
        );
        Ok(())
    }

    #[tokio::test]
    async fn restart_never_does_not_dirty() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;

        let mut svc = mk_svc("svc", sh("true"), sh("true"));
        svc.restart = RestartPolicy::Never;

        let rev = mk_rev("r1", vec![svc.clone()], vec![], vec![]);
        mgr.set_desired_units(rev, vec![]).await?;
        mgr.reconcile_dirty().await?;
        mgr.dirty_services.write().await.clear();

        mgr.maybe_restart_service(&svc).await?;

        assert!(
            mgr.dirty_services.read().await.is_empty(),
            "RestartPolicy::Never must not re-queue service"
        );
        Ok(())
    }

    // ── Hash idempotency / dirty tracking tests ───────────────────────────────

    #[tokio::test]
    async fn dirty_add_change_remove_hashes() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;

        let v1 = mk_rev(
            "r1",
            vec![mk_svc("svc", sh("echo s1"), sh("echo stop1"))],
            vec![],
            vec![],
        );
        let h1 = v1.get_service_map().keys().next().cloned().unwrap();

        mgr.set_desired_units(v1, vec![]).await?;
        assert!(mgr.dirty_services.read().await.contains(&h1));
        mgr.dirty_services.write().await.clear();

        let v2 = mk_rev(
            "r2",
            vec![mk_svc("svc", sh("echo s2"), sh("echo stop2"))],
            vec![],
            vec![],
        );
        let h2 = v2.get_service_map().keys().next().cloned().unwrap();

        mgr.set_desired_units(v2, vec![]).await?;
        let d = mgr.dirty_services.read().await.clone();
        assert!(d.contains(&h1), "old hash must be dirty");
        assert!(d.contains(&h2), "new hash must be dirty");
        mgr.dirty_services.write().await.clear();

        // Remove
        let v3 = mk_rev("r3", vec![], vec![], vec![]);
        mgr.set_desired_units(v3, vec![]).await?;
        assert!(
            mgr.dirty_services.read().await.contains(&h2),
            "removed hash must be dirty"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_stops_before_starts() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let order = td.path().join("order");
        let marker = td.path().join("marker");
        let order_s = order.display().to_string();
        let marker_s = marker.display().to_string();

        let v1 = mk_rev(
            "r1",
            vec![mk_svc(
                "svc",
                sh(format!("echo start_v1 >> {order_s}; touch {marker_s}")),
                sh(format!("echo stop_v1 >> {order_s}; rm -f {marker_s}")),
            )],
            vec![],
            vec![],
        );
        mgr.set_desired_units(v1, vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker.exists());

        let v2 = mk_rev(
            "r2",
            vec![mk_svc(
                "svc",
                sh(format!("echo start_v2 >> {order_s}; touch {marker_s}")),
                sh(format!("echo stop_v2 >> {order_s}; rm -f {marker_s}")),
            )],
            vec![],
            vec![],
        );
        mgr.set_desired_units(v2, vec![]).await?;
        mgr.reconcile_dirty().await?;

        let text = std::fs::read_to_string(&order)?;
        let lines: Vec<&str> = text.lines().collect();
        let p_stop = lines.iter().position(|l| *l == "stop_v1").unwrap();
        let p_start = lines.iter().position(|l| *l == "start_v2").unwrap();
        assert!(p_stop < p_start, "stop_v1 must precede start_v2");
        assert!(marker.exists());

        let v3 = mk_rev("r3", vec![], vec![], vec![]);
        mgr.set_desired_units(v3, vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(
            !marker.exists(),
            "marker should be gone after remove + stop"
        );

        Ok(())
    }

    #[tokio::test]
    async fn daemon_restart_requeues_stale_hash() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;

        let svc = mk_svc("svc", sh("true"), sh("true"));
        let rev = mk_rev("r1", vec![svc.clone()], vec![], vec![]);
        mgr.set_desired_units(rev, vec![]).await?;
        mgr.dirty_services.write().await.clear();

        // Simulate: LocalRunState has a *different* hash (stale) or None
        let wd = mgr
            .resolve_workdir_for(&svc.id, svc.workdir.as_ref())
            .await?;
        let mut st = LocalRunState::load(&wd)?;
        st.last_run_hash = Some("stale-hash".to_string());
        LocalRunState::save(&wd, &st)?;

        mgr.set_dirty_services().await?;

        assert!(
            mgr.dirty_services.read().await.contains(&svc.get_hash()),
            "stale hash should cause service to be re-queued on startup"
        );
        Ok(())
    }

    #[tokio::test]
    async fn lifecycle_update_delivered_via_set_desired_units() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker = td.path().join("marker");
        let marker_s = marker.display().to_string();

        let svc = mk_svc(
            "svc",
            sh(format!("touch {marker_s}")),
            sh(format!("rm -f {marker_s}")),
        );
        let rev = mk_rev("r1", vec![svc.clone()], vec![], vec![]);
        mgr.set_desired_units(rev.clone(), vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker.exists());

        // Deliver lifecycle=Stopped via set_desired_units (as server heartbeat would)
        mgr.set_desired_units(
            rev.clone(),
            vec![LifecycleUpdate {
                unit_id: "svc".to_string(),
                lifecycle: Lifecycle::Stopped,
            }],
        )
        .await?;
        mgr.reconcile_dirty().await?;
        assert!(!marker.exists(), "stop should have run");
        Ok(())
    }

    // ── Observe reporting: edge-trigger / anti-spam ──────────────────────────
    //
    // Regression guard for the heartbeat-spam + CPU-spike bug: a persistently
    // unhealthy observe (e.g. an error line stuck in a `logs | grep` health
    // check) must report the unhealthy state ONCE per healthy->unhealthy
    // transition — not on every failing poll. Before the fix the failure
    // branch of `decide_on_error` was level-triggered (`needs_send = is_failure`),
    // so it re-enqueued a fresh RunState every `fails_after`-th failure forever;
    // each carried a unique `report_time`/`log_tail` so the content-hash dedup
    // in `enqueue_event` never collapsed them and the pending queue grew
    // without bound.

    fn failing_health_hooks(fails_after: Option<u32>) -> ObserveHooks {
        ObserveHooks {
            every: Duration::from_secs(10),
            observe: sh("exit 1"),
            observe_timeout: None,
            record: None,
            record_timeout: None,
            report: None,
            report_timeout: None,
            fails_after,
        }
    }

    fn healthy_health_hooks(fails_after: Option<u32>) -> ObserveHooks {
        ObserveHooks {
            observe: sh("true"),
            ..failing_health_hooks(fails_after)
        }
    }

    /// Drain every pending event and return the RunState reports in order.
    async fn drain_runstates(mgr: &DeploymentManager) -> Vec<RunState> {
        let mut out = Vec::new();
        while let Some(ev) = claim_next_event(Some(mgr.root_dir.clone())).await.unwrap() {
            if let DeployReportKind::RunState(rs) = ev.report {
                out.push(rs);
            }
        }
        out
    }

    #[tokio::test]
    async fn health_persistently_unhealthy_reports_once_not_per_poll() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let spec = mk_observer_spec("obs");
        let hooks = failing_health_hooks(Some(1));

        // Simulate 5 consecutive failing health polls of the same service.
        for _ in 0..5 {
            mgr.run_observe(ObserveKind::Health, "run1", "r1", &spec, &hooks)
                .await?;
        }

        let states = drain_runstates(&mgr).await;
        let unhealthy = states.iter().filter(|s| s.healthy == Some(false)).count();
        assert_eq!(
            unhealthy, 1,
            "a persistently unhealthy service must report exactly once, \
             not once per poll (was {unhealthy}, i.e. one per failing poll pre-fix)"
        );
        Ok(())
    }

    #[tokio::test]
    async fn health_reports_once_across_fails_after_threshold() -> Result<()> {
        // Mirrors the default compose template (`fails_after: 3`): the state
        // crosses the threshold at poll 3, 6, 9. Pre-fix that emitted 3 reports;
        // post-fix only the first (the healthy->unhealthy transition) is sent.
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let spec = mk_observer_spec("obs");
        let hooks = failing_health_hooks(Some(3));

        for _ in 0..9 {
            mgr.run_observe(ObserveKind::Health, "run1", "r1", &spec, &hooks)
                .await?;
        }

        let states = drain_runstates(&mgr).await;
        let unhealthy = states.iter().filter(|s| s.healthy == Some(false)).count();
        assert_eq!(
            unhealthy, 1,
            "unhealthy state must be reported once at the threshold crossing, \
             not on every `fails_after`-th failure (was {unhealthy} pre-fix)"
        );
        Ok(())
    }

    #[tokio::test]
    async fn health_recovery_rearms_unhealthy_reporting() -> Result<()> {
        // The edge-trigger must work in both directions: unhealthy -> healthy
        // re-arms the reporter so the next healthy -> unhealthy transition is
        // reported again. Otherwise we'd go silent forever after the first flap.
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let spec = mk_observer_spec("obs");
        let failing = failing_health_hooks(Some(1));
        let ok = healthy_health_hooks(Some(1));

        // unhealthy burst -> 1 unhealthy report
        for _ in 0..3 {
            mgr.run_observe(ObserveKind::Health, "run1", "r1", &spec, &failing)
                .await?;
        }
        // recover -> 1 healthy report
        mgr.run_observe(ObserveKind::Health, "run1", "r1", &spec, &ok)
            .await?;
        // unhealthy again -> another unhealthy report
        for _ in 0..3 {
            mgr.run_observe(ObserveKind::Health, "run1", "r1", &spec, &failing)
                .await?;
        }

        let states = drain_runstates(&mgr).await;
        let unhealthy = states.iter().filter(|s| s.healthy == Some(false)).count();
        let healthy = states.iter().filter(|s| s.healthy == Some(true)).count();
        assert_eq!(unhealthy, 2, "each unhealthy transition should report once");
        assert_eq!(healthy, 1, "the recovery should report once");
        Ok(())
    }

    // ── Reconcile ordering / isolation regressions (customer 0.8.3 report) ─────

    // Reproduces the device-killing symptom: after a deploy that renames a
    // service (old id removed, new id added), the OLD unit's stop step fails
    // (e.g. `docker compose down` times out on a flaky LTE link) but reconcile
    // starts the NEW unit anyway. Both then run at once, contending for the same
    // exclusive hardware (camera / WittyPi / LTE), and the device falls over.
    //
    // reconcile_dirty runs Phase 2a (stop) then Phase 2b (start). A failed stop
    // is recorded in `failed_hashes` but Phase 2b starts the new unit
    // UNCONDITIONALLY. Safe invariant: if a stop failed this pass, the new unit
    // must NOT be started while the old one may still be running.
    #[tokio::test]
    async fn failed_stop_must_block_conflicting_start() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let old_marker = td.path().join("old_running");
        let new_marker = td.path().join("new_running");
        let old_s = old_marker.display().to_string();
        let new_s = new_marker.display().to_string();

        // v1: service "old" is up (marker present); its stop step FAILS, so the
        // old unit is left running (marker not removed — a failed teardown).
        let v1 = mk_rev(
            "r1",
            vec![mk_svc(
                "old",
                sh(format!("touch {old_s}")),
                sh("exit 1"), // stop fails → old stays up
            )],
            vec![],
            vec![],
        );
        mgr.set_desired_units(v1, vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(old_marker.exists(), "old unit should be running after v1");

        // v2: "old" removed, "new" added — a rename (different id / workspace /
        // compose project), which is exactly what the customer did to work
        // around earlier issues.
        let v2 = mk_rev(
            "r2",
            vec![mk_svc("new", sh(format!("touch {new_s}")), sh("true"))],
            vec![],
            vec![],
        );
        mgr.set_desired_units(v2, vec![]).await?;
        let _ = mgr.reconcile_dirty().await; // returns Err (stop failed) — expected

        // The old unit could not be torn down (its stop failed).
        assert!(
            old_marker.exists(),
            "old unit's stop failed, so it is still running"
        );
        // BUG: the new unit was started anyway → two containers run at once,
        // fighting over the same hardware. This assertion FAILS on current main.
        assert!(
            !new_marker.exists(),
            "new unit must NOT start while the old unit's stop failed (would run \
             two containers contending for exclusive hardware)"
        );
        Ok(())
    }

    // Reproduces the "everything stays pending, re-deploy sometimes fixes it"
    // symptom. reconcile_dirty's start phase is `self.apply_service(...).await?`
    // — the `?` aborts the WHOLE reconcile on the first unit whose start fails
    // (e.g. a `pull` step hitting a TLS-handshake timeout). Every not-yet-
    // processed sibling in the same revision is then skipped and left "pending",
    // with no error of its own. Because the start set is a HashMap, iteration
    // order is randomised per run, so a healthy sibling starts on some deploys
    // and is skipped on others — the flaky "sometimes works / --replace-all
    // sometimes flips pending→started" the customer observed.
    #[tokio::test]
    async fn failing_unit_must_not_block_sibling_start() -> Result<()> {
        // Same single-reconcile scenario, fresh manager each time. Post-fix the
        // healthy sibling starts on EVERY run; on current main it is skipped
        // whenever the failing unit is iterated first (~half the runs), so over
        // 25 runs the skip count is effectively always > 0.
        let mut skipped = 0;
        for i in 0..25 {
            let td = TempDir::new()?;
            let mgr = make_mgr(&td).await;
            let healthy = td.path().join("healthy_started");
            let healthy_s = healthy.display().to_string();

            let rev = mk_rev(
                &format!("r{i}"),
                vec![
                    // Fails to start (simulates a pull / TLS timeout).
                    mk_svc("broken", sh("exit 1"), sh("true")),
                    // Healthy, independent sibling that should always start.
                    mk_svc("healthy", sh(format!("touch {healthy_s}")), sh("true")),
                ],
                vec![],
                vec![],
            );
            mgr.set_desired_units(rev, vec![]).await?;
            let _ = mgr.reconcile_dirty().await; // Err from the broken unit

            if !healthy.exists() {
                skipped += 1;
            }
        }

        assert_eq!(
            skipped, 0,
            "a failing unit's start error aborted reconcile and left a healthy \
             sibling unstarted on {skipped}/25 runs (the `?` in the start phase \
             skips every remaining unit)"
        );
        Ok(())
    }

    // A corrupt run_state.json must self-heal, not error on the hot path. Before
    // the fix, `load` returned Err on invalid JSON, and because it is called on
    // every reconcile tick and heartbeat handshake, that error fired at very
    // high frequency. Now the bad file is deleted and defaults are returned.
    #[test]
    fn corrupt_run_state_is_deleted_not_errored() {
        let td = TempDir::new().unwrap();
        let wd = td.path();
        let path = wd.join("run_state.json");
        std::fs::write(&path, "{ this is not valid json").unwrap();

        let st = LocalRunState::load(wd).expect("load must not error on invalid json");
        assert_eq!(st.last_run_hash, None, "should reset to defaults");
        assert!(!path.exists(), "invalid run_state.json must be deleted");

        // A subsequent load is clean — no repeated error, file already handled.
        assert!(LocalRunState::load(wd).is_ok());
    }

    // Reproduces the customer's intent-loss orphan at the reconcile level. A
    // rename whose old-unit stop was never confirmed (crash before it ran) must
    // be re-queued on restart so the stop runs before the new unit — otherwise
    // the old container is orphaned alongside the new one. The in-memory dirty
    // set is lost on the crash; `set_dirty_services` (the boot path) must
    // reconstruct the pending teardown from `previous` + the still-set
    // `last_run_hash`.
    #[tokio::test]
    async fn removed_unit_with_unconfirmed_stop_is_reaped_on_restart() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker_a = td.path().join("a");
        let marker_b = td.path().join("b");
        let a = marker_a.display().to_string();
        let b = marker_b.display().to_string();

        // v1: cam-a up (marker A), with a working stop that removes A.
        let v1 = mk_rev(
            "r1",
            vec![mk_svc(
                "cam-a",
                sh(format!("touch {a}")),
                sh(format!("rm -f {a}")),
            )],
            vec![],
            vec![],
        );
        mgr.set_desired_units(v1, vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker_a.exists(), "cam-a should be running");

        // Rename to cam-b: records previous=v1 (still has cam-a) and marks dirty.
        let v2 = mk_rev(
            "r2",
            vec![mk_svc("cam-b", sh(format!("touch {b}")), sh("true"))],
            vec![],
            vec![],
        );
        mgr.set_desired_units(v2, vec![]).await?;

        // Simulate a crash BEFORE cam-a's stop ran: drop the in-memory dirty set,
        // then reboot -> set_dirty_services() rebuilds the work-set.
        mgr.dirty_services.write().await.clear();
        mgr.set_dirty_services().await?;
        let _ = mgr.reconcile_dirty().await;

        assert!(
            !marker_a.exists(),
            "cam-a's pending stop must be re-run after the restart — not orphaned"
        );
        assert!(marker_b.exists(), "cam-b should be running");
        Ok(())
    }

    // A: a MULTI-HOP orphan — one that was left running by an interrupted
    // rename and has since fallen out of `previous_units.json` after a further
    // deploy — must still be torn down from the durable per-unit ledger m87
    // records when it starts a unit. `previous` only holds one revision back, so
    // B alone can't see it.
    #[tokio::test]
    async fn multi_hop_orphan_is_reaped_from_ledger() -> Result<()> {
        let td = TempDir::new()?;
        let mgr = make_mgr(&td).await;
        let marker_a = td.path().join("a");
        let a = marker_a.display().to_string();

        // v1: cam-a runs; its teardown must be recorded durably in its workspace.
        let v1 = mk_rev(
            "r1",
            vec![mk_svc(
                "cam-a",
                sh(format!("touch {a}")),
                sh(format!("rm -f {a}")),
            )],
            vec![],
            vec![],
        );
        mgr.set_desired_units(v1, vec![]).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker_a.exists());

        // Two renames WITHOUT reconciling in between → cam-a is never stopped and
        // falls out of `previous` (which is now cam-b), then out of desired too.
        let v2 = mk_rev("r2", vec![mk_svc("cam-b", sh("true"), sh("true"))], vec![], vec![]);
        mgr.set_desired_units(v2, vec![]).await?;
        let v3 = mk_rev("r3", vec![mk_svc("cam-c", sh("true"), sh("true"))], vec![], vec![]);
        mgr.set_desired_units(v3, vec![]).await?;

        // Boot reap must still tear cam-a down from the ledger, even though it's
        // in neither previous(cam-b) nor desired(cam-c).
        mgr.reap_orphaned_units().await?;

        assert!(
            !marker_a.exists(),
            "multi-hop orphan cam-a must be reaped from the durable ledger"
        );
        Ok(())
    }

    // ── Deploy-report event retention ────────────────────────────────────────
    //
    // Regression guard for the heartbeat-storm bug: a device that churned or was
    // offline accumulated thousands of stale deploy-report events in
    // `events/pending`, which were then replayed forever as catch-up (pinning a
    // core and spamming heartbeats). The queue must be bounded by both age and
    // count regardless of connectivity.

    use filetime::{FileTime, set_file_mtime};

    /// Write a json event file and stamp its mtime `age` in the past.
    fn write_aged_event(dir: &Path, name: &str, age: Duration) {
        let path = dir.join(format!("{name}.json"));
        std::fs::write(&path, "{}").unwrap();
        let when = SystemTime::now() - age;
        set_file_mtime(&path, FileTime::from_system_time(when)).unwrap();
    }

    #[tokio::test]
    async fn prune_drops_events_older_than_max_age() -> Result<()> {
        let td = TempDir::new()?;
        let dir = td.path();
        write_aged_event(dir, "fresh", Duration::from_secs(60));
        write_aged_event(dir, "stale_a", Duration::from_secs(3 * 86_400));
        write_aged_event(dir, "stale_b", Duration::from_secs(5 * 86_400));

        let removed = prune_events_dir(
            dir,
            Duration::from_secs(86_400), // 1 day
            10_000,
            SystemTime::now(),
        )
        .await?;

        assert_eq!(removed, 2, "both events older than a day must be dropped");
        assert!(dir.join("fresh.json").exists(), "recent event must survive");
        assert!(!dir.join("stale_a.json").exists());
        assert!(!dir.join("stale_b.json").exists());
        Ok(())
    }

    #[tokio::test]
    async fn prune_enforces_max_count_dropping_oldest_first() -> Result<()> {
        let td = TempDir::new()?;
        let dir = td.path();
        // 6 fresh events, staggered mtimes so "oldest" is deterministic.
        for i in 0..6u64 {
            write_aged_event(dir, &format!("ev{i}"), Duration::from_secs(i));
        }

        let removed = prune_events_dir(
            dir,
            Duration::from_secs(0), // age bound disabled
            3,
            SystemTime::now(),
        )
        .await?;

        assert_eq!(removed, 3, "must trim down to the cap");
        // ev0..ev2 are the newest (smallest backdate) → survive; ev3..ev5 dropped.
        assert!(dir.join("ev0.json").exists());
        assert!(dir.join("ev1.json").exists());
        assert!(dir.join("ev2.json").exists());
        assert!(!dir.join("ev5.json").exists(), "oldest must be dropped first");
        Ok(())
    }

    #[tokio::test]
    async fn prune_ignores_non_json_files() -> Result<()> {
        let td = TempDir::new()?;
        let dir = td.path();
        let keep = dir.join("notes.txt");
        std::fs::write(&keep, "x").unwrap();
        set_file_mtime(
            &keep,
            FileTime::from_system_time(SystemTime::now() - Duration::from_secs(10 * 86_400)),
        )
        .unwrap();

        let removed =
            prune_events_dir(dir, Duration::from_secs(86_400), 10_000, SystemTime::now()).await?;

        assert_eq!(removed, 0, "non-event files must be left untouched");
        assert!(keep.exists());
        Ok(())
    }

    #[tokio::test]
    async fn prune_events_sweeps_pending_and_inflight() -> Result<()> {
        let td = TempDir::new()?;
        let root = td.path().to_path_buf();
        ensure_dirs(Some(root.clone())).await?;
        set_event_retention_secs(86_400); // 1 day

        write_aged_event(
            &pending_dir(Some(root.clone()))?,
            "old_pending",
            Duration::from_secs(3 * 86_400),
        );
        write_aged_event(
            &inflight_dir(Some(root.clone()))?,
            "old_inflight",
            Duration::from_secs(3 * 86_400),
        );
        write_aged_event(
            &pending_dir(Some(root.clone()))?,
            "recent",
            Duration::from_secs(30),
        );

        let removed = prune_events(Some(root.clone())).await?;

        assert_eq!(removed, 2, "stale events in both dirs must be pruned");
        assert!(pending_dir(Some(root.clone()))?.join("recent.json").exists());
        assert!(
            !pending_dir(Some(root.clone()))?
                .join("old_pending.json")
                .exists()
        );
        assert!(
            !inflight_dir(Some(root))?
                .join("old_inflight.json")
                .exists()
        );
        Ok(())
    }
}
