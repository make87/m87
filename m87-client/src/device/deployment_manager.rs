use anyhow::{Context, Result, anyhow};
use m87_shared::deploy_spec::{
    DeployReportKind, DeploymentRevision, DeploymentRevisionReport, ObserveHooks, OnFailure,
    Outcome, RetrySpec, RollbackPolicy, RollbackReport, RunReport, RunSpec, RunState, RunType,
    Step, StepReport, Undo, UndoMode, WorkdirMode,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Display,
    path::{Path, PathBuf},
    sync::Arc,
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
const MAX_TAIL_BYTES: usize = 4 * 1024; // 4KB

fn data_dir(dir_path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = dir_path {
        return Ok(path);
    }
    Ok(dirs::data_dir().context("data_dir")?.join("m87"))
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

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct LocalRunState {
    pub consecutive_health_failures: u32,
    #[serde(default)]
    pub consecutive_alive_failures: u32,
    #[serde(default)]
    pub ran_successful: bool,
    #[serde(default)]
    pub reported_health_once: bool,
    #[serde(default)]
    pub reported_alive_once: bool,
    #[serde(default)]
    pub last_health: bool,
    #[serde(default)]
    pub last_alive: bool,
}

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
        // return true;
        match self {
            ObserveKind::Liveness => {
                st.last_alive == false
                    || st.last_health == false
                    || !st.reported_alive_once
                    || st.consecutive_alive_failures > 0
            }
            ObserveKind::Health => {
                st.last_health != true
                    || st.last_alive != true
                    || !st.reported_health_once
                    || st.consecutive_health_failures > 0
            }
        }
    }

    fn decide_on_error(
        &self,
        _st: &LocalRunState,
        hooks: &ObserveHooks,
        consecutive: u32,
    ) -> ObserveDecision {
        let fails_after = hooks.fails_after.unwrap_or(1);
        let is_failure = consecutive > 0 && (consecutive % fails_after == 0);
        let needs_send = is_failure;
        // let needs_send = match self {
        //     ObserveKind::Liveness => {
        //         (st.last_alive == false || !st.reported_alive_once) && is_failure
        //     }
        //     ObserveKind::Health => {
        //         (st.last_health == true || !st.reported_health_once) && is_failure
        //     }
        // };
        // consecutive here is consecutive / fails after since we care about consecutive crashes we consider consecutive failures
        // round down

        let consecutive = if fails_after == 0 {
            0
        } else {
            consecutive / fails_after
        };

        ObserveDecision {
            is_failure,
            needs_send,
            consecutive,
        }
    }

    fn build_runstate_event(
        &self,
        run_id: &str,
        revision_id: &str,
        ok: bool,
        log_tail: Option<String>,
    ) -> RunState {
        match self {
            ObserveKind::Liveness => {
                if ok {
                    RunState {
                        run_id: run_id.to_string(),
                        revision_id: revision_id.to_string(),
                        healthy: None,
                        alive: Some(true),
                        report_time: now_ms_u64(),
                        log_tail: None,
                    }
                } else {
                    RunState {
                        run_id: run_id.to_string(),
                        revision_id: revision_id.to_string(),
                        healthy: Some(false),
                        alive: Some(false),
                        report_time: now_ms_u64(),
                        log_tail,
                    }
                }
            }
            ObserveKind::Health => {
                if ok {
                    RunState {
                        run_id: run_id.to_string(),
                        revision_id: revision_id.to_string(),
                        healthy: Some(true),
                        alive: Some(true),
                        report_time: now_ms_u64(),
                        log_tail: None,
                    }
                } else {
                    RunState {
                        run_id: run_id.to_string(),
                        revision_id: revision_id.to_string(),
                        healthy: Some(false),
                        alive: None,
                        report_time: now_ms_u64(),
                        log_tail,
                    }
                }
            }
        }
    }
}

impl LocalRunState {
    fn state_file_path(work_dir: &Path) -> Result<PathBuf> {
        Ok(work_dir.join("run_state.json"))
    }

    fn load(work_dir: &Path) -> Result<LocalRunState> {
        let path = LocalRunState::state_file_path(work_dir)?;

        if !path.exists() {
            return Ok(LocalRunState::default());
        }

        let display_name = work_dir.display();
        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read state file for dir {}", &display_name))?;

        let state: LocalRunState = serde_json::from_str(&contents)
            .with_context(|| format!("Failed to parse state file for dir {}", &display_name))?;

        Ok(state)
    }

    fn save(work_dir: &Path, st: &LocalRunState) -> Result<()> {
        let path = LocalRunState::state_file_path(work_dir)?;

        let contents =
            serde_json::to_string_pretty(st).context("Failed to serialize unit state")?;
        let display_name = work_dir.display();

        std::fs::write(&path, contents)
            .with_context(|| format!("Failed to write state file for dir {}", &display_name))?;

        Ok(())
    }

    fn delete(work_dir: &Path) -> Result<()> {
        let path = LocalRunState::state_file_path(work_dir)?;

        let display_name = work_dir.display();
        std::fs::remove_file(path)
            .with_context(|| format!("Failed to delete state file for dir {}", &display_name))?;

        Ok(())
    }

    fn failures_mut(&mut self, kind: ObserveKind) -> &mut u32 {
        match kind {
            ObserveKind::Liveness => &mut self.consecutive_alive_failures,
            ObserveKind::Health => &mut self.consecutive_health_failures,
        }
    }
}

pub struct RevisionStore {}

impl RevisionStore {
    fn desired_path(dir_path: Option<PathBuf>) -> Result<PathBuf> {
        let config_dir = data_dir(dir_path)?;
        let desired_path = config_dir.join("desired_units.json");
        Ok(desired_path)
    }

    fn previous_path(dir_path: Option<PathBuf>) -> Result<PathBuf> {
        let config_dir = data_dir(dir_path)?;
        let previous_path = config_dir.join("previous_units.json");
        Ok(previous_path)
    }

    // pub fn get_all() -> Result<HashMap<String, RunSpec>> {
    //     let desired_path = RevisionStore::desired_path()?;

    //     if !desired_path.exists() {
    //         return Ok(HashMap::new());
    //     }

    //     let contents =
    //         std::fs::read_to_string(&desired_path).context("Failed to read desired units file")?;
    //     let config: DeploymentRevision =
    //         serde_json::from_str(&contents).context("Failed to parse desired units file")?;

    //     Ok(config
    //         .units
    //         .iter()
    //         .map(|u| (u.get_id(), u.clone()))
    //         .collect())
    // }

    /// Get the current rollback policy
    pub fn get_rollback_policy(dir_path: Option<PathBuf>) -> Result<Option<RollbackPolicy>> {
        let desired_path = RevisionStore::desired_path(dir_path)?;
        if !desired_path.exists() {
            return Ok(None);
        }

        let contents =
            std::fs::read_to_string(&desired_path).context("Failed to read desired units file")?;
        let config: DeploymentRevision =
            serde_json::from_str(&contents).context("Failed to parse desired units file")?;

        Ok(config.rollback)
    }

    /// Get entire previous configuration for rollback
    pub fn get_previous_config(dir_path: Option<PathBuf>) -> Result<Option<DeploymentRevision>> {
        let previous_path = RevisionStore::previous_path(dir_path)?;
        if !previous_path.exists() {
            return Ok(None);
        }

        let contents = std::fs::read_to_string(&previous_path)
            .context("Failed to read previous units file")?;
        let config: DeploymentRevision =
            serde_json::from_str(&contents).context("Failed to parse previous units file")?;

        Ok(Some(config))
    }

    pub fn get_desired_config(dir_path: Option<PathBuf>) -> Result<Option<DeploymentRevision>> {
        let desired_path = RevisionStore::desired_path(dir_path)?;
        if !desired_path.exists() {
            return Ok(None);
        }

        let contents =
            std::fs::read_to_string(&desired_path).context("Failed to read desired units file")?;
        let config: DeploymentRevision =
            serde_json::from_str(&contents).context("Failed to parse desired units file")?;

        Ok(Some(config))
    }

    /// Set new desired configuration, backing up current to previous
    pub fn set_config(config: &DeploymentRevision, dir_path: Option<PathBuf>) -> Result<()> {
        let previous_path = RevisionStore::previous_path(dir_path.clone())?;
        let desired_path = RevisionStore::desired_path(dir_path)?;
        if desired_path.exists() {
            std::fs::copy(&desired_path, &previous_path)
                .context("Failed to backup previous units")?;
        }

        // Write new desired config
        let contents = serde_json::to_string_pretty(&config)
            .context("Failed to serialize desired units config")?;
        std::fs::write(&desired_path, contents).context("Failed to write desired units file")?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct DeploymentManager {
    root_dir: PathBuf,
    dirty: Arc<RwLock<HashSet<String>>>,
    log_manager: LogManager,
    rollback_policy: Arc<RwLock<Option<RollbackPolicy>>>,
    deployment_started_at: Arc<RwLock<Option<Instant>>>,
}

impl DeploymentManager {
    /// Create a new UnitManager with a custom state store.
    pub async fn new(data_dir_path: Option<PathBuf>) -> Result<Self> {
        let _ = ensure_dirs(data_dir_path.clone()).await?;
        let _ = recover_inflight(data_dir_path.clone()).await?;
        let root_dir = data_dir(data_dir_path.clone())?;

        let log_manager = LogManager::start();
        // Load rollback policy from disk if exists
        let rollback_policy = RevisionStore::get_rollback_policy(data_dir_path).unwrap_or(None);

        Ok(Self {
            root_dir,
            dirty: Arc::new(RwLock::new(HashSet::new())),
            log_manager,
            rollback_policy: Arc::new(RwLock::new(rollback_policy)),
            deployment_started_at: Arc::new(RwLock::new(None)),
        })
    }

    pub fn get_current_deploy_hash(data_dir_path: Option<PathBuf>) -> String {
        match RevisionStore::get_desired_config(data_dir_path) {
            Ok(Some(config)) => config.get_hash(),
            _ => "".to_string(),
        }
    }

    /// Get reference to the log manager for external use (e.g., streams/logs routing)
    pub async fn start_log_follow(&self) -> Result<()> {
        if let Some(spec) = RevisionStore::get_desired_config(Some(self.root_dir.clone()))? {
            for (_, unit) in spec.get_job_map() {
                if let Some(observer_spec) = &unit.observe {
                    if let Some(log_spec) = &observer_spec.logs {
                        let workdir = self.resolve_workdir(&unit).await?;
                        self.log_manager
                            .follow_start(unit.id, log_spec, unit.env, workdir)
                            .await;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn stop_log_follow(&self) -> Result<()> {
        if let Some(spec) = RevisionStore::get_desired_config(Some(self.root_dir.clone()))? {
            for (_, unit) in spec.get_job_map() {
                self.log_manager.follow_stop(unit.id).await;
            }
        }
        Ok(())
    }

    /// Replace desired set (authoritative). Marks changes dirty.
    pub async fn set_desired_units(&self, config: DeploymentRevision) -> Result<()> {
        let old_config = RevisionStore::get_desired_config(Some(self.root_dir.clone()))?;
        if let Some(oc) = &old_config {
            if oc.get_hash() == config.get_hash() {
                return Ok(());
            }
        }

        let new_map = config.get_job_map(); // keyed by hash
        let old_desired = match &old_config {
            Some(spec) => spec.get_job_map(), // keyed by hash
            None => BTreeMap::new(),
        };

        RevisionStore::set_config(&config, Some(self.root_dir.clone()))?;

        let mut dirty = self.dirty.write().await;

        // 1) Mark added hashes (new units) dirty
        for (h, u) in &new_map {
            if !old_desired.contains_key(h) {
                dirty.insert(h.clone());
                continue;
            }

            // 2) If it existed before but didn't run successfully, retry
            let wd = self.resolve_workdir(u).await?;
            if let Ok(st) = LocalRunState::load(&wd) {
                if !st.ran_successful {
                    dirty.insert(h.clone());
                }
            } else {
                dirty.insert(h.clone());
            }
        }

        // 3) Mark removed hashes dirty (so old services can be stopped)
        for (old_hash, _old_u) in old_desired.iter() {
            if !new_map.contains_key(old_hash) {
                dirty.insert(old_hash.clone());
            }
        }

        // 4) Mark replacements dirty on BOTH sides (same run id, different hash)
        //    This is the key fix: ensures old version is reconciled/stopped even when id stays the same.
        let old_by_id: HashMap<String, String> = old_desired
            .iter()
            .map(|(h, u)| (u.id.clone(), h.clone()))
            .collect();
        let new_by_id: HashMap<String, String> = new_map
            .iter()
            .map(|(h, u)| (u.id.clone(), h.clone()))
            .collect();

        for (run_id, old_hash) in &old_by_id {
            if let Some(new_hash) = new_by_id.get(run_id) {
                if new_hash != old_hash {
                    dirty.insert(old_hash.clone());
                    dirty.insert(new_hash.clone());
                }
            }
        }

        *self.rollback_policy.write().await = config.rollback.clone();
        *self.deployment_started_at.write().await = Some(Instant::now());

        Ok(())
    }

    async fn set_dirty_ids(&self) -> Result<()> {
        let desired = match RevisionStore::get_desired_config(Some(self.root_dir.clone()))? {
            Some(spec) => spec.get_job_map(),
            None => BTreeMap::new(),
        };
        let mut dirty = self.dirty.write().await;

        // mark removed as dirty so we can stop logs / stop service if needed
        for (_, u) in desired.iter() {
            let wd = self.resolve_workdir(u).await?;
            if let Ok(st) = LocalRunState::load(&wd) {
                if !st.ran_successful {
                    dirty.insert(u.get_hash());
                }
            }
        }
        Ok(())
    }

    /// Start the single supervisor loop.
    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut next_health: HashMap<String, Instant> = HashMap::new();
            let mut next_liveness: HashMap<String, Instant> = HashMap::new();

            // coarse tick keeps CPU low; checks run only when due
            let tick = Duration::from_millis(250);

            // add ids of desired jobs that are not ran_successfully
            let _ = self.set_dirty_ids().await;
            loop {
                if SHUTDOWN.is_cancelled() {
                    break;
                }

                // 1) reconcile dirty changes (run missing / stop old)
                if let Err(e) = self.reconcile_dirty().await {
                    tracing::error!("reconcile error: {e}");
                    let Ok(Some(desired)) =
                        RevisionStore::get_desired_config(Some(self.root_dir.clone()))
                    else {
                        tracing::error!("no desired config found");
                        continue;
                    };

                    let _ = enqueue_event(
                        DeployReportKind::DeploymentRevisionReport(DeploymentRevisionReport {
                            revision_id: desired.id.expect("revision id is required"),
                            outcome: Outcome::Failed,
                            dirty: true,
                            error: Some(format!("reconcile error: {e}")),
                        }),
                        Some(self.root_dir.clone()),
                    )
                    .await;
                    // TODO: Rollback right away?
                }

                // 2) schedule/poll liveness + health only when due
                let now = Instant::now();
                let desired_spec =
                    match RevisionStore::get_desired_config(Some(self.root_dir.clone())) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!("failed to get all revisions: {e}");
                            // TODO: Rollback right away?
                            continue;
                        }
                    };

                if let Some(spec) = desired_spec {
                    for (id, u) in spec.get_job_map().iter() {
                        if !u.enabled {
                            continue;
                        }
                        let Some(obs) = &u.observe else {
                            continue;
                        };
                        let desired_revision_id = spec.id.clone().expect("revision id is required");

                        if let Some(liv) = &obs.liveness {
                            let due = next_liveness.get(id).copied().unwrap_or(now);
                            if now >= due {
                                next_liveness.insert(id.clone(), now + liv.every);
                                let _ = self
                                    .run_observe_check(
                                        ObserveKind::Liveness,
                                        &u.id,
                                        &desired_revision_id,
                                        u,
                                        liv,
                                    )
                                    .await;
                            }
                        }
                        if let Some(health) = &obs.health {
                            let due = next_health.get(id).copied().unwrap_or(now);
                            if now >= due {
                                next_health.insert(id.clone(), now + health.every);
                                let _ = self
                                    .run_observe_check(
                                        ObserveKind::Health,
                                        &u.id,
                                        &desired_revision_id,
                                        u,
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

    async fn reconcile_dirty(&self) -> Result<()> {
        let dirty_hashes: Vec<String> = {
            let dirty = self.dirty.read().await;
            if dirty.is_empty() {
                return Ok(());
            }
            dirty.iter().cloned().collect()
        };

        let desired_cfg = RevisionStore::get_desired_config(Some(self.root_dir.clone()))?;
        let desired_cfg = desired_cfg
            .as_ref()
            .ok_or_else(|| anyhow!("no desired config"))?;
        let desired_rev = desired_cfg.id.clone().expect("revision id is required");

        let prev_cfg = RevisionStore::get_previous_config(Some(self.root_dir.clone()))?;
        let prev_rev = prev_cfg.as_ref().and_then(|c| c.id.clone());

        // Phase 1: decide what to stop / start
        let mut to_stop: Vec<RunSpec> = Vec::new();
        let mut to_start: Vec<RunSpec> = Vec::new();

        for h in &dirty_hashes {
            let new_spec = desired_cfg.get_job_by_hash(h).clone();
            let old_spec = prev_cfg.as_ref().and_then(|c| c.get_job_by_hash(h).clone());

            match (old_spec, new_spec) {
                // Removed: stop old service if possible
                (Some(old), None) => {
                    if matches!(old.run_type, RunType::Service) {
                        to_stop.push(old);
                    }
                }

                // Present: if disabled stop service
                (Some(old), Some(new)) => {
                    if matches!(old.run_type, RunType::Service) && !new.enabled {
                        to_stop.push(old.clone());
                    }

                    // If it's a service AND changed (hash differs) stop old first then start new
                    if matches!(new.run_type, RunType::Service) && old.get_hash() != new.get_hash()
                    {
                        to_stop.push(old);
                        if new.enabled {
                            to_start.push(new);
                        }
                    } else {
                        // Jobs/services that are enabled and changed/new run in phase 2
                        if new.enabled && old.get_hash() != new.get_hash() {
                            to_start.push(new);
                        } else if new.enabled && matches!(new.run_type, RunType::Job) {
                            // optional: rerun jobs even if same hash is handled elsewhere
                            // (see section 3)
                        }
                    }
                }

                // Added: start if enabled
                (None, Some(new)) => {
                    if new.enabled {
                        to_start.push(new);
                    }
                }

                (None, None) => {}
            }
        }

        // Dedup by run id so you don't stop/start twice if multiple hashes map to same id
        // (keep last occurrence; simplest HashMap overwrite)
        let mut stop_by_id = std::collections::HashMap::<String, RunSpec>::new();
        for s in to_stop {
            stop_by_id.insert(s.id.clone(), s);
        }

        let mut start_by_id = std::collections::HashMap::<String, RunSpec>::new();
        for s in to_start {
            start_by_id.insert(s.id.clone(), s);
        }

        // Phase 2a: execute stops
        for (_id, spec) in stop_by_id.iter() {
            let wd = self.resolve_workdir(spec).await?;
            let rev = prev_rev.clone().unwrap_or_else(|| desired_rev.clone());
            let _ = self.stop_service(spec, &rev, &wd).await;
        }

        // Phase 2b: execute starts/runs
        for (_id, spec) in start_by_id.iter() {
            let wd = self.resolve_workdir(spec).await?;

            match spec.run_type {
                RunType::Observe => {}
                RunType::Job => {
                    if spec.enabled {
                        self.maybe_run_job(spec, &desired_rev, &wd).await?;
                    }
                }
                RunType::Service => {
                    if spec.enabled {
                        self.apply_service(spec, &desired_rev, &wd).await?;
                    }
                }
            }
        }

        // Finally: clear processed dirty hashes
        let mut dirty = self.dirty.write().await;
        for h in dirty_hashes {
            dirty.remove(&h);
        }

        Ok(())
    }

    async fn maybe_run_job(&self, spec: &RunSpec, revision_id: &str, wd: &Path) -> Result<()> {
        // normal job
        self.execute_unit_steps(spec, revision_id, wd).await
    }

    async fn apply_service(&self, spec: &RunSpec, revision_id: &str, wd: &Path) -> Result<()> {
        self.execute_unit_steps(spec, revision_id, wd).await
    }

    async fn stop_service(&self, spec: &RunSpec, revision_id: &str, wd: &Path) -> Result<()> {
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
        // if ephemeral workspace, delete it
        if let Some(workdir) = &spec.workdir {
            // change to match and remove local run state if persistent
            match workdir.mode {
                WorkdirMode::Persistent => {
                    let path = self.get_workspace_path(spec)?;
                    LocalRunState::delete(&path)?;
                }
                WorkdirMode::Ephemeral => {
                    let path = self.get_workspace_path(spec)?;
                    tokio::fs::remove_dir_all(path).await?;
                }
            }
        }
        Ok(())
    }

    async fn execute_unit_steps(&self, spec: &RunSpec, revision_id: &str, wd: &Path) -> Result<()> {
        let mut st = LocalRunState::load(wd)?;
        if st.ran_successful {
            // already done
            return Ok(());
        }
        // materialize files (only if any)
        self.materialize_files(spec, wd).await?;

        let res = match self
            .execute_steps(
                &spec.id,
                &revision_id.to_string(),
                wd,
                &spec.env,
                &spec.steps,
                spec.on_failure.as_ref(),
            )
            .await
        {
            Ok(()) => {
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
        };

        st.ran_successful = true;
        LocalRunState::save(wd, &st)?;

        res
    }

    async fn run_observe_check(
        &self,
        kind: ObserveKind,
        run_id: &str,
        revision_id: &str,
        spec: &RunSpec,
        hooks: &ObserveHooks,
    ) -> Result<()> {
        let r = self
            .run_observe(kind, run_id, revision_id, spec, hooks)
            .await;

        match r {
            Ok(d) if d.consecutive > 0 => {
                tracing::info!("{} check had {} consecutive failures", &kind, d.consecutive);
                let _ = self
                    .check_rollback_on_observe_failure(
                        kind,
                        revision_id,
                        r.map(|d| d.clone().consecutive).ok(),
                    )
                    .await?;
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
        spec: &RunSpec,
        hooks: &ObserveHooks,
    ) -> Result<ObserveDecision> {
        let wd = self.resolve_workdir(spec).await?;
        let mut st = LocalRunState::load(&wd)?;

        let observe_timeout = hooks.observe_timeout.unwrap_or(kind.default_timeout());

        let res = run_command(
            run_id,
            &wd,
            &spec.env,
            &hooks.observe,
            Some(observe_timeout),
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

                    match kind {
                        ObserveKind::Health => {
                            st.reported_health_once = true;
                            st.last_health = true;
                            st.consecutive_health_failures = 0;
                        }
                        ObserveKind::Liveness => {
                            st.reported_alive_once = true;
                            st.last_alive = true;
                            st.consecutive_alive_failures = 0;
                        }
                    }
                    LocalRunState::save(&wd, &st)?;
                }

                Ok(ObserveDecision {
                    is_failure: false,
                    needs_send,
                    consecutive: 0,
                })
            }
            Err(e) => {
                let failures = st.failures_mut(kind);
                *failures = failures.saturating_add(1);
                let consecutive = *failures;

                let decision = kind.decide_on_error(&st, hooks, consecutive);
                // if decision.is_failure {
                //     match kind {
                //         ObserveKind::Health => {
                //             st.last_health = false;
                //         }
                //         ObserveKind::Liveness => {
                //             st.last_alive = false;
                //             st.last_health = false;
                //         }
                //     }
                // }

                LocalRunState::save(&wd, &st)?;

                let mut on_fail_log_tail = None;
                let mut record_log_tail = None;
                if decision.is_failure {
                    if let Some(record) = &hooks.record {
                        let record_timeout = hooks.record_timeout.unwrap_or(kind.default_timeout());

                        let r = run_command(
                            run_id,
                            &wd,
                            &spec.env,
                            record,
                            Some(record_timeout),
                            MAX_TAIL_BYTES,
                        )
                        .await;

                        record_log_tail = match r {
                            Ok(tail) => Some(tail),
                            Err(RunCommandError::Failed(cmd)) => Some(cmd.combined_tail),
                            Err(RunCommandError::Io(_)) => None,
                            Err(RunCommandError::Other(_)) => None,
                        };
                    }

                    if let Some(report) = &hooks.report {
                        let report_timeout = hooks.report_timeout.unwrap_or(kind.default_timeout());

                        let r = run_command(
                            run_id,
                            &wd,
                            &spec.env,
                            report,
                            Some(report_timeout),
                            MAX_TAIL_BYTES,
                        )
                        .await;

                        on_fail_log_tail = match r {
                            Ok(tail) => Some(tail),
                            Err(RunCommandError::Failed(cmd)) => Some(cmd.combined_tail),
                            Err(RunCommandError::Io(_)) => None,
                            Err(RunCommandError::Other(_)) => None,
                        };
                    }
                }

                if decision.needs_send {
                    let observe_log_tail = match &e {
                        RunCommandError::Failed(cmd) => Some(cmd.combined_tail.clone()),
                        _ => None,
                    };

                    let log_tail = merge_log_tails(observe_log_tail, record_log_tail);
                    let log_tail = merge_log_tails(log_tail, on_fail_log_tail);

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

                    match kind {
                        ObserveKind::Health => {
                            st.reported_health_once = true;
                            st.last_health = false;
                        }
                        ObserveKind::Liveness => {
                            st.reported_alive_once = true;
                            st.last_health = false;
                        }
                    }

                    LocalRunState::save(&wd, &st)?;
                }

                match kind {
                    ObserveKind::Liveness => Ok(decision),
                    ObserveKind::Health => Ok(decision),
                }
            }
        }
    }

    async fn check_rollback_on_observe_failure(
        &self,
        kind: ObserveKind,
        revision_id: &str,
        consecutive: Option<u32>,
    ) -> Result<()> {
        use m87_shared::deploy_spec::RollbackTrigger;

        let policy = match &*self.rollback_policy.read().await {
            Some(p) => p.clone(),
            None => return Ok(()),
        };

        if !self.is_past_stabilization_period(&policy).await {
            return Ok(());
        }

        let trigger = match kind {
            ObserveKind::Health => &policy.on_health_failure,
            ObserveKind::Liveness => &policy.on_liveness_failure,
        };

        let should_rollback = match trigger {
            RollbackTrigger::Never => false,
            RollbackTrigger::Any => consecutive.unwrap_or(0) > 0,
            RollbackTrigger::All => self.check_all_units_failing().await?,
            RollbackTrigger::Consecutive(n) => consecutive.unwrap_or(0) >= *n,
        };

        if should_rollback {
            tracing::warn!(
                "{} failure triggered rollback for revision_id {}",
                kind,
                revision_id
            );
            self.trigger_rollback(revision_id).await?;
        }

        Ok(())
    }

    async fn is_past_stabilization_period(&self, policy: &RollbackPolicy) -> bool {
        let deployment_time = self.deployment_started_at.read().await;

        match *deployment_time {
            None => true, // No deployment time tracked, allow rollback
            Some(start) => {
                let elapsed = start.elapsed();
                elapsed.as_secs() >= policy.stabilization_period_secs
            }
        }
    }

    async fn check_all_units_failing(&self) -> Result<bool> {
        let desired = match RevisionStore::get_desired_config(Some(self.root_dir.clone()))? {
            Some(config) => config.get_job_map(),
            None => return Ok(false),
        };

        if desired.is_empty() {
            return Ok(false);
        }

        let mut all_failing = true;
        for (_id, spec) in &desired {
            let wd = self.resolve_workdir(spec).await?;
            if let Ok(st) = LocalRunState::load(&wd) {
                if st.consecutive_health_failures == 0 {
                    all_failing = false;
                    break;
                }
            }
        }

        Ok(all_failing)
    }

    async fn trigger_rollback(&self, revision_id: &str) -> Result<()> {
        tracing::warn!("ROLLBACK TRIGGERED - Reverting to previous configuration");

        // Load previous configuration
        let prev_config = match RevisionStore::get_previous_config(Some(self.root_dir.clone()))? {
            Some(config) => config,
            None => {
                tracing::error!("No previous configuration available for rollback");
                let _ = enqueue_event(
                    DeployReportKind::RollbackReport(RollbackReport {
                        revision_id: revision_id.to_string(),
                        new_revision_id: None,
                    }),
                    Some(self.root_dir.clone()),
                );
                return Err(anyhow!("No previous configuration available"));
            }
        };

        tracing::info!(
            "Rolling back to previous configuration with {} units",
            prev_config.jobs.len()
        );

        // Apply previous configuration (this will reset deployment_started_at)
        self.set_desired_units(prev_config.clone()).await?;

        // TODO: this jsut changes the target revision. Rollback happens in the main loop ehwn this returns
        let _ = enqueue_event(
            DeployReportKind::RollbackReport(RollbackReport {
                revision_id: revision_id.to_string(),
                new_revision_id: prev_config.id,
            }),
            Some(self.root_dir.clone()),
        );

        tracing::info!("Rollback complete");
        Ok(())
    }

    async fn execute_steps(
        &self,
        run_id: &str,
        revision_id: &str,
        wd: &Path,
        env: &BTreeMap<String, String>,
        steps: &[Step],
        on_failure: Option<&OnFailure>,
    ) -> Result<()> {
        let policy = on_failure.cloned().unwrap_or(OnFailure {
            undo: UndoMode::None,
            continue_on_failure: false,
        });

        let mut executed: Vec<&Step> = Vec::new();

        for step in steps {
            let res = self
                .run_step_with_retry(run_id, revision_id, wd, env, step)
                .await;
            match res {
                Ok(()) => executed.push(step),
                Err(e) => {
                    // Undo
                    tracing::error!("Failed to run step: {}", e);
                    match policy.undo {
                        UndoMode::None => {}
                        UndoMode::ExecutedSteps => {
                            self.undo_steps(run_id, revision_id, wd, env, &executed)
                                .await;
                        }
                    }

                    if policy.continue_on_failure {
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    async fn undo_steps(
        &self,
        unit_id: &str,
        revision_id: &str,
        wd: &Path,
        env: &BTreeMap<String, String>,
        steps: &[&Step],
    ) {
        for step in steps.iter().rev() {
            if let Some(undo) = &step.undo {
                // if undo fails we dont care for now. run_step takes care of sending the event to the user
                let _ = run_undo(
                    unit_id,
                    wd,
                    env,
                    undo,
                    step,
                    revision_id,
                    MAX_TAIL_BYTES,
                    Some(self.root_dir.clone()),
                )
                .await;
            }
        }
    }

    async fn run_step_with_retry(
        &self,
        unit_id: &str,
        revision_id: &str,
        wd: &Path,
        env: &BTreeMap<String, String>,
        step: &Step,
    ) -> Result<()> {
        let retry = step.retry.clone().unwrap_or(RetrySpec {
            attempts: 1,
            backoff: Duration::from_millis(0),
            on_exit_codes: None,
        });

        let attempts = retry.attempts.max(1);
        for i in 0..attempts {
            let res = run_step(
                unit_id,
                wd,
                env,
                step,
                revision_id,
                i,
                MAX_TAIL_BYTES,
                Some(self.root_dir.clone()),
            )
            .await;
            match res {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if i + 1 >= attempts {
                        return Err(e);
                    }
                    sleep(retry.backoff).await;
                }
            }
        }

        return Err(anyhow!("Failed to run command"));
    }

    async fn resolve_workdir(&self, spec: &RunSpec) -> Result<PathBuf> {
        // observe-only still gets a deterministic cwd for relative paths in log/health commands
        let resolved = self.get_workspace_path(spec)?;

        tokio::fs::create_dir_all(&resolved).await?;
        Ok(resolved)
    }

    fn get_workspace_path(&self, spec: &RunSpec) -> Result<PathBuf> {
        let base = if let Some(wd) = &spec.workdir {
            if let Some(p) = &wd.path {
                PathBuf::from(p)
            } else {
                self.root_dir.join("jobs").join(&spec.id)
            }
        } else {
            self.root_dir.join("jobs").join(&spec.id)
        };

        // choose persistent/ephemeral
        let mode = spec
            .workdir
            .as_ref()
            .map(|w| w.mode.clone())
            .unwrap_or(WorkdirMode::Persistent);

        let resolved = match mode {
            WorkdirMode::Persistent => base,
            WorkdirMode::Ephemeral => self
                .root_dir
                .join("tmp")
                .join("jobs")
                .join(&spec.get_hash()),
        };

        Ok(resolved)
    }

    async fn materialize_files(&self, spec: &RunSpec, wd: &Path) -> Result<()> {
        if spec.files.is_empty() {
            return Ok(());
        }
        for (rel, content) in &spec.files {
            let p = wd.join(rel);
            if let Some(parent) = p.parent() {
                let res = tokio::fs::create_dir_all(parent).await;
                if let Err(e) = &res {
                    tracing::error!("Failed to create directory: {}", e);
                }
                res?;
            }
            tokio::fs::write(&p, content).await?;
        }
        Ok(())
    }
}

pub async fn enqueue_event(event: DeployReportKind, root_dir: Option<PathBuf>) -> Result<()> {
    ensure_dirs(root_dir.clone()).await?;

    let id = event.get_hash().to_string();
    let pending = pending_dir(root_dir)?.join(format!("{id}.json"));
    let tmp = pending.with_extension("json.tmp");

    let bytes = serde_json::to_vec(&event).context("serialize event")?;

    let mut f = fs::File::create(&tmp).await.context("create tmp")?;
    f.write_all(&bytes).await.context("write tmp")?;
    f.flush().await.context("flush tmp")?;
    drop(f);

    fs::rename(&tmp, &pending)
        .await
        .context("atomic rename tmp->pending")?;
    Ok(())
}

pub struct ClaimedEvent {
    pub path: PathBuf, // inflight file path
    pub report: DeployReportKind,
}

pub async fn recover_inflight(root_dir: Option<PathBuf>) -> Result<()> {
    ensure_dirs(root_dir.clone()).await?;
    let inflight = inflight_dir(root_dir.clone())?;
    let pending = pending_dir(root_dir)?;

    let mut rd = fs::read_dir(&inflight).await?;
    while let Some(e) = rd.next_entry().await? {
        let p = e.path();
        if p.extension().and_then(|s| s.to_str()) == Some("json") {
            let target = pending.join(p.file_name().unwrap());
            let _ = fs::rename(&p, &target).await;
        }
    }
    Ok(())
}

pub async fn claim_next_event(root_dir: Option<PathBuf>) -> anyhow::Result<Option<ClaimedEvent>> {
    ensure_dirs(root_dir.clone()).await?;

    let pending = pending_dir(root_dir.clone())?;
    let inflight = inflight_dir(root_dir)?;

    // collect candidate paths
    let mut paths = Vec::new();
    let mut rd = fs::read_dir(&pending).await?;
    while let Some(e) = rd.next_entry().await? {
        let p = e.path();
        if p.extension().and_then(|s| s.to_str()) == Some("json") {
            paths.push(p);
        }
    }

    // compute keys (async) then sort
    let mut keyed = Vec::with_capacity(paths.len());
    for p in paths {
        let t = match fs::metadata(&p).await {
            Ok(m) => m.created().unwrap_or(SystemTime::UNIX_EPOCH),
            Err(_) => SystemTime::UNIX_EPOCH,
        };
        keyed.push((t, p));
    }

    keyed.sort_by_key(|(t, _)| *t);

    let Some((_t, p)) = keyed.into_iter().next() else {
        return Ok(None);
    };

    let inflight_path = inflight.join(p.file_name().unwrap());
    fs::rename(&p, &inflight_path)
        .await
        .context("claim rename pending->inflight")?;

    let bytes = fs::read(&inflight_path).await.context("read inflight")?;
    let event: DeployReportKind = serde_json::from_slice(&bytes).context("parse inflight")?;

    Ok(Some(ClaimedEvent {
        path: inflight_path,
        report: event,
    }))
}

pub async fn ack_event(hash: &str, root_dir: Option<PathBuf>) -> Result<()> {
    let path = inflight_dir(root_dir)?.join(format!("{hash}.json"));
    fs::remove_file(&path).await.context("delete inflight")?;
    Ok(())
}

pub async fn on_new_event(root_dir: Option<PathBuf>) -> Option<ClaimedEvent> {
    loop {
        // Try immediately (covers backlog + missed cycles)
        match claim_next_event(root_dir.clone()).await {
            Ok(Some(ev)) => return Some(ev),
            Ok(None) => {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            Err(e) => {
                tracing::error!("event queue error: {e}");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

async fn run_step(
    unit_id: &str,
    wd: &Path,
    env: &BTreeMap<String, String>,
    step: &Step,
    revision_id: &str,
    i: u32,
    max_tail_bytes: usize,
    root_dir: Option<PathBuf>,
) -> Result<()> {
    tracing::info!(
        "running step {}. Attempt {}",
        step.name.clone().unwrap_or(format!("{}", step.run)),
        i + 1
    );
    let res = run_command(unit_id, wd, env, &step.run, step.timeout, max_tail_bytes).await;
    let res = match res {
        Ok(tail) => Ok(StepReport {
            revision_id: revision_id.to_string(),
            run_id: unit_id.to_string(),
            name: step.name.clone(),
            attempts: i + 1,
            log_tail: tail,
            exit_code: None,
            success: true,
            is_undo: false,
            error: None,
            report_time: now_ms_u64(),
        }),
        Err(RunCommandError::Other(e)) => {
            tracing::error!(
                "Failed to run step {}: {}",
                step.name.clone().unwrap_or(format!("{}", step.run)),
                e
            );
            Err(e)
        }
        Err(RunCommandError::Io(e)) => {
            tracing::error!(
                "Failed to run step {}: {}",
                step.name.clone().unwrap_or(format!("{}", step.run)),
                e
            );
            Err(e.into())
        }
        Err(RunCommandError::Failed(e)) => Ok(StepReport {
            revision_id: revision_id.to_string(),
            run_id: unit_id.to_string(),
            name: step.name.clone(),
            attempts: i + 1,
            log_tail: e.combined_tail,
            exit_code: e.exit_code,
            success: false,
            is_undo: false,
            error: e.error,
            report_time: now_ms_u64(),
        }),
    };
    match res {
        Ok(report) => {
            enqueue_event(DeployReportKind::StepReport(report.clone()), root_dir).await?;
            if !report.success {
                Err(anyhow!(
                    "Step {} failed: {}",
                    step.name.clone().unwrap_or("unknown step".to_string()),
                    report.error.unwrap_or("unknown error".to_string())
                ))
            } else {
                Ok(())
            }
        }

        Err(e) => Err(e),
    }
}

async fn run_undo(
    unit_id: &str,
    wd: &Path,
    env: &BTreeMap<String, String>,
    undo: &Undo,
    step: &Step,
    revision_id: &str,
    max_tail_bytes: usize,
    root_dir: Option<PathBuf>,
) -> Result<()> {
    tracing::info!(
        "undo step {}",
        step.name.clone().unwrap_or(format!("{}", step.run))
    );
    let res = run_command(unit_id, wd, env, &undo.run, undo.timeout, max_tail_bytes).await;
    let res = match res {
        Ok(tail) => Ok(StepReport {
            revision_id: revision_id.to_string(),
            run_id: unit_id.to_string(),
            name: step.name.clone(),
            attempts: 0,
            is_undo: true,
            log_tail: tail,
            exit_code: None,
            success: true,
            error: None,
            report_time: now_ms_u64(),
        }),
        Err(RunCommandError::Other(e)) => Err(e),
        Err(RunCommandError::Io(e)) => Err(e.into()),
        Err(RunCommandError::Failed(e)) => Ok(StepReport {
            revision_id: revision_id.to_string(),
            run_id: unit_id.to_string(),
            name: step.name.clone(),
            attempts: 0,
            is_undo: true,
            log_tail: e.combined_tail,
            exit_code: e.exit_code,
            success: false,
            error: e.error,
            report_time: now_ms_u64(),
        }),
    };
    match res {
        Ok(report) => {
            enqueue_event(DeployReportKind::StepReport(report.clone()), root_dir).await?;
            if !report.success {
                Err(anyhow!(
                    "Step {} failed: {}",
                    step.name.clone().unwrap_or("unknown step".to_string()),
                    report.error.unwrap_or("unknown error".to_string())
                ))
            } else {
                Ok(())
            }
        }

        Err(e) => Err(e),
    }
}

fn merge_log_tails(primary: Option<String>, secondary: Option<String>) -> Option<String> {
    match (primary, secondary) {
        (Some(a), Some(b)) => Some(format!("{}\n{}", a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use m87_shared::deploy_spec::{CommandSpec, RebootMode, StopSpec, Workdir};
    use tempfile::TempDir;

    fn sh(s: impl Into<String>) -> CommandSpec {
        CommandSpec::Sh(s.into())
    }

    fn step(name: &str, cmd: CommandSpec) -> Step {
        Step {
            name: Some(name.to_string()),
            run: cmd,
            timeout: Some(Duration::from_secs(5)),
            retry: None,
            undo: None,
        }
    }

    fn mk_service(id: &str, start_cmd: CommandSpec, stop_cmd: CommandSpec) -> RunSpec {
        RunSpec {
            id: id.to_string(),
            run_type: RunType::Service,
            enabled: true,
            workdir: Some(Workdir {
                mode: WorkdirMode::Persistent,
                path: None,
            }),
            files: BTreeMap::new(),
            env: BTreeMap::new(),
            steps: vec![step("start", start_cmd)],
            on_failure: None,
            stop: Some(StopSpec {
                steps: vec![step("stop", stop_cmd)],
            }),
            reboot: RebootMode::None,
            observe: None,
        }
    }

    fn mk_rev(id: &str, jobs: Vec<RunSpec>) -> DeploymentRevision {
        DeploymentRevision {
            id: Some(id.to_string()),
            jobs,
            rollback: None,
        }
    }

    #[tokio::test]
    async fn dirty_add_change_remove_hashes() -> Result<()> {
        let td = TempDir::new()?;
        let base = td.path().join("m87"); // this is your data_dir root

        let mgr = DeploymentManager::new(Some(base.clone())).await?;

        // add v1
        let v1 = mk_rev(
            "rev1",
            vec![mk_service("svc", sh("echo start_v1"), sh("echo stop_v1"))],
        );
        let h1 = v1.get_job_map().keys().next().unwrap().clone();

        mgr.set_desired_units(v1).await?;
        assert!(mgr.dirty.read().await.contains(&h1));
        mgr.dirty.write().await.clear();

        // change same id (different hash) by changing command
        let v2 = mk_rev(
            "rev2",
            vec![mk_service("svc", sh("echo start_v2"), sh("echo stop_v2"))],
        );
        let h2 = v2.get_job_map().keys().next().unwrap().clone();

        mgr.set_desired_units(v2).await?;
        let d = mgr.dirty.read().await.clone();
        assert!(d.contains(&h1), "old hash should be dirty");
        assert!(d.contains(&h2), "new hash should be dirty");
        mgr.dirty.write().await.clear();

        // remove: v2 hash should be marked dirty so reconcile can stop it
        let v3 = mk_rev("rev3", vec![]);
        mgr.set_desired_units(v3).await?;
        assert!(
            mgr.dirty.read().await.contains(&h2),
            "removed hash should be dirty"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_stops_before_starts_on_change() -> Result<()> {
        let td = TempDir::new()?;
        let base = td.path().join("m87");

        let mgr = DeploymentManager::new(Some(base.clone())).await?;

        let order = td.path().join("order.txt");
        let marker = td.path().join("marker.txt");
        let order_s = order.display().to_string();
        let marker_s = marker.display().to_string();

        // v1
        let v1 = mk_rev(
            "rev1",
            vec![mk_service(
                "svc",
                sh(format!("echo start_v1 >> {order_s}; touch {marker_s}")),
                sh(format!("echo stop_v1 >> {order_s}; rm -f {marker_s}")),
            )],
        );

        mgr.set_desired_units(v1).await?;
        mgr.reconcile_dirty().await?;
        assert!(marker.exists(), "expected v1 running marker");

        // v2 (same id, different content => different hash)
        let v2 = mk_rev(
            "rev2",
            vec![mk_service(
                "svc",
                sh(format!("echo start_v2 >> {order_s}; touch {marker_s}")),
                sh(format!("echo stop_v2 >> {order_s}; rm -f {marker_s}")),
            )],
        );

        mgr.set_desired_units(v2).await?;
        mgr.reconcile_dirty().await?;

        let contents = std::fs::read_to_string(&order)?;
        let lines: Vec<&str> = contents.lines().collect();

        let stop_v1 = lines
            .iter()
            .position(|l| *l == "stop_v1")
            .expect("missing stop_v1");
        let start_v2 = lines
            .iter()
            .position(|l| *l == "start_v2")
            .expect("missing start_v2");

        assert!(stop_v1 < start_v2, "stop_v1 must come before start_v2");
        assert!(marker.exists(), "expected v2 running marker");

        // remove -> should stop v2
        let v3 = mk_rev("rev3", vec![]);
        mgr.set_desired_units(v3).await?;
        mgr.reconcile_dirty().await?;
        assert!(!marker.exists(), "expected marker removed after stop");

        Ok(())
    }
}
