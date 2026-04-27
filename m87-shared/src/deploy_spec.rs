use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Internal hash helpers
// ---------------------------------------------------------------------------

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

fn hash_json<T: Serialize>(v: &T) -> String {
    let data = serde_json::to_vec(v).expect("hash_json serialization must not fail");
    sha256_hex(data)
}

// ---------------------------------------------------------------------------
// File-reference resolution error
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ResolveFilesError {
    MissingFile {
        key: String,
        path: PathBuf,
    },
    Io {
        key: String,
        path: PathBuf,
        source: io::Error,
    },
}

impl Display for ResolveFilesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveFilesError::MissingFile { key, path } => write!(
                f,
                "files['{}'] references missing file: {}",
                key,
                path.display()
            ),
            ResolveFilesError::Io { key, path, source } => write!(
                f,
                "failed to read files['{}'] from {}: {}",
                key,
                path.display(),
                source
            ),
        }
    }
}

impl std::error::Error for ResolveFilesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ResolveFilesError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Lifecycle – runtime state of any managed unit
// ---------------------------------------------------------------------------

/// Desired runtime state for a service, observer, or job definition.
///
/// - `Running`  – unit is active (default).
/// - `Paused`   – observe polling / scheduling suspended; process keeps running.
/// - `Stopped`  – unit is fully stopped; stop-steps have been / will be executed.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Lifecycle {
    #[default]
    Running,
    Paused,
    Stopped,
}

impl Lifecycle {
    pub fn is_running(&self) -> bool {
        matches!(self, Lifecycle::Running)
    }
    pub fn is_paused(&self) -> bool {
        matches!(self, Lifecycle::Paused)
    }
    pub fn is_stopped(&self) -> bool {
        matches!(self, Lifecycle::Stopped)
    }
}

impl Display for Lifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Lifecycle::Running => write!(f, "running"),
            Lifecycle::Paused => write!(f, "paused"),
            Lifecycle::Stopped => write!(f, "stopped"),
        }
    }
}

// ---------------------------------------------------------------------------
// RestartPolicy – controls automatic service recovery
// ---------------------------------------------------------------------------

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    /// Never restart automatically.
    Never,
    /// Restart when liveness / health observe checks fail (the sensible default).
    #[default]
    OnFailure,
    /// Always restart whenever the unit is found stopped.
    Always,
}

// ---------------------------------------------------------------------------
// ServiceSpec – a managed unit that runs startup steps and/or observe hooks.
//
// When `steps` is empty the spec behaves as a *pure observer* (no startup
// phase).  The two YAML sections `services:` and `observers:` both deserialize
// into this type; the section determines semantic intent and validation.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub id: String,

    /// Desired lifecycle (default: Running).
    #[serde(default)]
    pub lifecycle: Lifecycle,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<Workdir>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,

    /// Startup steps.  Empty = pure observer (no startup phase).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<Step>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<OnFailure>,

    /// Observe hooks (liveness / health).
    /// Optional on a service, semantically required on an observer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observe: Option<ObserveSpec>,

    /// Graceful-stop steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop: Option<StopSpec>,

    #[serde(default, skip_serializing_if = "RebootMode::is_none")]
    pub reboot: RebootMode,

    /// Automatic-restart behaviour when observe checks fail.
    #[serde(default)]
    pub restart: RestartPolicy,
}

impl ServiceSpec {
    pub fn get_hash(&self) -> String {
        hash_json(self)
    }

    /// True when this spec has no startup steps (pure observer).
    pub fn is_observer(&self) -> bool {
        self.steps.is_empty()
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    pub fn resolve_file_references(
        &mut self,
        base_dir: Option<PathBuf>,
    ) -> Result<(), ResolveFilesError> {
        resolve_files_map(&mut self.files, base_dir)
    }
}

// ---------------------------------------------------------------------------
// JobDef – a reusable job template; triggered explicitly per run.
//
// No observe hooks, no stop steps – jobs are one-shot executions.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDef {
    pub id: String,

    /// `Running` = can be triggered; `Stopped` = disabled (cannot be triggered).
    #[serde(default)]
    pub lifecycle: Lifecycle,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<Workdir>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,

    #[serde(default)]
    pub steps: Vec<Step>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<OnFailure>,

    #[serde(default, skip_serializing_if = "RebootMode::is_none")]
    pub reboot: RebootMode,
}

impl JobDef {
    pub fn get_hash(&self) -> String {
        hash_json(self)
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    pub fn resolve_file_references(
        &mut self,
        base_dir: Option<PathBuf>,
    ) -> Result<(), ResolveFilesError> {
        resolve_files_map(&mut self.files, base_dir)
    }
}

// ---------------------------------------------------------------------------
// JobRun – one triggered execution of a JobDef
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum JobRunStatus {
    Queued,
    Running,
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRun {
    /// Unique ID for this specific execution.
    pub run_id: String,
    /// References `JobDef.id` in the active revision.
    pub job_def_id: String,
    pub revision_id: String,
    /// Per-trigger environment variable overrides (merged over `JobDef.env`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env_overrides: BTreeMap<String, String>,
    pub status: JobRunStatus,
    pub enqueued_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// LifecycleUpdate – sent server → device via heartbeat to change runtime state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleUpdate {
    pub unit_id: String,
    pub lifecycle: Lifecycle,
}

// ---------------------------------------------------------------------------
// UnitKind – discriminates services / observers / job-runs in status output
// ---------------------------------------------------------------------------

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum UnitKind {
    #[default]
    Service,
    Observer,
    Job,
}

impl Display for UnitKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnitKind::Service => write!(f, "service"),
            UnitKind::Observer => write!(f, "observer"),
            UnitKind::Job => write!(f, "job"),
        }
    }
}

// ---------------------------------------------------------------------------
// DeploymentRevision – the full desired state pushed to a device
// ---------------------------------------------------------------------------

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRevision {
    #[serde(default)]
    pub id: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<ServiceSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observers: Vec<ServiceSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<JobDef>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackPolicy>,
}

impl Display for DeploymentRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let json = serde_json::to_string_pretty(self).unwrap();
        write!(f, "{}", json)
    }
}

impl DeploymentRevision {
    pub fn new(
        services: Vec<ServiceSpec>,
        observers: Vec<ServiceSpec>,
        jobs: Vec<JobDef>,
        rollback: Option<RollbackPolicy>,
    ) -> Self {
        Self {
            id: Some(uuid::Uuid::new_v4().to_string()),
            services,
            observers,
            jobs,
            rollback,
        }
    }

    pub fn empty() -> Self {
        Self {
            id: Some(uuid::Uuid::new_v4().to_string()),
            ..Default::default()
        }
    }

    pub fn clone_with_new_id(&self) -> Self {
        let mut clone = self.clone();
        clone.id = Some(uuid::Uuid::new_v4().to_string());
        clone
    }

    pub fn get_hash(&self) -> String {
        let mut hasher = Sha256::new();
        for s in &self.services {
            hasher.update(s.get_hash().as_bytes());
        }
        for o in &self.observers {
            hasher.update(o.get_hash().as_bytes());
        }
        for j in &self.jobs {
            hasher.update(j.get_hash().as_bytes());
        }
        if let Some(r) = &self.rollback {
            let data = serde_json::to_vec(&(
                &r.on_health_failure,
                &r.on_liveness_failure,
                r.stabilization_period_secs,
            ))
            .expect("RollbackPolicy must be serializable");
            hasher.update(data);
        }
        format!("{:x}", hasher.finalize())
    }

    // --- accessor maps (keyed by spec hash) --------------------------------

    /// `hash → ServiceSpec` for active services.
    pub fn get_service_map(&self) -> BTreeMap<String, ServiceSpec> {
        self.services
            .iter()
            .filter(|s| !s.lifecycle.is_stopped())
            .map(|s| (s.get_hash(), s.clone()))
            .collect()
    }

    /// `hash → ServiceSpec` for active observers.
    pub fn get_observer_map(&self) -> BTreeMap<String, ServiceSpec> {
        self.observers
            .iter()
            .filter(|o| !o.lifecycle.is_stopped())
            .map(|o| (o.get_hash(), o.clone()))
            .collect()
    }

    /// `id → JobDef` for enabled job definitions.
    pub fn get_job_map(&self) -> BTreeMap<String, JobDef> {
        self.jobs
            .iter()
            .filter(|j| j.lifecycle.is_running())
            .map(|j| (j.id.clone(), j.clone()))
            .collect()
    }

    // --- point lookups ------------------------------------------------------

    pub fn get_service_by_hash(&self, hash: &str) -> Option<ServiceSpec> {
        self.services.iter().find(|s| s.get_hash() == hash).cloned()
    }

    pub fn get_service_by_id(&self, id: &str) -> Option<ServiceSpec> {
        self.services.iter().find(|s| s.id == id).cloned()
    }

    pub fn get_observer_by_hash(&self, hash: &str) -> Option<ServiceSpec> {
        self.observers
            .iter()
            .find(|o| o.get_hash() == hash)
            .cloned()
    }

    pub fn get_observer_by_id(&self, id: &str) -> Option<ServiceSpec> {
        self.observers.iter().find(|o| o.id == id).cloned()
    }

    pub fn get_job_by_id(&self, id: &str) -> Option<JobDef> {
        self.jobs.iter().find(|j| j.id == id).cloned()
    }

    // --- YAML I/O -----------------------------------------------------------

    /// Parse a revision YAML.
    ///
    /// Accepts both the new format (`services:` / `observers:` / `jobs:`) and
    /// the legacy format (`jobs: [{type: service|job|observe, ...}]`).
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        let raw: serde_yaml::Value = serde_yaml::from_str(yaml)?;

        // Detect legacy format: `jobs` list whose first element has a `type` field.
        let is_legacy = raw
            .get("jobs")
            .and_then(|j| j.as_sequence())
            .and_then(|s| s.first())
            .and_then(|f| f.get("type"))
            .is_some();

        let mut rev: Self = if is_legacy {
            let legacy: LegacyDeploymentRevision = serde_yaml::from_value(raw)?;
            legacy.into()
        } else {
            serde_yaml::from_value(raw)?
        };

        if rev.id.is_none() {
            rev.id = Some(derive_id_from_hash(&rev));
        }

        Ok(rev)
    }

    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    pub fn resolve_file_references(
        &mut self,
        base_dir: Option<PathBuf>,
    ) -> Result<(), ResolveFilesError> {
        for s in &mut self.services {
            s.resolve_file_references(base_dir.clone())?;
        }
        for o in &mut self.observers {
            o.resolve_file_references(base_dir.clone())?;
        }
        for j in &mut self.jobs {
            j.resolve_file_references(base_dir.clone())?;
        }
        Ok(())
    }
}

/// Derive a stable v4 UUID from the content hash (used when no `id` is provided).
fn derive_id_from_hash(rev: &DeploymentRevision) -> String {
    let h = rev.get_hash();
    let prefix = &h[..h.len().min(32)];
    // If parsing fails, fall back to a random UUID.
    let seed = match u128::from_str_radix(prefix, 16) {
        Ok(s) => s,
        Err(_) => return uuid::Uuid::new_v4().to_string(),
    };
    let mut bytes = seed.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC4122 variant
    uuid::Uuid::from_bytes(bytes).to_string()
}

// ---------------------------------------------------------------------------
// Legacy format support
//
// Old YAML: `jobs: [{id: x, type: service|job|observe, enabled: bool, ...}]`
// ---------------------------------------------------------------------------

/// Only used during legacy deserialization; not part of the public API.
#[derive(Debug, Deserialize)]
struct LegacyDeploymentRevision {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    jobs: Vec<LegacyRunSpec>,
    #[serde(default)]
    rollback: Option<RollbackPolicy>,
}

impl From<LegacyDeploymentRevision> for DeploymentRevision {
    fn from(legacy: LegacyDeploymentRevision) -> Self {
        let mut services = Vec::new();
        let mut observers = Vec::new();
        let mut jobs = Vec::new();

        for spec in legacy.jobs {
            let lifecycle = if spec.enabled {
                Lifecycle::Running
            } else {
                Lifecycle::Stopped
            };
            match spec.run_type {
                LegacyRunType::Service => services.push(ServiceSpec {
                    id: spec.id,
                    lifecycle,
                    workdir: spec.workdir,
                    files: spec.files,
                    env: spec.env,
                    steps: spec.steps,
                    on_failure: spec.on_failure,
                    observe: spec.observe,
                    stop: spec.stop,
                    reboot: spec.reboot,
                    restart: RestartPolicy::OnFailure,
                }),
                LegacyRunType::Observe => observers.push(ServiceSpec {
                    id: spec.id,
                    lifecycle,
                    workdir: spec.workdir,
                    files: spec.files,
                    env: spec.env,
                    steps: vec![],
                    on_failure: None,
                    observe: spec.observe,
                    stop: spec.stop,
                    reboot: spec.reboot,
                    restart: RestartPolicy::OnFailure,
                }),
                LegacyRunType::Job => jobs.push(JobDef {
                    id: spec.id,
                    lifecycle,
                    workdir: spec.workdir,
                    files: spec.files,
                    env: spec.env,
                    steps: spec.steps,
                    on_failure: spec.on_failure,
                    reboot: spec.reboot,
                }),
            }
        }

        Self {
            id: legacy.id,
            services,
            observers,
            jobs,
            rollback: legacy.rollback,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum LegacyRunType {
    #[default]
    Service,
    Job,
    Observe,
}

/// Flat mirror of the old `RunSpec` – used only during legacy parsing.
#[derive(Debug, Deserialize)]
struct LegacyRunSpec {
    pub id: String,
    #[serde(rename = "type", default)]
    pub run_type: LegacyRunType,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub workdir: Option<Workdir>,
    #[serde(default)]
    pub files: BTreeMap<String, String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default)]
    pub on_failure: Option<OnFailure>,
    #[serde(default)]
    pub stop: Option<StopSpec>,
    #[serde(default)]
    pub reboot: RebootMode,
    #[serde(default)]
    pub observe: Option<ObserveSpec>,
}

fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// API request/response bodies
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDeployRevisionBody {
    /// YAML string for `DeploymentRevision`.
    pub revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
}

impl Display for CreateDeployRevisionBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap())
    }
}

/// Body for patching the active revision.
///
/// Only one mutation field may be set per request (except `active` which may
/// accompany `revision`).
#[derive(Deserialize, Serialize, Default)]
pub struct UpdateDeployRevisionBody {
    /// Replace the whole revision (YAML string of `DeploymentRevision`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,

    // --- typed add/update (new format) ------------------------------------
    /// YAML of a `ServiceSpec` to upsert into `services`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_service: Option<String>,
    /// YAML of a `ServiceSpec` to upsert into `observers`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_observer: Option<String>,
    /// YAML of a `JobDef` to upsert into `jobs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_job: Option<String>,

    /// Remove a unit from any section by its `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove_unit_id: Option<String>,

    /// Push a runtime lifecycle change; delivered to the device via heartbeat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_update: Option<LifecycleUpdate>,

    /// Activate / deactivate this revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,

    // --- legacy fields kept for backward compatibility --------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_run_spec: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_run_spec: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove_run_spec_id: Option<String>,
}

impl Display for UpdateDeployRevisionBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap())
    }
}

/// Body for triggering a job run.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TriggerJobBody {
    /// Per-trigger environment variable overrides merged on top of `JobDef.env`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env_overrides: BTreeMap<String, String>,
}

// ---------------------------------------------------------------------------
// RollbackPolicy
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackPolicy {
    /// Rollback trigger when health checks fail.
    #[serde(default)]
    pub on_health_failure: RollbackTrigger,
    /// Rollback trigger when liveness checks fail.
    #[serde(default)]
    pub on_liveness_failure: RollbackTrigger,
    /// Seconds to monitor before declaring the deployment stable.
    #[serde(default = "default_stabilization_period")]
    pub stabilization_period_secs: u64,
}

impl RollbackPolicy {
    pub fn new(
        on_health_failure: RollbackTrigger,
        on_liveness_failure: RollbackTrigger,
        stabilization_period_secs: u64,
    ) -> Self {
        Self {
            on_health_failure,
            on_liveness_failure,
            stabilization_period_secs,
        }
    }
}

fn default_stabilization_period() -> u64 {
    60
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RollbackTrigger {
    #[default]
    Never,
    Any,
    All,
    Consecutive(u32),
}

// ---------------------------------------------------------------------------
// Step, retry, undo, on-failure
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub run: CommandSpec,
    #[serde(
        default,
        with = "option_duration_human",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetrySpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub undo: Option<Undo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Undo {
    pub run: CommandSpec,
    #[serde(
        default,
        with = "option_duration_human",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OnFailure {
    #[serde(default)]
    pub undo: UndoMode,
    #[serde(default)]
    pub continue_on_failure: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UndoMode {
    #[default]
    None,
    ExecutedSteps,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrySpec {
    pub attempts: u32,
    #[serde(
        default,
        with = "option_duration_human",
        skip_serializing_if = "Option::is_none"
    )]
    pub backoff: Option<Duration>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_exit_codes: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CommandSpec {
    Sh(String),
    Argv(Vec<String>),
}

impl Display for CommandSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandSpec::Sh(s) => write!(f, "{}", s),
            CommandSpec::Argv(args) => write!(f, "{}", args.join(" ")),
        }
    }
}

// ---------------------------------------------------------------------------
// Stop spec
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopSpec {
    pub steps: Vec<Step>,
}

// ---------------------------------------------------------------------------
// Observe spec and hooks
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logs: Option<LogSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liveness: Option<ObserveHooks>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<ObserveHooks>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follow: Option<CommandSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveHooks {
    #[serde(with = "duration_human")]
    pub every: Duration,
    pub observe: CommandSpec,
    #[serde(
        default,
        with = "option_duration_human",
        skip_serializing_if = "Option::is_none"
    )]
    pub observe_timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<CommandSpec>,
    #[serde(
        default,
        with = "option_duration_human",
        skip_serializing_if = "Option::is_none"
    )]
    pub record_timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<CommandSpec>,
    #[serde(
        default,
        with = "option_duration_human",
        skip_serializing_if = "Option::is_none"
    )]
    pub report_timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fails_after: Option<u32>,
}

impl Default for ObserveHooks {
    fn default() -> Self {
        Self {
            every: Duration::from_secs(10),
            observe: CommandSpec::Sh("echo 'No observe command specified'".to_string()),
            observe_timeout: None,
            record: None,
            record_timeout: None,
            report: None,
            report_timeout: None,
            fails_after: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Workdir
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workdir {
    #[serde(default)]
    pub mode: WorkdirMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum WorkdirMode {
    #[default]
    Persistent,
    Ephemeral,
}

// ---------------------------------------------------------------------------
// RebootMode
// ---------------------------------------------------------------------------

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum RebootMode {
    #[default]
    None,
    Request,
    Auto,
}

impl RebootMode {
    pub fn is_none(&self) -> bool {
        matches!(self, RebootMode::None)
    }
}

// ---------------------------------------------------------------------------
// Shared file-reference resolver (used by ServiceSpec and JobDef)
// ---------------------------------------------------------------------------

fn resolve_files_map(
    files: &mut BTreeMap<String, String>,
    base_dir: Option<PathBuf>,
) -> Result<(), ResolveFilesError> {
    use std::{fs, path::Path};

    let base_dir = base_dir.unwrap_or_else(|| Path::new(".").to_path_buf());
    let mut resolved = BTreeMap::new();

    for (key, value) in std::mem::take(files) {
        let full_path = {
            let p = PathBuf::from(&value);
            if p.is_absolute() { p } else { base_dir.join(p) }
        };

        let content = if !value.contains('\n') && full_path.is_file() {
            let raw = fs::read_to_string(&full_path).map_err(|e| ResolveFilesError::Io {
                key: key.clone(),
                path: full_path,
                source: e,
            })?;
            normalize_newlines(&raw)
        } else {
            value
        };

        resolved.insert(key, content);
    }

    *files = resolved;
    Ok(())
}

fn normalize_newlines(s: &str) -> String {
    if s.contains('\r') {
        s.replace("\r\n", "\n").replace('\r', "\n")
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// Outcome
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Success,
    Failed,
    Unknown,
}

impl Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Outcome::Success => write!(f, "success"),
            Outcome::Failed => write!(f, "failed"),
            Outcome::Unknown => write!(f, "unknown"),
        }
    }
}

// ---------------------------------------------------------------------------
// Telemetry / report types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRevisionReport {
    pub revision_id: String,
    pub outcome: Outcome,
    pub dirty: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub run_id: String,
    pub revision_id: String,
    pub outcome: Outcome,
    pub report_time: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepReport {
    pub revision_id: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub report_time: u64,
    pub success: bool,
    pub is_undo: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackReport {
    pub revision_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_revision_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObserveKind {
    Alive,
    Healthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub run_id: String,
    pub revision_id: String,
    pub healthy: Option<bool>,
    pub alive: Option<bool>,
    pub report_time: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
}

impl RunState {
    pub fn as_observe_update(&self) -> &Self {
        self
    }
}

/// Job run status report – sent from device → server when a `JobRun` completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunReport {
    pub run_id: String,
    pub job_def_id: String,
    pub revision_id: String,
    pub status: JobRunStatus,
    pub report_time: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeployReportKind {
    DeploymentRevisionReport(DeploymentRevisionReport),
    RunReport(RunReport),
    StepReport(StepReport),
    RollbackReport(RollbackReport),
    RunState(RunState),
    JobRunReport(JobRunReport),
}

impl DeployReportKind {
    pub fn get_revision_id(&self) -> Option<&str> {
        match self {
            DeployReportKind::DeploymentRevisionReport(r) => Some(&r.revision_id),
            DeployReportKind::RunReport(r) => Some(&r.revision_id),
            DeployReportKind::StepReport(r) => Some(&r.revision_id),
            DeployReportKind::RollbackReport(r) => Some(&r.revision_id),
            DeployReportKind::RunState(r) => Some(&r.revision_id),
            DeployReportKind::JobRunReport(r) => Some(&r.revision_id),
        }
    }

    pub fn get_run_id(&self) -> Option<&str> {
        match self {
            DeployReportKind::DeploymentRevisionReport(_) => None,
            DeployReportKind::RunReport(r) => Some(&r.run_id),
            DeployReportKind::StepReport(r) => Some(&r.run_id),
            DeployReportKind::RollbackReport(_) => None,
            DeployReportKind::RunState(r) => Some(&r.run_id),
            DeployReportKind::JobRunReport(r) => Some(&r.run_id),
        }
    }

    pub fn get_hash(&self) -> String {
        hash_json(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployReport {
    pub device_id: String,
    pub revision_id: String,
    pub kind: DeployReportKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    pub created_at: u64,
}

// ---------------------------------------------------------------------------
// Duration serde helpers
// ---------------------------------------------------------------------------

pub mod duration_human {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        let secs = d.as_secs_f64();
        if secs < 60.0 {
            s.serialize_str(&format!("{}s", d.as_secs()))
        } else if secs < 3600.0 {
            s.serialize_str(&format!("{}m", d.as_secs() / 60))
        } else {
            s.serialize_str(&format!("{}h", d.as_secs() / 3600))
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Duration;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "a duration string like '30s', '5m', '1h'")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Duration, E> {
                parse_duration(v).map_err(E::custom)
            }
        }
        d.deserialize_str(V)
    }
}

pub mod option_duration_human {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(dur) => duration_human::serialize(dur, s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Option<Duration>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "an optional duration string")
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Option<Duration>, E> {
                Ok(None)
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Option<Duration>, D::Error> {
                duration_human::deserialize(d).map(Some)
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Option<Duration>, E> {
                parse_duration(v).map(Some).map_err(E::custom)
            }
        }
        d.deserialize_option(V)
    }
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix("ms") {
        return n
            .trim()
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| format!("invalid duration '{}': {}", s, e));
    }
    if let Some(n) = s.strip_suffix('s') {
        return n
            .trim()
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| format!("invalid duration '{}': {}", s, e));
    }
    if let Some(n) = s.strip_suffix('m') {
        return n
            .trim()
            .parse::<u64>()
            .map(|v| Duration::from_secs(v * 60))
            .map_err(|e| format!("invalid duration '{}': {}", s, e));
    }
    if let Some(n) = s.strip_suffix('h') {
        return n
            .trim()
            .parse::<u64>()
            .map(|v| Duration::from_secs(v * 3600))
            .map_err(|e| format!("invalid duration '{}': {}", s, e));
    }
    // bare integer → seconds
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|e| format!("invalid duration '{}': {}", s, e))
}

// ---------------------------------------------------------------------------
// Instruction hash (used in heartbeat to detect staleness)
// ---------------------------------------------------------------------------

pub fn build_instruction_hash(deploy_hash: &str, config_hash: &str) -> String {
    sha256_hex(format!("{deploy_hash}{config_hash}"))
}

// ---------------------------------------------------------------------------
// Status snapshot types (returned by the status API)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentStatusSnapshot {
    pub revision_id: String,
    pub outcome: Outcome,
    pub dirty: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackStatus>,
    pub runs: Vec<RunStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStatus {
    pub run_id: String,
    /// Whether the unit is enabled (not stopped).
    pub enabled: bool,
    pub unit_kind: UnitKind,
    pub outcome: Outcome,
    pub last_update: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alive: Option<ObserveStatusItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthy: Option<ObserveStatusItem>,
    pub steps: Vec<StepStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepStatus {
    pub step_id: String,
    pub name: String,
    pub is_undo: bool,
    pub defined_in_spec: bool,
    pub state: StepState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_update: Option<u64>,
    pub attempt: Option<StepAttemptStatus>,
    pub attempts_total: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepAttemptStatus {
    pub n: u32,
    pub report_time: u64,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveStatusItem {
    pub report_time: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_revision_id: Option<String>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StepState {
    #[default]
    Pending,
    Running,
    Success,
    Failed,
    Skipped,
}

// ---------------------------------------------------------------------------
// Failure aggregation types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SliceLevel {
    Days,
    Hours,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketTotals {
    pub crashes: u64,
    pub unhealthy_checks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketRow {
    pub start_ts_ms: u64,
    pub end_ts_ms: u64,
    pub total: BucketTotals,
    pub by_run: BTreeMap<String, BucketTotals>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureAggResponse {
    pub level: SliceLevel,
    pub runs: Vec<String>,
    pub buckets: Vec<BucketRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureAggQuery {
    pub from_ts_ms: u64,
    pub to_ts_ms: u64,
    pub bucket_ms: u64,
    pub level: SliceLevel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
}

// ---------------------------------------------------------------------------
// ObserveStatus (used in device status endpoint)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveStatus {
    pub name: String,
    pub alive: Option<bool>,
    pub healthy: Option<bool>,
    pub crashes: u32,
    pub unhealthy_checks: u32,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_service_yaml(id: &str) -> String {
        format!(
            r#"
services:
  - id: {id}
    steps:
      - name: start
        run: echo start
    stop:
      steps:
        - name: stop
          run: echo stop
    observe:
      liveness:
        every: 30s
        observe: echo alive
"#
        )
    }

    fn mk_observer_yaml(id: &str) -> String {
        format!(
            r#"
observers:
  - id: {id}
    observe:
      health:
        every: 60s
        observe: curl -f http://localhost/health
"#
        )
    }

    fn mk_job_yaml(id: &str) -> String {
        format!(
            r#"
jobs:
  - id: {id}
    steps:
      - name: run
        run: echo running job
"#
        )
    }

    #[test]
    fn parse_new_format_service() {
        let yaml = mk_service_yaml("web");
        let rev = DeploymentRevision::from_yaml(&yaml).unwrap();
        assert_eq!(rev.services.len(), 1);
        assert_eq!(rev.observers.len(), 0);
        assert_eq!(rev.jobs.len(), 0);
        let svc = &rev.services[0];
        assert_eq!(svc.id, "web");
        assert!(!svc.is_observer());
        assert!(svc.observe.is_some());
    }

    #[test]
    fn parse_new_format_observer() {
        let yaml = mk_observer_yaml("api-health");
        let rev = DeploymentRevision::from_yaml(&yaml).unwrap();
        assert_eq!(rev.observers.len(), 1);
        assert_eq!(rev.services.len(), 0);
        let obs = &rev.observers[0];
        assert_eq!(obs.id, "api-health");
        assert!(obs.is_observer());
    }

    #[test]
    fn parse_new_format_job() {
        let yaml = mk_job_yaml("migrate");
        let rev = DeploymentRevision::from_yaml(&yaml).unwrap();
        assert_eq!(rev.jobs.len(), 1);
        assert_eq!(rev.jobs[0].id, "migrate");
    }

    #[test]
    fn parse_legacy_service() {
        let yaml = r#"
jobs:
  - id: web
    type: service
    enabled: true
    steps:
      - name: start
        run: echo start
    stop:
      steps:
        - name: stop
          run: echo stop
"#;
        let rev = DeploymentRevision::from_yaml(yaml).unwrap();
        assert_eq!(rev.services.len(), 1);
        assert_eq!(rev.observers.len(), 0);
        assert_eq!(rev.jobs.len(), 0);
        assert_eq!(rev.services[0].id, "web");
        assert!(rev.services[0].lifecycle.is_running());
    }

    #[test]
    fn parse_legacy_observe() {
        let yaml = r#"
jobs:
  - id: checker
    type: observe
    enabled: true
    observe:
      health:
        every: 30s
        observe: echo ok
"#;
        let rev = DeploymentRevision::from_yaml(yaml).unwrap();
        assert_eq!(rev.observers.len(), 1);
        assert_eq!(rev.services.len(), 0);
        assert_eq!(rev.jobs.len(), 0);
    }

    #[test]
    fn parse_legacy_job() {
        let yaml = r#"
jobs:
  - id: migrate
    type: job
    steps:
      - name: run
        run: ./migrate.sh
"#;
        let rev = DeploymentRevision::from_yaml(yaml).unwrap();
        assert_eq!(rev.jobs.len(), 1);
        assert_eq!(rev.jobs[0].id, "migrate");
    }

    #[test]
    fn legacy_disabled_becomes_stopped() {
        let yaml = r#"
jobs:
  - id: web
    type: service
    enabled: false
    steps:
      - name: start
        run: echo start
"#;
        let rev = DeploymentRevision::from_yaml(yaml).unwrap();
        assert!(rev.services[0].lifecycle.is_stopped());
    }

    #[test]
    fn hash_changes_when_service_changes() {
        let yaml1 = mk_service_yaml("web");
        let mut rev1 = DeploymentRevision::from_yaml(&yaml1).unwrap();

        let yaml2 = r#"
services:
  - id: web
    steps:
      - name: start
        run: echo start_v2
"#;
        let _ = rev1;
        let rev2 = DeploymentRevision::from_yaml(yaml2).unwrap();

        assert_ne!(rev1.get_hash(), rev2.get_hash());
    }

    #[test]
    fn get_service_map_excludes_stopped() {
        let yaml = r#"
services:
  - id: active
    lifecycle: running
    steps:
      - name: start
        run: echo active
  - id: inactive
    lifecycle: stopped
    steps:
      - name: start
        run: echo inactive
"#;
        let rev = DeploymentRevision::from_yaml(&yaml).unwrap();
        let map = rev.get_service_map();
        assert_eq!(map.len(), 1);
        assert!(map.values().any(|s| s.id == "active"));
    }

    #[test]
    fn get_service_map_includes_paused() {
        let yaml = r#"
services:
  - id: paused-svc
    lifecycle: paused
    steps:
      - name: start
        run: echo paused
"#;
        let rev = DeploymentRevision::from_yaml(&yaml).unwrap();
        let map = rev.get_service_map();
        // paused units are still "known" to the reconciler (just observe is suspended)
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn stable_id_derived_from_hash() {
        let yaml = mk_service_yaml("web");
        let rev1 = DeploymentRevision::from_yaml(&yaml).unwrap();
        let rev2 = DeploymentRevision::from_yaml(&yaml).unwrap();
        assert_eq!(rev1.id, rev2.id, "same content must yield same id");
    }

    #[test]
    fn duration_roundtrip() {
        let yaml = r#"
observers:
  - id: ping
    observe:
      liveness:
        every: 45s
        observe: ping host
"#;
        let rev = DeploymentRevision::from_yaml(yaml).unwrap();
        let obs = &rev.observers[0];
        let liveness = obs.observe.as_ref().unwrap().liveness.as_ref().unwrap();
        assert_eq!(liveness.every, Duration::from_secs(45));
    }

    #[test]
    fn lifecycle_default_is_running() {
        let spec: ServiceSpec = serde_yaml::from_str("id: x\nsteps: []").unwrap();
        assert_eq!(spec.lifecycle, Lifecycle::Running);
    }

    #[test]
    fn restart_policy_default_is_on_failure() {
        let spec: ServiceSpec = serde_yaml::from_str("id: x").unwrap();
        assert_eq!(spec.restart, RestartPolicy::OnFailure);
    }

    #[test]
    fn job_run_status_roundtrip() {
        let run = JobRun {
            run_id: "r1".into(),
            job_def_id: "migrate".into(),
            revision_id: "rev1".into(),
            env_overrides: BTreeMap::new(),
            status: JobRunStatus::Queued,
            enqueued_at: 1234567890,
            started_at: None,
            completed_at: None,
            error: None,
        };
        let json = serde_json::to_string(&run).unwrap();
        let back: JobRun = serde_json::from_str(&json).unwrap();
        assert_eq!(back.status, JobRunStatus::Queued);
        assert_eq!(back.job_def_id, "migrate");
    }
}
