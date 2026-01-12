use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::time::Duration;

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

fn hash_json<T: Serialize>(v: &T) -> String {
    let data = serde_json::to_vec(v).expect("hash_json serialization must not fail");
    sha256_hex(data)
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRevision {
    // sha2 hash of the deployment revision
    #[serde(default)]
    pub id: Option<String>,
    pub jobs: Vec<RunSpec>,
    #[serde(default)]
    pub rollback: Option<RollbackPolicy>,
}

impl DeploymentRevision {
    pub fn new(units: Vec<RunSpec>, rollback: Option<RollbackPolicy>) -> Self {
        let rev = Self {
            id: Some(uuid::Uuid::new_v4().to_string()),
            jobs: units,
            rollback,
        };
        rev
    }

    pub fn empty() -> Self {
        Self {
            id: Some(uuid::Uuid::new_v4().to_string()),
            jobs: Vec::new(),
            rollback: None,
        }
    }

    pub fn clone_with_new_id(&self) -> Self {
        let mut clone = self.clone();
        clone.id = Some(uuid::Uuid::new_v4().to_string());
        clone
    }

    pub fn get_hash(&self) -> String {
        let mut hasher = Sha256::new();
        for u in &self.jobs {
            hasher.update(u.get_hash().as_bytes());
        }
        if let Some(r) = &self.rollback {
            let data = serde_json::to_vec(&(
                &r.on_health_failure,
                &r.on_liveness_failure,
                r.stabilization_period_secs,
            ))
            .expect("This should be serializable");
            hasher.update(data);
        }
        format!("{:x}", hasher.finalize())
    }

    pub fn get_job_map(&self) -> BTreeMap<String, RunSpec> {
        self.jobs
            .iter()
            .filter(|u| u.enabled)
            .map(|u| (u.get_hash(), u.clone()))
            .collect()
    }

    pub fn get_job(&self, run_id: &str) -> Option<RunSpec> {
        let res = self.jobs.iter().find(|u| u.get_hash() == run_id);
        res.cloned()
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        let mut rev: Self = serde_yaml::from_str(yaml)?;
        // if id is none create uuid with hash as seed
        if rev.id.is_none() {
            let seed = rev.get_hash().parse::<u128>().unwrap();
            let id = uuid::Uuid::from_u128(
                seed & 0xFFFFFFFFFFFF4FFFBFFFFFFFFFFFFFFF | 0x40008000000000000000,
            );
            rev.id = Some(id.to_string());
        }
        Ok(rev)
    }

    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDeployRevisionBody {
    /// YAML string for DeploymentRevision.
    pub revision: String,
    #[serde(default)]
    pub active: Option<bool>,
}

#[derive(Deserialize, Serialize, Default)]
pub struct UpdateDeployRevisionBody {
    #[serde(default)]
    pub revision: Option<String>,
    // yaml of the new run spec
    #[serde(default)]
    pub add_run_spec: Option<String>,
    // yaml of the updated run spec
    #[serde(default)]
    pub update_run_spec: Option<String>,
    // id of the run spec to remove
    #[serde(default)]
    pub remove_run_spec_id: Option<String>,
    #[serde(default)]
    pub active: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackPolicy {
    /// Automatically rollback if health checks fail
    #[serde(default)]
    pub on_health_failure: RollbackTrigger,
    /// Automatically rollback if liveness checks fail
    #[serde(default)]
    pub on_liveness_failure: RollbackTrigger,
    /// Time window to monitor for failures before considering deployment stable
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
    60 // 1 minute
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RollbackTrigger {
    /// Never rollback automatically
    #[default]
    Never,
    /// Rollback if any unit fails
    Any,
    /// Rollback only if all units fail
    All,
    /// Rollback if a specific number of consecutive failures
    Consecutive(u32),
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct RunSpec {
    pub id: String,
    #[serde(rename = "type")]
    pub run_type: RunType,
    pub enabled: bool,

    // service / job only
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

impl RunSpec {
    pub fn new(
        id: String,
        run_type: RunType,
        enabled: bool,
        workdir: Option<Workdir>,
        files: BTreeMap<String, String>,
        env: BTreeMap<String, String>,
        steps: Vec<Step>,
        on_failure: Option<OnFailure>,
        stop: Option<StopSpec>,
        reboot: RebootMode,
        observe: Option<ObserveSpec>,
    ) -> Self {
        Self {
            id,
            run_type,
            enabled,
            workdir,
            files,
            env,
            steps,
            on_failure,
            stop,
            reboot,
            observe,
        }
    }

    pub fn get_hash(&self) -> String {
        hash_json(&self)
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        let rev: Self = serde_yaml::from_str(yaml)?;
        Ok(rev)
    }

    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    pub fn enable(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum RunType {
    #[default]
    Service,
    Job,
    Observe,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum RebootMode {
    #[default]
    None,
    Request,
    Auto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    #[serde(default)]
    pub name: Option<String>,
    pub run: CommandSpec,
    #[serde(default)]
    pub timeout: Option<Duration>,
    #[serde(default)]
    pub retry: Option<RetrySpec>,
    #[serde(default)]
    pub undo: Option<Undo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Undo {
    pub run: CommandSpec,
    #[serde(default)]
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OnFailure {
    #[serde(default)]
    pub undo: UndoMode,
    #[serde(default)]
    pub continue_on_failure: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UndoMode {
    #[default]
    None,
    ExecutedSteps,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrySpec {
    pub attempts: u32,
    pub backoff: Duration,
    #[serde(default)]
    pub on_exit_codes: Option<Vec<i32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CommandSpec {
    /// executed as: /bin/sh -lc "<string>"
    Sh(String),
    /// execve-style argv
    Argv(Vec<String>),
}

impl Display for CommandSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandSpec::Sh(cmd) => write!(f, "sh -lc {}", cmd),
            CommandSpec::Argv(args) => write!(f, "{}", args.join(" ")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopSpec {
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveSpec {
    #[serde(default)]
    pub logs: Option<LogSpec>,
    #[serde(default)]
    pub liveness: Option<LivenessSpec>,
    #[serde(default)]
    pub health: Option<HealthSpec>,
}

fn default_max_log_bytes() -> u64 {
    262144
}

fn default_max_log_lines() -> u32 {
    1024
}

fn default_log_timeout() -> Duration {
    Duration::from_secs(5)
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LogLimit {
    #[serde(default = "default_max_log_bytes")]
    pub max_bytes: u64,
    #[serde(default = "default_max_log_lines")]
    pub max_lines: u32,
    #[serde(default = "default_log_timeout")]
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSpec {
    pub tail: CommandSpec,
    #[serde(default)]
    pub follow: Option<CommandSpec>,
    #[serde(default)]
    pub since: Option<Duration>,
    #[serde(default)]
    pub limits: Option<LogLimit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessSpec {
    pub every: Duration,
    pub check: CommandSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSpec {
    pub every: Duration,
    pub run: CommandSpec,
    pub fails_after: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
pub struct Workdir {
    #[serde(default)]
    pub mode: WorkdirMode,
    #[serde(default)]
    pub path: Option<String>, // if omitted: agent uses root_dir/programs/<id>
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum WorkdirMode {
    #[default]
    Persistent,
    Ephemeral,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRevisionReport {
    pub revision_id: String,
    pub outcome: Outcome,
    pub dirty: bool,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub run_id: String,
    pub revision_id: String,

    pub outcome: Outcome,

    /// If outcome is failure, set an error string.
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepReport {
    pub revision_id: String,
    pub run_id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub attempts: u32,
    #[serde(default)]
    pub exit_code: Option<i32>,
    pub report_time: u64,

    /// Whether the step ultimately succeeded.
    pub success: bool,

    #[serde(default)]
    /// Whether the step is an undo step.
    pub is_undo: bool,

    /// If it failed, short error text.
    #[serde(default)]
    pub error: Option<String>,

    /// Best-effort log tail for this step only (bounded).
    #[serde(default)]
    pub log_tail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackReport {
    pub revision_id: String,
    /// Whether rollback completed successfully.
    pub success: bool,

    /// Which step indexes had undo executed (reverse order typically).
    #[serde(default)]
    pub undone_steps: Vec<u32>,

    /// Any rollback error.
    #[serde(default)]
    pub error: Option<String>,

    #[serde(default)]
    pub log_tail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub run_id: String,
    pub revision_id: String,
    pub healthy: Option<bool>,
    pub alive: Option<bool>,
    pub report_time: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum DeployReportKind {
    DeploymentRevisionReport(DeploymentRevisionReport),
    RunReport(RunReport),
    StepReport(StepReport),
    RollbackReport(RollbackReport),
    RunState(RunState),
}

impl DeployReportKind {
    pub fn get_revision_id(&self) -> &str {
        match self {
            DeployReportKind::DeploymentRevisionReport(r) => &r.revision_id,
            DeployReportKind::RunReport(r) => &r.revision_id,
            DeployReportKind::StepReport(r) => &r.revision_id,
            DeployReportKind::RollbackReport(r) => &r.revision_id,
            DeployReportKind::RunState(r) => &r.revision_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployReport {
    pub device_id: String,
    pub revision_id: String,
    pub kind: DeployReportKind,

    /// TTL target
    pub expires_at: Option<u64>,

    /// When the report was received/created
    pub created_at: u64,
}
