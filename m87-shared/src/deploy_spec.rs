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

    pub fn get_job_by_hash(&self, run_hash: &str) -> Option<RunSpec> {
        let res = self.jobs.iter().find(|u| u.get_hash() == run_hash);
        res.cloned()
    }

    pub fn get_job_by_id(&self, run_id: &str) -> Option<RunSpec> {
        let res = self.jobs.iter().find(|u| u.id == run_id);
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
}

impl Display for CreateDeployRevisionBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let json = serde_json::to_string_pretty(self).unwrap();
        write!(f, "{}", json)
    }
}

#[derive(Deserialize, Serialize, Default)]
pub struct UpdateDeployRevisionBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    // yaml of the new run spec
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_run_spec: Option<String>,
    // yaml of the updated run spec
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_run_spec: Option<String>,
    // id of the run spec to remove
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove_run_spec_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
}

impl Display for UpdateDeployRevisionBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // converto json and rint
        let json = serde_json::to_string_pretty(self).unwrap();
        write!(f, "{}", json)
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<Workdir>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,

    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<OnFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop: Option<StopSpec>,
    #[serde(default)]
    pub reboot: RebootMode,

    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    // skip if default
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
    #[serde(with = "duration_human")]
    pub backoff: Duration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct LivenessSpec {
//     #[serde(flatten)]
//     pub hooks: ObserveHooks,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct HealthSpec {
//     #[serde(flatten)]
//     pub hooks: ObserveHooks,
// }

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
    pub report_time: u64,
    #[serde(default)]
    pub log_tail: Option<String>,
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

    pub fn get_run_id(&self) -> Option<String> {
        match self {
            DeployReportKind::DeploymentRevisionReport(_) => None,
            DeployReportKind::RunReport(r) => Some(r.run_id.clone()),
            DeployReportKind::StepReport(r) => Some(r.run_id.clone()),
            DeployReportKind::RollbackReport(_) => None,
            DeployReportKind::RunState(r) => Some(r.run_id.clone()),
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

pub mod duration_human {
    use super::*;
    use serde::{
        Deserializer, Serializer,
        de::{self, Visitor},
    };
    use std::fmt;

    pub fn serialize<S>(d: &Duration, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let secs = d.as_secs();

        if secs % 3600 == 0 {
            s.serialize_str(&format!("{}h", secs / 3600))
        } else if secs % 60 == 0 {
            s.serialize_str(&format!("{}m", secs / 60))
        } else {
            s.serialize_str(&format!("{}s", secs))
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurationVisitor;

        impl<'de> Visitor<'de> for DurationVisitor {
            type Value = Duration;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a duration like 10s, 5m, or 2h")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let (num, unit) = v.chars().partition::<String, _>(|c| c.is_ascii_digit());

                let value: u64 = num.parse().map_err(E::custom)?;

                match unit.as_str() {
                    "s" => Ok(Duration::from_secs(value)),
                    "m" => Ok(Duration::from_secs(value * 60)),
                    "h" => Ok(Duration::from_secs(value * 3600)),
                    _ => Err(E::custom("invalid duration unit (use s, m, h)")),
                }
            }
        }

        d.deserialize_str(DurationVisitor)
    }
}

pub mod option_duration_human {
    use super::duration_human;
    use serde::{Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(v: &Option<Duration>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match v {
            Some(d) => duration_human::serialize(d, s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Some(duration_human::deserialize(d)?))
    }
}

pub fn build_instruction_hash(deploy_hash: &str, config_hash: &str) -> String {
    format!("{}-{}", deploy_hash, config_hash)
}
