use anyhow::{Context, Result, anyhow, bail};
use m87_shared::deploy_spec::{
    CommandSpec, CreateDeployRevisionBody, DeployReport, DeployReportKind, DeploymentRevision,
    DeploymentStatusSnapshot, JobDef, JobRun, Lifecycle, LogSpec, ObserveHooks, ObserveSpec,
    OnFailure, RebootMode, RetrySpec, ServiceSpec, Step, StopSpec, TriggerJobBody, Undo, UndoMode,
    UpdateDeployRevisionBody, Workdir, WorkdirMode,
};
use serde_yaml::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::auth::AuthManager;
use crate::config::Config;
use crate::devices::resolve_device_cached;
use crate::server;

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum SpecType {
    Auto,
    Compose,
    Runspec,
    Deployment,
}

impl Default for SpecType {
    fn default() -> Self {
        SpecType::Auto
    }
}

fn load_file_to_string(p: &Path) -> Result<String> {
    std::fs::read_to_string(p).with_context(|| format!("failed to read file: {}", p.display()))
}

/// Compose detection: if root mapping contains `services`, treat as compose.
fn is_docker_compose_yaml(yaml: &str) -> bool {
    match serde_yaml::from_str::<Value>(yaml) {
        Ok(v) => {
            let Some(m) = v.as_mapping() else {
                return false;
            };
            m.contains_key(&Value::String("services".to_string()))
        }
        Err(_) => false,
    }
}

async fn ctx_for_device(device_name: &str) -> Result<(String, String, String, bool)> {
    let resolved = resolve_device_cached(device_name).await?;
    let token = AuthManager::get_cli_token().await?;
    let cfg = Config::load()?;
    Ok((
        resolved.id,
        resolved.url,
        token,
        cfg.trust_invalid_server_cert,
    ))
}

async fn resolve_target_deployment_id(
    device_id: &str,
    api_url: &str,
    token: &str,
    trust_invalid: bool,
    deployment_id: Option<String>,
) -> Result<Option<String>> {
    if let Some(id) = deployment_id {
        return Ok(Some(id));
    }
    let active = server::get_active_deployment_id(api_url, token, trust_invalid, device_id)
        .await
        .context("failed to get active deployment")?;
    Ok(active)
}

pub async fn deploy_file(
    device_name: &str,
    file: PathBuf,
    ty: SpecType,
    name: Option<String>,
    deployment_id: Option<String>,
) -> Result<()> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let target_dep_id =
        resolve_target_deployment_id(&device_id, &api_url, &token, trust_invalid, deployment_id)
            .await?;
    let target_dep_id = match target_dep_id {
        Some(id) => id,
        None => {
            tracing::info!("No active deployment found, creating a new one");
            let new = create_deployment(device_name, true).await?;

            let new_id = new.id.unwrap();
            tracing::info!("Created new deployment with ID: {}", new_id);
            new_id
        }
    };
    let base_dir = file.parent().map(|f| f.to_path_buf());
    // Convert input -> run-spec YAML string (typed for runspec)
    let update_body = match ty {
        SpecType::Compose => {
            let svc = compose_file_to_service_spec(&file, name.as_deref()).await?;
            UpdateDeployRevisionBody {
                add_service: Some(svc.to_yaml()?),
                ..Default::default()
            }
        }
        SpecType::Runspec => {
            // Auto-detect service vs job from the file
            let s = load_file_to_string(&file)?;
            let update = parse_unit_yaml(&s, base_dir)?;
            update
        }
        SpecType::Auto => {
            let s = load_file_to_string(&file)?;
            if is_docker_compose_yaml(&s) {
                let svc = compose_file_to_service_spec(&file, name.as_deref()).await?;
                UpdateDeployRevisionBody {
                    add_service: Some(svc.to_yaml()?),
                    ..Default::default()
                }
            } else {
                // Try new format first (DeploymentRevision with services/observers/jobs)
                match DeploymentRevision::from_yaml(&s) {
                    Ok(mut dr) => {
                        let _ = dr.resolve_file_references(base_dir);
                        UpdateDeployRevisionBody {
                            revision: Some(dr.to_yaml()?),
                            ..Default::default()
                        }
                    }
                    Err(e) => {
                        bail!("Failed to parse deployment YAML: {}", e);
                    }
                }
            }
        }
        SpecType::Deployment => {
            let s = load_file_to_string(&file)?;
            let mut dr = DeploymentRevision::from_yaml(&s)?;
            dr.resolve_file_references(base_dir)?;
            UpdateDeployRevisionBody {
                revision: Some(dr.to_yaml()?),
                ..Default::default()
            }
        }
    };

    server::update_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &target_dep_id,
        update_body,
    )
    .await
    .context("failed to add run spec")?;
    Ok(())
}

pub async fn undeploy_file(
    device_name: &str,
    job_id: String,
    deployment_id: Option<String>,
) -> Result<()> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let target_dep_id =
        resolve_target_deployment_id(&device_id, &api_url, &token, trust_invalid, deployment_id)
            .await?;
    let target_dep_id = match target_dep_id {
        Some(id) => id,
        None => {
            tracing::info!("No active deployment found, creating a new one");
            let new = create_deployment(device_name, true).await?;

            let new_id = new.id.unwrap();
            tracing::info!("Created new deployment with ID: {}", new_id);
            new_id
        }
    };

    deployment_update(
        device_name,
        DeploymentUpdateArgs {
            deployment_id: Some(target_dep_id),
            rm: vec![job_id],
            ..Default::default()
        },
    )
    .await
    .context("failed to undeploy job")?;
    Ok(())
}

pub async fn get_deployments(device_name: &str) -> Result<Vec<DeploymentRevision>> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let deployments =
        server::get_deployments(&api_url, &token, trust_invalid, &device_id, None, None)
            .await
            .context("failed to list deployments")?;

    Ok(deployments)
}

pub async fn get_active_deployment_id(device_name: &str) -> Result<Option<String>> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let active =
        server::get_active_deployment_id(&api_url, &token, trust_invalid, &device_id).await?;

    Ok(active)
}

pub async fn deployment_active_set(device_name: &str, deployment_id: String) -> Result<()> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    server::update_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &deployment_id,
        UpdateDeployRevisionBody {
            active: Some(true),
            ..Default::default()
        },
    )
    .await
    .context("failed to activate deployment")?;
    Ok(())
}

pub async fn get_deployment(device_name: &str, deployment_id: &str) -> Result<DeploymentRevision> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let deployment =
        server::get_deployment(&api_url, &token, trust_invalid, &device_id, deployment_id)
            .await
            .context("failed to get deployment")?;
    Ok(deployment)
}

pub async fn create_deployment(device_name: &str, active: bool) -> Result<DeploymentRevision> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let deployment = DeploymentRevision::empty();

    let created = server::create_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        CreateDeployRevisionBody {
            revision: deployment.to_yaml()?,
            active: Some(active),
        },
    )
    .await
    .context("failed to create deployment")?;
    Ok(created)
}

pub async fn remove_deployment(device_name: &str, deployment_id: String) -> Result<()> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    server::delete_deployment(&api_url, &token, trust_invalid, &device_id, &deployment_id)
        .await
        .context("failed to delete deployment")?;
    Ok(())
}

pub async fn clone_deployment(
    device_name: &str,
    src_deployment_id: String,
    active: bool,
) -> Result<DeploymentRevision> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let source = server::get_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &src_deployment_id,
    )
    .await
    .context("failed to fetch source deployment")?;

    let clone = source.clone_with_new_id();
    let yml = clone.to_yaml()?;
    let created = server::create_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        CreateDeployRevisionBody {
            revision: yml,
            active: Some(active),
        },
    )
    .await
    .context("failed to clone deployment")?;

    Ok(created)
}

use clap::Parser;

#[derive(Parser, Debug)]
pub struct DeploymentUpdateArgs {
    pub deployment_id: Option<String>,

    /// Remove one or more jobs: --rm <job_id> (optional <job_id2>)
    #[arg(long, action = clap::ArgAction::Append)]
    pub rm: Vec<String>,

    /// Replace a job: --replace <job_id>=<file>
    /// Can be used multiple times.
    #[arg(long, action = clap::ArgAction::Append)]
    pub replace: Vec<String>,

    /// Rename a job: --rename <job_id>=<name>
    /// Can be used multiple times.
    #[arg(long, action = clap::ArgAction::Append)]
    pub rename: Vec<String>,

    /// Enable one or more jobs
    #[arg(long, action = clap::ArgAction::Append)]
    pub enable: Vec<String>,

    /// Disable one or more jobs: --disable <job_id> (optional <job_id2>)
    #[arg(long, action = clap::ArgAction::Append)]
    pub disable: Vec<String>,

    /// Spec type for replacements (auto detects by default)
    #[arg(long, value_enum, default_value_t = SpecType::Auto)]
    pub r#type: SpecType,
}

impl Default for DeploymentUpdateArgs {
    fn default() -> Self {
        Self {
            deployment_id: None,
            rm: Vec::new(),
            replace: Vec::new(),
            rename: Vec::new(),
            enable: Vec::new(),
            disable: Vec::new(),
            r#type: SpecType::Auto,
        }
    }
}

pub async fn deployment_update(
    device_name: &str,
    args: DeploymentUpdateArgs,
) -> Result<DeploymentRevision> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let deployment_id = match args.deployment_id {
        Some(d) => d,
        None => match get_active_deployment_id(device_name).await? {
            Some(id) => id,
            None => {
                tracing::error!(
                    "No active deployment found for device {}. Specify a deployment id or create a active deployment",
                    device_name
                );
                return Err(anyhow!(
                    "No active deployment found for device {}",
                    device_name
                ));
            }
        },
    };

    let needs_full_revision_patch =
        !args.rename.is_empty() || !args.enable.is_empty() || !args.disable.is_empty();

    if !needs_full_revision_patch {
        for id in &args.rm {
            server::update_deployment(
                &api_url,
                &token,
                trust_invalid,
                &device_id,
                &deployment_id,
                UpdateDeployRevisionBody {
                    remove_run_spec_id: Some(id.clone()),
                    ..Default::default()
                },
            )
            .await
            .with_context(|| format!("failed to remove run spec {id}"))?;
        }

        for rep in &args.replace {
            let (spec_id, path) = parse_kv_eq(rep)?;
            let path = PathBuf::from(path);

            let s = load_file_to_string(&path)?;
            let base = path.parent().map(|p| p.to_path_buf());
            let update = if is_docker_compose_yaml(&s) {
                let svc = compose_file_to_service_spec(&path, Some(&spec_id)).await?;
                UpdateDeployRevisionBody {
                    add_service: Some(svc.to_yaml()?),
                    ..Default::default()
                }
            } else {
                parse_unit_yaml(&s, base)?
            };

            server::update_deployment(
                &api_url,
                &token,
                trust_invalid,
                &device_id,
                &deployment_id,
                update,
            )
            .await
            .with_context(|| format!("failed to update run spec {spec_id}"))?;
        }

        let revision =
            server::get_deployment(&api_url, &token, trust_invalid, &device_id, &deployment_id)
                .await
                .context("failed to fetch updated deployment")?;
        return Ok(revision);
    }

    let mut dep =
        server::get_deployment(&api_url, &token, trust_invalid, &device_id, &deployment_id)
            .await
            .context("failed to fetch deployment")?;

    for id in &args.rm {
        dep.services.retain(|s| s.id != *id);
        dep.observers.retain(|s| s.id != *id);
        dep.jobs.retain(|s| s.id != *id);
    }

    for rep in &args.replace {
        let (spec_id, path) = parse_kv_eq(rep)?;
        let path = PathBuf::from(path);
        let s = load_file_to_string(&path)?;
        // Try service first, then job
        if let Ok(mut svc) = ServiceSpec::from_yaml(&s) {
            svc.id = spec_id.clone();
            if let Some(idx) = dep.services.iter().position(|s| s.id == spec_id) {
                dep.services[idx] = svc;
            } else if let Some(idx) = dep.observers.iter().position(|s| s.id == spec_id) {
                dep.observers[idx] = svc;
            } else {
                bail!("spec_id not found for --replace: {spec_id}");
            }
        } else if let Ok(mut jd) = JobDef::from_yaml(&s) {
            jd.id = spec_id.clone();
            let idx = dep
                .jobs
                .iter()
                .position(|j| j.id == spec_id)
                .with_context(|| format!("spec_id not found for --replace: {spec_id}"))?;
            dep.jobs[idx] = jd;
        } else {
            bail!("failed to parse replacement spec for {spec_id}");
        }
    }

    for r in &args.rename {
        let (spec_id, new_name) = parse_kv_eq(r)?;
        let mut found = false;
        for svc in dep.services.iter_mut().chain(dep.observers.iter_mut()) {
            if svc.id == spec_id {
                svc.id = new_name.clone();
                found = true;
                break;
            }
        }
        if !found {
            if let Some(jd) = dep.jobs.iter_mut().find(|j| j.id == spec_id) {
                jd.id = new_name;
            } else {
                bail!("spec_id not found for --rename: {spec_id}");
            }
        }
    }

    for id in &args.enable {
        let mut found = false;
        for svc in dep.services.iter_mut().chain(dep.observers.iter_mut()) {
            if svc.id == *id {
                svc.lifecycle = Lifecycle::Running;
                found = true;
                break;
            }
        }
        if !found {
            if let Some(jd) = dep.jobs.iter_mut().find(|j| j.id == *id) {
                jd.lifecycle = Lifecycle::Running;
            } else {
                bail!("spec_id not found for --enable: {id}");
            }
        }
    }
    for id in &args.disable {
        let mut found = false;
        for svc in dep.services.iter_mut().chain(dep.observers.iter_mut()) {
            if svc.id == *id {
                svc.lifecycle = Lifecycle::Stopped;
                found = true;
                break;
            }
        }
        if !found {
            if let Some(jd) = dep.jobs.iter_mut().find(|j| j.id == *id) {
                jd.lifecycle = Lifecycle::Stopped;
            } else {
                bail!("spec_id not found for --disable: {id}");
            }
        }
    }

    server::update_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &deployment_id,
        UpdateDeployRevisionBody {
            revision: Some(dep.to_yaml()?),
            ..Default::default()
        },
    )
    .await
    .context("failed to update deployment")?;

    server::get_deployment(&api_url, &token, trust_invalid, &device_id, &deployment_id)
        .await
        .context("failed to fetch updated deployment")
}

fn parse_kv_eq(s: &str) -> Result<(String, String)> {
    let (k, v) = s.split_once('=').context("expected format <id>=<value>")?;
    Ok((k.to_string(), v.to_string()))
}

/// Parse a unit YAML (ServiceSpec or JobDef) and return the appropriate UpdateDeployRevisionBody.
fn parse_unit_yaml(yaml: &str, base_dir: Option<PathBuf>) -> Result<UpdateDeployRevisionBody> {
    // Try ServiceSpec first (has more fields; JobDef is a subset)
    if let Ok(mut svc) = ServiceSpec::from_yaml(yaml) {
        svc.resolve_file_references(base_dir)?;
        return Ok(UpdateDeployRevisionBody {
            add_service: Some(svc.to_yaml()?),
            ..Default::default()
        });
    }
    if let Ok(mut jd) = JobDef::from_yaml(yaml) {
        jd.resolve_file_references(base_dir)?;
        return Ok(UpdateDeployRevisionBody {
            add_job: Some(jd.to_yaml()?),
            ..Default::default()
        });
    }
    bail!("failed to parse as ServiceSpec or JobDef")
}

pub async fn compose_file_to_service_spec(file: &Path, name: Option<&str>) -> Result<ServiceSpec> {
    // Read compose file (kept verbatim; we do not attempt to interpret/transform compose contents).
    let compose = tokio::fs::read_to_string(file)
        .await
        .with_context(|| format!("failed to read compose file: {}", file.display()))?;

    let file_name = file
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "docker-compose.yml".to_string());

    let id = if let Some(n) = name.filter(|s| !s.trim().is_empty()) {
        n.to_string()
    } else if let Some(stem) = file
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
    {
        stem.to_string()
    } else {
        return Err(anyhow!(
            "cannot derive ServiceSpec id: pass name or use a compose file with a valid name"
        ));
    };

    let mut files = BTreeMap::new();
    files.insert(file_name.clone(), compose);

    let pull = Step {
        name: Some("pull".to_string()),
        run: CommandSpec::Sh(format!("docker compose -f {} pull", file_name)),
        timeout: Some(Duration::from_secs(15 * 60)),
        retry: Some(RetrySpec {
            attempts: 2,
            backoff: Some(Duration::from_secs(15)),
            on_exit_codes: vec![],
        }),
        undo: None,
    };

    let up = Step {
        name: Some("up".to_string()),
        run: CommandSpec::Sh(format!(
            "docker compose -f {} up -d --remove-orphans",
            file_name
        )),
        timeout: Some(Duration::from_secs(10 * 60)),
        retry: None,
        undo: Some(Undo {
            run: CommandSpec::Sh(format!(
                "docker compose -f {} down --remove-orphans",
                file_name
            )),
            timeout: Some(Duration::from_secs(5 * 60)),
        }),
    };

    let stop = StopSpec {
        steps: vec![Step {
            name: Some("down".to_string()),
            run: CommandSpec::Sh(format!(
                "docker compose -f {} down --remove-orphans",
                file_name
            )),
            timeout: Some(Duration::from_secs(5 * 60)),
            retry: None,
            undo: None,
        }],
    };

    let observe = ObserveSpec {
        logs: Some(LogSpec {
            follow: Some(CommandSpec::Sh(format!(
                "docker compose -f {} logs -f --timestamps -n 50",
                file_name
            ))),
        }),
        liveness: Some(ObserveHooks {
            every: Duration::from_secs(5),
            observe: CommandSpec::Sh(format!(
                r#"docker compose -f {} ps --status exited --status restarting --status dead --status paused --status removing --status created | sed '1d' | grep -q . && exit 1 || exit 0"#,
                file_name
            )),
            record: Some(CommandSpec::Sh(format!(
                "docker compose -f {} logs --timestamps --tail 200",
                file_name
            ))),
            ..Default::default()
        }),
        health: Some(ObserveHooks {
            every: Duration::from_secs(10),
            observe: CommandSpec::Sh(format!(
                r#"docker compose -f {} logs --no-color --tail=200 | grep -Ei 'error|panic|fatal|crash' && exit 1 || exit 0"#,
                file_name
            )),
            record: Some(CommandSpec::Sh(format!(
                "docker compose -f {} logs --timestamps --tail 200",
                file_name
            ))),
            fails_after: Some(3),
            ..Default::default()
        }),
    };

    Ok(ServiceSpec {
        id,
        lifecycle: Lifecycle::Running,
        workdir: Some(Workdir {
            mode: WorkdirMode::Ephemeral,
            path: None,
        }),
        files,
        env: BTreeMap::new(),
        steps: vec![pull, up],
        on_failure: Some(OnFailure {
            undo: UndoMode::ExecutedSteps,
            continue_on_failure: false,
        }),
        stop: Some(stop),
        reboot: RebootMode::None,
        observe: Some(observe),
        restart: m87_shared::deploy_spec::RestartPolicy::OnFailure,
    })
}

pub async fn get_deployment_reports(
    device_name: &str,
    deployment_id: &str,
) -> Result<Vec<DeployReport>> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let reports =
        server::get_deployment_reports(&api_url, &token, trust_invalid, &device_id, &deployment_id)
            .await
            .context("failed to fetch source deployment")?;

    Ok(reports)
}

pub async fn get_deployment_snapshot(
    device_name: &str,
    deployment_id: &str,
) -> Result<DeploymentStatusSnapshot> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let snapshot = server::get_device_revision_snapshot(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &deployment_id,
    )
    .await
    .context("failed to fetch source deployment")?;

    Ok(snapshot)
}

// ---------------------------------------------------------------------------
// get_active_revision – fetch the active DeploymentRevision for a device
// ---------------------------------------------------------------------------

pub async fn get_active_revision(device_name: &str) -> Result<DeploymentRevision> {
    let id = get_active_deployment_id(device_name)
        .await?
        .context("no active deployment on device")?;
    get_deployment(device_name, &id).await
}

// ---------------------------------------------------------------------------
// deploy_file_replace_all – atomically replace the whole device state
// ---------------------------------------------------------------------------

pub async fn deploy_file_replace_all(device_name: &str, file: PathBuf) -> Result<()> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let target_dep_id =
        resolve_target_deployment_id(&device_id, &api_url, &token, trust_invalid, None).await?;
    let target_dep_id = match target_dep_id {
        Some(id) => id,
        None => {
            let new = create_deployment(device_name, true).await?;
            new.id.unwrap()
        }
    };

    let base_dir = file.parent().map(|f| f.to_path_buf());
    let s = load_file_to_string(&file)?;
    let mut dr = DeploymentRevision::from_yaml(&s).context("failed to parse revision YAML")?;
    dr.resolve_file_references(base_dir)?;

    server::update_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &target_dep_id,
        UpdateDeployRevisionBody {
            revision: Some(dr.to_yaml()?),
            ..Default::default()
        },
    )
    .await
    .context("failed to replace deployment state")?;

    Ok(())
}

// ---------------------------------------------------------------------------
// send_lifecycle – send a runtime lifecycle update for a unit
// ---------------------------------------------------------------------------

pub async fn send_lifecycle(device_name: &str, unit_id: &str, lifecycle: Lifecycle) -> Result<()> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;
    server::send_lifecycle_update(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        unit_id,
        lifecycle,
    )
    .await
    .with_context(|| format!("failed to send lifecycle update for '{unit_id}'"))
}

// ---------------------------------------------------------------------------
// trigger_job – create a new JobRun for a job definition
// ---------------------------------------------------------------------------

pub async fn trigger_job(
    device_name: &str,
    job_id: &str,
    env_overrides: BTreeMap<String, String>,
) -> Result<JobRun> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    // We need the active revision id
    let revision_id = server::get_active_deployment_id(&api_url, &token, trust_invalid, &device_id)
        .await
        .context("failed to get active deployment id")?
        .context("no active deployment — deploy a revision first")?;

    let body = TriggerJobBody { env_overrides };

    server::trigger_job(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &revision_id,
        job_id,
        body,
    )
    .await
    .with_context(|| format!("failed to trigger job '{job_id}'"))
}

// ---------------------------------------------------------------------------
// list_job_runs / get_job_run
// ---------------------------------------------------------------------------

pub async fn list_job_runs(device_name: &str, job_id: Option<&str>) -> Result<Vec<JobRun>> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;
    server::list_job_runs(&api_url, &token, trust_invalid, &device_id, job_id)
        .await
        .context("failed to list job runs")
}

pub async fn get_job_run(device_name: &str, run_id: &str) -> Result<JobRun> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;
    server::get_job_run(&api_url, &token, trust_invalid, &device_id, run_id)
        .await
        .with_context(|| format!("failed to get job run '{run_id}'"))
}

// ---------------------------------------------------------------------------
// rollback_device
// ---------------------------------------------------------------------------

pub async fn rollback_device(device_name: &str) -> Result<()> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;
    server::rollback_device(&api_url, &token, trust_invalid, &device_id)
        .await
        .context("failed to rollback device")
}

// ---------------------------------------------------------------------------
// Step log access — fetches StepReport entries from server-side deploy_reports
// ---------------------------------------------------------------------------

/// Return all StepReport entries for the active revision, optionally filtered
/// by `unit_id` (the service/observer id or the job run_id).
pub async fn get_unit_step_logs(
    device_name: &str,
    unit_id: Option<&str>,
) -> Result<Vec<DeployReport>> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let revision_id = server::get_active_deployment_id(&api_url, &token, trust_invalid, &device_id)
        .await
        .context("failed to get active deployment id")?
        .context("no active deployment — deploy a revision first")?;

    let all =
        server::get_deployment_reports(&api_url, &token, trust_invalid, &device_id, &revision_id)
            .await
            .context("failed to fetch deployment reports")?;

    let filtered = all
        .into_iter()
        .filter(|r| {
            // Keep only step reports …
            if !matches!(&r.kind, DeployReportKind::StepReport(_)) {
                return false;
            }
            // … and optionally filter by unit / run id
            match unit_id {
                Some(id) => r.kind.get_run_id() == Some(id),
                None => true,
            }
        })
        .collect();

    Ok(filtered)
}

/// Convenience wrapper: step logs for a specific job run id.
pub async fn get_job_run_logs(device_name: &str, run_id: &str) -> Result<Vec<DeployReport>> {
    get_unit_step_logs(device_name, Some(run_id)).await
}
