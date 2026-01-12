use anyhow::{Context, Result, bail};
use m87_shared::deploy_spec::{
    CommandSpec, CreateDeployRevisionBody, DeploymentRevision, HealthSpec, LogLimit, LogSpec,
    ObserveSpec, OnFailure, RebootMode, RetrySpec, RunSpec, RunType, Step, StopSpec, Undo,
    UndoMode, UpdateDeployRevisionBody, Workdir, WorkdirMode,
};
use serde_yaml::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::auth::AuthManager;
use crate::config::Config;
use crate::devices::resolve_device_short_id_cached;
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
    let resolved = resolve_device_short_id_cached(device_name).await?;
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

    // Convert input -> run-spec YAML string (typed for runspec)
    let update_body = match ty {
        SpecType::Compose => {
            let run_spec = compose_file_to_runspec_yaml(&file, name.as_deref())
                .await?
                .to_yaml()?;
            UpdateDeployRevisionBody {
                add_run_spec: Some(run_spec),
                ..Default::default()
            }
        }
        SpecType::Runspec => {
            let s = load_file_to_string(&file)?;
            let _ = RunSpec::from_yaml(&s)?;
            UpdateDeployRevisionBody {
                add_run_spec: Some(s),
                ..Default::default()
            }
        }
        SpecType::Auto => {
            let s = load_file_to_string(&file)?;
            if is_docker_compose_yaml(&s) {
                let run_spec = compose_file_to_runspec_yaml(&file, name.as_deref())
                    .await?
                    .to_yaml()?;
                UpdateDeployRevisionBody {
                    add_run_spec: Some(run_spec),
                    ..Default::default()
                }
            } else {
                match RunSpec::from_yaml(&s) {
                    Ok(_) => UpdateDeployRevisionBody {
                        add_run_spec: Some(s),
                        ..Default::default()
                    },
                    Err(_) => {
                        let deployment = DeploymentRevision::from_yaml(&s);
                        if let Ok(_) = deployment {
                            UpdateDeployRevisionBody {
                                revision: Some(s),
                                ..Default::default()
                            }
                        } else {
                            bail!("Failed to parse deployment YAML");
                        }
                    }
                }
            }
        }
        SpecType::Deployment => {
            let s = load_file_to_string(&file)?;
            let _ = DeploymentRevision::from_yaml(&s)?;
            UpdateDeployRevisionBody {
                revision: Some(s),
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

    // Convert input -> run-spec YAML string (typed for runspec)
    let job_id = match ty {
        SpecType::Compose => {
            let run_spec = compose_file_to_runspec_yaml(&file, name.as_deref()).await?;
            run_spec.get_hash()
        }
        SpecType::Runspec => {
            let s = load_file_to_string(&file)?;
            RunSpec::from_yaml(&s)?.get_hash()
        }
        SpecType::Auto => {
            let s = load_file_to_string(&file)?;
            if is_docker_compose_yaml(&s) {
                let run_spec = compose_file_to_runspec_yaml(&file, name.as_deref()).await?;
                run_spec.get_hash()
            } else {
                match RunSpec::from_yaml(&s) {
                    Ok(r) => r.get_hash(),
                    Err(_) => {
                        let deployment = DeploymentRevision::from_yaml(&s);
                        if let Ok(_) = deployment {
                            bail!(
                                "Undeploy is only supporting undeployment of Jobs not entire deployments"
                            );
                        } else {
                            bail!("Failed to parse deployment YAML");
                        }
                    }
                }
            }
        }
        SpecType::Deployment => {
            bail!("Undeploy is only supporting undeployment of Jobs not entire deployments");
        }
    };

    deployment_update(
        device_name,
        DeploymentUpdateArgs {
            deployment_id: target_dep_id,
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

pub async fn get_active_deployment(device_name: &str) -> Result<Option<String>> {
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

pub async fn get_deployment(
    device_name: &str,
    deployment_id: String,
) -> Result<DeploymentRevision> {
    let (device_id, api_url, token, trust_invalid) = ctx_for_device(device_name).await?;

    let deployment =
        server::get_deployment(&api_url, &token, trust_invalid, &device_id, &deployment_id)
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
    pub deployment_id: String,

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
            deployment_id: String::new(),
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

    let needs_full_revision_patch =
        !args.rename.is_empty() || !args.enable.is_empty() || !args.disable.is_empty();

    if !needs_full_revision_patch {
        for id in &args.rm {
            server::update_deployment(
                &api_url,
                &token,
                trust_invalid,
                &device_id,
                &args.deployment_id,
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

            let spec = file_to_run_spec(&path, args.r#type).await?;
            let yml = spec.to_yaml()?;

            server::update_deployment(
                &api_url,
                &token,
                trust_invalid,
                &device_id,
                &args.deployment_id,
                UpdateDeployRevisionBody {
                    update_run_spec: Some(yml),
                    ..Default::default()
                },
            )
            .await
            .with_context(|| format!("failed to update run spec {spec_id}"))?;
        }

        let revision = server::get_deployment(
            &api_url,
            &token,
            trust_invalid,
            &device_id,
            &args.deployment_id,
        )
        .await
        .context("failed to fetch updated deployment")?;
        return Ok(revision);
    }

    let mut dep = server::get_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &args.deployment_id,
    )
    .await
    .context("failed to fetch deployment")?;

    for id in &args.rm {
        dep.jobs.retain(|s| s.id != *id);
    }

    for rep in &args.replace {
        let (spec_id, path) = parse_kv_eq(rep)?;
        let path = PathBuf::from(path);

        let mut rs = file_to_run_spec(&path, args.r#type).await?;
        rs.id = spec_id.clone();

        let idx = dep
            .jobs
            .iter()
            .position(|s| s.id == spec_id)
            .with_context(|| format!("spec_id not found for --replace: {spec_id}"))?;
        dep.jobs[idx] = rs;
    }

    for r in &args.rename {
        let (spec_id, new_name) = parse_kv_eq(r)?;
        let rs = dep
            .jobs
            .iter_mut()
            .find(|s| s.id == spec_id)
            .with_context(|| format!("spec_id not found for --rename: {spec_id}"))?;
        rs.id = new_name;
    }

    for id in &args.enable {
        let rs = dep
            .jobs
            .iter_mut()
            .find(|s| s.id == *id)
            .with_context(|| format!("spec_id not found for --enable: {id}"))?;
        rs.enabled = true;
    }
    for id in &args.disable {
        let rs = dep
            .jobs
            .iter_mut()
            .find(|s| s.id == *id)
            .with_context(|| format!("spec_id not found for --disable: {id}"))?;
        rs.enabled = false;
    }

    server::update_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &args.deployment_id,
        UpdateDeployRevisionBody {
            revision: Some(dep.to_yaml()?),
            ..Default::default()
        },
    )
    .await
    .context("failed to update deployment")?;

    server::get_deployment(
        &api_url,
        &token,
        trust_invalid,
        &device_id,
        &args.deployment_id,
    )
    .await
    .context("failed to fetch updated deployment")
}

fn parse_kv_eq(s: &str) -> Result<(String, String)> {
    let (k, v) = s.split_once('=').context("expected format <id>=<value>")?;
    Ok((k.to_string(), v.to_string()))
}

async fn file_to_run_spec(path: &Path, spec_type: SpecType) -> Result<RunSpec> {
    let res = match spec_type {
        SpecType::Compose => compose_file_to_runspec_yaml(path, None).await?,
        SpecType::Runspec => {
            let s = load_file_to_string(path)?;
            RunSpec::from_yaml(&s)?
        }
        SpecType::Auto => {
            let s = load_file_to_string(path)?;
            if is_docker_compose_yaml(&s) {
                compose_file_to_runspec_yaml(path, None).await?
            } else {
                match RunSpec::from_yaml(&s) {
                    Ok(s) => s,
                    Err(_) => {
                        let deployment = DeploymentRevision::from_yaml(&s);
                        if let Ok(_) = deployment {
                            bail!("--type deployment is not valid for --replace")
                        } else {
                            bail!("Failed to parse deployment YAML");
                        }
                    }
                }
            }
        }
        SpecType::Deployment => bail!("--type deployment is not valid for --replace"),
    };
    Ok(res)
}

pub async fn compose_file_to_runspec_yaml(file: &Path, name: Option<&str>) -> Result<RunSpec> {
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
        bail!("cannot derive RunSpec id: pass _name or use a compose file with a valid name");
    };

    let mut files = BTreeMap::new();
    files.insert(file_name.clone(), compose);

    let pull = Step {
        name: Some("pull".to_string()),
        run: CommandSpec::Argv(vec![
            "docker".into(),
            "compose".into(),
            "-f".into(),
            file_name.clone(),
            "pull".into(),
        ]),
        timeout: Some(Duration::from_secs(15 * 60)),
        retry: Some(RetrySpec {
            attempts: 2,
            backoff: Duration::from_secs(15),
            on_exit_codes: None,
        }),
        undo: None,
    };

    let up = Step {
        name: Some("up".to_string()),
        run: CommandSpec::Argv(vec![
            "docker".into(),
            "compose".into(),
            "-f".into(),
            file_name.clone(),
            "up".into(),
            "-d".into(),
            "--remove-orphans".into(),
        ]),
        timeout: Some(Duration::from_secs(10 * 60)),
        retry: None,
        undo: Some(Undo {
            run: CommandSpec::Argv(vec![
                "docker".into(),
                "compose".into(),
                "-f".into(),
                file_name.clone(),
                "down".into(),
                "--remove-orphans".into(),
            ]),
            timeout: Some(Duration::from_secs(5 * 60)),
        }),
    };

    let stop = StopSpec {
        steps: vec![Step {
            name: Some("down".to_string()),
            run: CommandSpec::Argv(vec![
                "docker".into(),
                "compose".into(),
                "-f".into(),
                file_name.clone(),
                "down".into(),
                "--remove-orphans".into(),
            ]),
            timeout: Some(Duration::from_secs(5 * 60)),
            retry: None,
            undo: None,
        }],
    };

    let observe = ObserveSpec {
        logs: Some(LogSpec {
            tail: CommandSpec::Argv(vec![
                "docker".into(),
                "compose".into(),
                "-f".into(),
                file_name.clone(),
                "logs".into(),
                "--timestamps".into(),
                "--no-color".into(),
                "--tail".into(),
                "200".into(),
            ]),
            follow: Some(CommandSpec::Argv(vec![
                "docker".into(),
                "compose".into(),
                "-f".into(),
                file_name.clone(),
                "logs".into(),
                "-f".into(),
                "--timestamps".into(),
                "--no-color".into(),
            ])),
            since: Some(Duration::from_secs(30 * 60)),
            limits: Some(LogLimit::default()),
        }),
        liveness: None,
        health: Some(HealthSpec {
            every: Duration::from_secs(30),
            run: CommandSpec::Sh(format!(
                r#" docker compose -f "{file}" ps \
              --status exited \
              --status restarting \
              --status dead \
              --status paused \
              --status removing \
              --status created \
              >/dev/null 2>&1 || exit 0
            exit 1"#,
                file = file_name
            )),
            fails_after: 3,
        }),
    };

    Ok(RunSpec::new(
        id,
        RunType::Service,
        true,
        Some(Workdir {
            mode: WorkdirMode::Ephemeral,
            path: None,
        }),
        files,
        BTreeMap::new(),
        vec![pull, up],
        Some(OnFailure {
            undo: UndoMode::ExecutedSteps,
            continue_on_failure: false,
        }),
        Some(stop),
        RebootMode::None,
        Some(observe),
    ))
}
