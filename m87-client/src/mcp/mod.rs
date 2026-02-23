//! MCP (Model Context Protocol) server for m87 CLI
//!
//! Exposes m87 platform commands as MCP tools via stdio transport.
//! This allows AI agents to programmatically call m87 operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{auth, device, devices, org, tui};
use crate::device::forward::start_forward;
use crate::streams::stream_type::ForwardTarget;
use crate::util::shutdown::SHUTDOWN;
use dashmap::DashMap;
use rmcp::{
    ServerHandler,
    handler::server::tool::ToolRouter,
    handler::server::wrapper::Parameters,
    model::{
        CallToolResult, Content, ErrorData, Implementation, ServerCapabilities, ServerInfo,
    },
    schemars::{self, JsonSchema},
    tool, tool_handler, tool_router,
};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

struct ForwardSession {
    id: String,
    device: String,
    specs: Vec<String>,
    targets: Vec<ForwardTarget>,
    cancel: CancellationToken,
}

/// MCP Server for m87 platform commands
#[derive(Clone)]
pub struct M87McpServer {
    tool_router: ToolRouter<Self>,
    forward_sessions: Arc<DashMap<String, ForwardSession>>,
    next_session_id: Arc<AtomicU64>,
}

fn internal_err(e: impl std::fmt::Display + std::fmt::Debug) -> ErrorData {
    // Use Debug format to include the full anyhow error chain, not just the outermost context
    ErrorData::internal_error(format!("{e:?}"), None)
}

/// Strip ANSI escape sequences from output (useless for AI agents).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Skip ESC [ ... <final byte>
            if let Some(next) = chars.next() {
                if next == '[' {
                    // CSI sequence: consume until 0x40..0x7E
                    for c2 in chars.by_ref() {
                        if ('\x40'..='\x7e').contains(&c2) {
                            break;
                        }
                    }
                }
                // else: other ESC sequences — just skip the one char
            }
        } else {
            out.push(c);
        }
    }
    out
}

// ===== Batch support =====

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DeviceTarget {
    /// Single device name or ID (mutually exclusive with `devices`)
    device: Option<String>,
    /// Multiple device names/IDs for batch execution (mutually exclusive with `device`)
    devices: Option<Vec<String>>,
}

impl DeviceTarget {
    fn resolve(&self) -> Result<(Vec<String>, bool), ErrorData> {
        match (&self.device, &self.devices) {
            (Some(d), None) => Ok((vec![d.clone()], false)),
            (None, Some(ds)) => {
                if ds.is_empty() {
                    Err(ErrorData::invalid_request(
                        "devices array must not be empty",
                        None,
                    ))
                } else {
                    Ok((ds.clone(), true))
                }
            }
            (Some(_), Some(_)) => Err(ErrorData::invalid_request(
                "Provide either 'device' or 'devices', not both",
                None,
            )),
            (None, None) => Err(ErrorData::invalid_request(
                "Either 'device' or 'devices' is required",
                None,
            )),
        }
    }
}

async fn run_batch<F, Fut>(devices: Vec<String>, op: F) -> Vec<serde_json::Value>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = serde_json::Value>,
{
    let futs: Vec<_> = devices
        .into_iter()
        .map(|d| {
            let fut = op(d.clone());
            async move {
                let mut result = fut.await;
                if let Some(obj) = result.as_object_mut() {
                    obj.insert("device".into(), d.into());
                }
                result
            }
        })
        .collect();
    futures::future::join_all(futs).await
}

// ===== Parameter Structs =====

#[derive(Deserialize, JsonSchema)]
struct DeviceApproveReq {
    /// Device ID to approve
    device: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceRejectReq {
    /// Device ID to reject
    device: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceStatusReq {
    #[serde(flatten)]
    target: DeviceTarget,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceAuditLogsReq {
    #[serde(flatten)]
    target: DeviceTarget,
    /// Start time (ISO 8601)
    since: Option<String>,
    /// End time (ISO 8601)
    until: Option<String>,
    /// Maximum number of logs
    max: Option<u32>,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceAccessListReq {
    /// Device name or ID
    device: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceAccessAddReq {
    /// Device name or ID
    device: String,
    /// Email or organization ID
    email_or_org_id: String,
    /// Role (admin, editor, viewer)
    role: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceAccessRemoveReq {
    /// Device name or ID
    device: String,
    /// Email or organization ID
    email_or_org_id: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceLsReq {
    /// Remote path (device:path format)
    path: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceCpReq {
    /// Source path (local or device:path)
    source: String,
    /// Destination path (local or device:path)
    dest: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceSyncReq {
    /// Source path (local or device:path)
    source: String,
    /// Destination path (local or device:path)
    dest: String,
    /// Delete files not in source
    delete: Option<bool>,
    /// Dry run (show what would be done)
    dry_run: Option<bool>,
    /// Exclude patterns
    exclude: Option<Vec<String>>,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceExecReq {
    #[serde(flatten)]
    target: DeviceTarget,
    /// Command and arguments to execute
    command: Vec<String>,
    /// Timeout in seconds
    timeout_secs: Option<u64>,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceDeployReq {
    /// Device name or ID
    device: String,
    /// File path to deploy
    file: String,
    /// Spec type (auto, compose, runspec, deployment)
    spec_type: Option<String>,
    /// Deployment name
    name: Option<String>,
    /// Deployment ID
    deployment_id: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceUndeployReq {
    /// Device name or ID
    device: String,
    /// Job ID
    job_id: String,
    /// Deployment ID
    deployment_id: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceDeploymentListReq {
    #[serde(flatten)]
    target: DeviceTarget,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceDeploymentNewReq {
    /// Device name or ID
    device: String,
    /// Make this deployment active
    active: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceDeploymentShowReq {
    /// Device name or ID
    device: String,
    /// Deployment ID (uses active if not specified)
    deployment_id: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceDeploymentRmReq {
    /// Device name or ID
    device: String,
    /// Deployment ID
    deployment_id: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceDeploymentActiveReq {
    /// Device name or ID
    device: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceDeploymentActivateReq {
    /// Device name or ID
    device: String,
    /// Deployment ID
    deployment_id: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceDeploymentStatusReq {
    #[serde(flatten)]
    target: DeviceTarget,
    /// Deployment ID (uses active if not specified)
    deployment_id: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceDeploymentCloneReq {
    /// Device name or ID
    device: String,
    /// Source deployment ID
    deployment_id: String,
    /// Make cloned deployment active
    active: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
struct OrgListReq {}

#[derive(Deserialize, JsonSchema)]
struct OrgCreateReq {
    /// Organization ID
    id: String,
    /// Owner email
    email: String,
}

#[derive(Deserialize, JsonSchema)]
struct OrgDeleteReq {
    /// Organization ID
    id: String,
}

#[derive(Deserialize, JsonSchema)]
struct OrgUpdateReq {
    /// Organization ID
    id: String,
    /// New organization ID
    new_id: String,
}

#[derive(Deserialize, JsonSchema)]
struct OrgMembersListReq {
    /// Organization ID
    org_id: String,
}

#[derive(Deserialize, JsonSchema)]
struct OrgMembersAddReq {
    /// Organization ID
    org_id: String,
    /// Member email
    email: String,
    /// Role (admin, editor, viewer)
    role: String,
}

#[derive(Deserialize, JsonSchema)]
struct OrgMembersRemoveReq {
    /// Organization ID
    org_id: String,
    /// Member email
    email: String,
}

#[derive(Deserialize, JsonSchema)]
struct OrgDevicesListReq {
    /// Organization ID
    org_id: String,
}

#[derive(Deserialize, JsonSchema)]
struct OrgDevicesAddReq {
    /// Organization ID
    org_id: String,
    /// Device name or ID
    device: String,
}

#[derive(Deserialize, JsonSchema)]
struct OrgDevicesRemoveReq {
    /// Organization ID
    org_id: String,
    /// Device name or ID
    device: String,
}

#[derive(Deserialize, JsonSchema)]
struct ForwardStartReq {
    /// Device name or ID
    device: String,
    /// Forward specs (e.g. ["8080:80", "/tmp/sock:/var/run/docker.sock"])
    specs: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ForwardStopReq {
    /// Session ID returned by forward_start
    session_id: String,
}

#[derive(Deserialize, JsonSchema)]
struct ForwardListReq {}

#[derive(Deserialize, JsonSchema)]
struct DockerExecReq {
    /// Device name or ID
    device: String,
    /// Docker CLI arguments (e.g. ["ps", "-a"] or ["run", "-d", "nginx"])
    args: Vec<String>,
    /// Timeout in seconds (default 60). Use a higher value for builds/pulls.
    timeout_secs: Option<u64>,
}

#[tool_router]
impl M87McpServer {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
            forward_sessions: Arc::new(DashMap::new()),
            next_session_id: Arc::new(AtomicU64::new(1)),
        }
    }

    // Device management

    #[tool(description = "List all accessible devices")]
    async fn devices_list(&self) -> Result<CallToolResult, ErrorData> {
        let devices = devices::list_devices().await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&devices)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Approve a pending device registration")]
    async fn devices_approve(&self, Parameters(req): Parameters<DeviceApproveReq>) -> Result<CallToolResult, ErrorData> {
        auth::accept_auth_request(&req.device).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "approved"}).to_string())]))
    }

    #[tool(description = "Reject a pending device registration")]
    async fn devices_reject(&self, Parameters(req): Parameters<DeviceRejectReq>) -> Result<CallToolResult, ErrorData> {
        auth::reject_auth_request(&req.device).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "rejected"}).to_string())]))
    }

    // Device commands

    #[tool(description = "Get device status and health. Supports batch: pass 'devices' array instead of 'device' to query multiple devices at once.")]
    async fn device_status(&self, Parameters(req): Parameters<DeviceStatusReq>) -> Result<CallToolResult, ErrorData> {
        let (devices, is_batch) = req.target.resolve()?;

        let results = run_batch(devices, |device| async move {
            match devices::get_device_status(&device).await {
                Ok(status) => match serde_json::to_value(&status) {
                    Ok(v) => v,
                    Err(e) => serde_json::json!({ "error": format!("{e:?}") }),
                },
                Err(e) => serde_json::json!({ "error": format!("{e:?}") }),
            }
        }).await;

        if is_batch {
            let text = serde_json::json!({ "results": results }).to_string();
            Ok(CallToolResult::success(vec![Content::text(text)]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(results[0].to_string())]))
        }
    }

    #[tool(description = "Get audit logs for a device. Supports batch: pass 'devices' array instead of 'device' to query multiple devices at once.")]
    async fn device_audit_logs(&self, Parameters(req): Parameters<DeviceAuditLogsReq>) -> Result<CallToolResult, ErrorData> {
        let (devices, is_batch) = req.target.resolve()?;
        let max = req.max.unwrap_or(100);
        let until = req.until;
        let since = req.since;

        let results = run_batch(devices, |device| {
            let until = until.clone();
            let since = since.clone();
            async move {
                match devices::get_audit_logs(&device, until, since, max).await {
                    Ok(logs) => match serde_json::to_value(&logs) {
                        Ok(v) => serde_json::json!({ "logs": v }),
                        Err(e) => serde_json::json!({ "error": format!("{e:?}") }),
                    },
                    Err(e) => serde_json::json!({ "error": format!("{e:?}") }),
                }
            }
        }).await;

        if is_batch {
            let text = serde_json::json!({ "results": results }).to_string();
            Ok(CallToolResult::success(vec![Content::text(text)]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(results[0].to_string())]))
        }
    }

    #[tool(description = "List users with access to a device")]
    async fn device_access_list(&self, Parameters(req): Parameters<DeviceAccessListReq>) -> Result<CallToolResult, ErrorData> {
        let users = devices::get_device_users(&req.device).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&users)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Grant access to a device")]
    async fn device_access_add(&self, Parameters(req): Parameters<DeviceAccessAddReq>) -> Result<CallToolResult, ErrorData> {
        let role = m87_shared::roles::Role::from_str(&req.role)
            .map_err(|e| ErrorData::invalid_request(format!("Invalid role: {}", e), None))?;
        devices::add_access(&req.device, &req.email_or_org_id, role).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "added"}).to_string())]))
    }

    #[tool(description = "Revoke access to a device")]
    async fn device_access_remove(&self, Parameters(req): Parameters<DeviceAccessRemoveReq>) -> Result<CallToolResult, ErrorData> {
        devices::remove_access(&req.device, &req.email_or_org_id).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "removed"}).to_string())]))
    }

    // File operations

    #[tool(description = "List files in a device directory")]
    async fn device_ls(&self, Parameters(req): Parameters<DeviceLsReq>) -> Result<CallToolResult, ErrorData> {
        let resp = device::fs::list(&req.path).await
            .map_err(internal_err)?;
        let entries: Vec<_> = resp.iter().map(|e| serde_json::json!({"name": e.file_name(), "is_dir": e.metadata().is_dir()})).collect();
        let text = serde_json::to_string(&entries)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Copy files between local and remote device")]
    async fn device_cp(&self, Parameters(req): Parameters<DeviceCpReq>) -> Result<CallToolResult, ErrorData> {
        device::fs::copy(&req.source, &req.dest).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "copied"}).to_string())]))
    }

    #[tool(description = "Sync files between local and remote device")]
    async fn device_sync(&self, Parameters(req): Parameters<DeviceSyncReq>) -> Result<CallToolResult, ErrorData> {
        let delete = req.delete.unwrap_or(false);
        let dry_run = req.dry_run.unwrap_or(false);
        let exclude = req.exclude.unwrap_or_default();
        device::fs::sync(&req.source, &req.dest, delete, dry_run, &exclude).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "synced"}).to_string())]))
    }

    // Exec

    #[tool(description = "Execute a command on a device and return output. Non-zero exit codes are returned as data, not errors. Supports batch: pass 'devices' array instead of 'device' to execute on multiple devices at once.")]
    async fn device_exec(&self, Parameters(req): Parameters<DeviceExecReq>) -> Result<CallToolResult, ErrorData> {
        let (devices, is_batch) = req.target.resolve()?;
        let timeout_secs = req.timeout_secs.unwrap_or(30);
        let command = req.command;

        let results = run_batch(devices, |device| {
            let cmd = command.clone();
            async move {
                match tui::exec::run_exec_capture(&device, cmd, timeout_secs).await {
                    Ok(capture) => serde_json::json!({
                        "output": strip_ansi(&capture.output),
                        "exit_code": capture.exit_code,
                    }),
                    Err(e) => serde_json::json!({ "error": format!("{e:?}") }),
                }
            }
        }).await;

        if is_batch {
            let text = serde_json::json!({ "results": results }).to_string();
            Ok(CallToolResult::success(vec![Content::text(text)]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(results[0].to_string())]))
        }
    }

    // Deployment operations

    #[tool(description = "Add a deployment spec to a device")]
    async fn device_deploy(&self, Parameters(req): Parameters<DeviceDeployReq>) -> Result<CallToolResult, ErrorData> {
        let spec_type_str = req.spec_type.as_deref().unwrap_or("auto");
        let spec_type = match spec_type_str {
            "compose" => device::deploy::SpecType::Compose,
            "runspec" => device::deploy::SpecType::Runspec,
            "deployment" => device::deploy::SpecType::Deployment,
            _ => device::deploy::SpecType::Auto,
        };
        device::deploy::deploy_file(&req.device, std::path::PathBuf::from(&req.file), spec_type, req.name, req.deployment_id).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "deployed"}).to_string())]))
    }

    #[tool(description = "Remove a deployment spec from a device")]
    async fn device_undeploy(&self, Parameters(req): Parameters<DeviceUndeployReq>) -> Result<CallToolResult, ErrorData> {
        device::deploy::undeploy_file(&req.device, req.job_id, req.deployment_id).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "undeployed"}).to_string())]))
    }

    #[tool(description = "List all deployments for a device. Supports batch: pass 'devices' array instead of 'device' to query multiple devices at once.")]
    async fn device_deployment_list(&self, Parameters(req): Parameters<DeviceDeploymentListReq>) -> Result<CallToolResult, ErrorData> {
        let (devices, is_batch) = req.target.resolve()?;

        let results = run_batch(devices, |device| async move {
            match device::deploy::get_deployments(&device).await {
                Ok(revs) => match serde_json::to_value(&revs) {
                    Ok(v) => serde_json::json!({ "deployments": v }),
                    Err(e) => serde_json::json!({ "error": format!("{e:?}") }),
                },
                Err(e) => serde_json::json!({ "error": format!("{e:?}") }),
            }
        }).await;

        if is_batch {
            let text = serde_json::json!({ "results": results }).to_string();
            Ok(CallToolResult::success(vec![Content::text(text)]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(results[0].to_string())]))
        }
    }

    #[tool(description = "Create a new deployment for a device")]
    async fn device_deployment_new(&self, Parameters(req): Parameters<DeviceDeploymentNewReq>) -> Result<CallToolResult, ErrorData> {
        let active = req.active.unwrap_or(false);
        let rev = device::deploy::create_deployment(&req.device, active).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&rev)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Show deployment details")]
    async fn device_deployment_show(&self, Parameters(req): Parameters<DeviceDeploymentShowReq>) -> Result<CallToolResult, ErrorData> {
        let deployment_id = match req.deployment_id {
            Some(id) => id,
            None => device::deploy::get_active_deployment_id(&req.device).await
                .map_err(internal_err)?
                .ok_or_else(|| ErrorData::internal_error("No active deployment".to_string(), None))?,
        };
        let rev = device::deploy::get_deployment(&req.device, &deployment_id).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&rev)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Remove a deployment")]
    async fn device_deployment_rm(&self, Parameters(req): Parameters<DeviceDeploymentRmReq>) -> Result<CallToolResult, ErrorData> {
        device::deploy::remove_deployment(&req.device, req.deployment_id).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "removed"}).to_string())]))
    }

    #[tool(description = "Get the currently active deployment")]
    async fn device_deployment_active(&self, Parameters(req): Parameters<DeviceDeploymentActiveReq>) -> Result<CallToolResult, ErrorData> {
        let active_id = device::deploy::get_active_deployment_id(&req.device).await
            .map_err(internal_err)?;
        let text = serde_json::json!({"active_deployment_id": active_id}).to_string();
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Set the active deployment")]
    async fn device_deployment_activate(&self, Parameters(req): Parameters<DeviceDeploymentActivateReq>) -> Result<CallToolResult, ErrorData> {
        device::deploy::deployment_active_set(&req.device, req.deployment_id).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "activated"}).to_string())]))
    }

    #[tool(description = "Get deployment status. Supports batch: pass 'devices' array instead of 'device' to query multiple devices at once.")]
    async fn device_deployment_status(&self, Parameters(req): Parameters<DeviceDeploymentStatusReq>) -> Result<CallToolResult, ErrorData> {
        let (devices, is_batch) = req.target.resolve()?;
        let deployment_id = req.deployment_id;

        let results = run_batch(devices, |device| {
            let dep_id = deployment_id.clone();
            async move {
                let id = match dep_id {
                    Some(id) => id,
                    None => match device::deploy::get_active_deployment_id(&device).await {
                        Ok(Some(id)) => id,
                        Ok(None) => return serde_json::json!({ "error": "No active deployment" }),
                        Err(e) => return serde_json::json!({ "error": format!("{e:?}") }),
                    },
                };
                match device::deploy::get_deployment_snapshot(&device, &id).await {
                    Ok(snapshot) => match serde_json::to_value(&snapshot) {
                        Ok(v) => v,
                        Err(e) => serde_json::json!({ "error": format!("{e:?}") }),
                    },
                    Err(e) => serde_json::json!({ "error": format!("{e:?}") }),
                }
            }
        }).await;

        if is_batch {
            let text = serde_json::json!({ "results": results }).to_string();
            Ok(CallToolResult::success(vec![Content::text(text)]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(results[0].to_string())]))
        }
    }

    #[tool(description = "Clone a deployment")]
    async fn device_deployment_clone(&self, Parameters(req): Parameters<DeviceDeploymentCloneReq>) -> Result<CallToolResult, ErrorData> {
        let active = req.active.unwrap_or(false);
        let rev = device::deploy::clone_deployment(&req.device, req.deployment_id, active).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&rev)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    // Organization commands

    #[tool(description = "List organizations")]
    async fn org_list(&self, Parameters(_req): Parameters<OrgListReq>) -> Result<CallToolResult, ErrorData> {
        let orgs = org::list_organizations().await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&orgs)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Create an organization")]
    async fn org_create(&self, Parameters(req): Parameters<OrgCreateReq>) -> Result<CallToolResult, ErrorData> {
        org::create_organization(&req.id, &req.email).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "created"}).to_string())]))
    }

    #[tool(description = "Delete an organization")]
    async fn org_delete(&self, Parameters(req): Parameters<OrgDeleteReq>) -> Result<CallToolResult, ErrorData> {
        org::delete_organization(&req.id).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "deleted"}).to_string())]))
    }

    #[tool(description = "Update organization")]
    async fn org_update(&self, Parameters(req): Parameters<OrgUpdateReq>) -> Result<CallToolResult, ErrorData> {
        org::update_organization(&req.id, &req.new_id).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "updated"}).to_string())]))
    }

    #[tool(description = "List organization members")]
    async fn org_members_list(&self, Parameters(req): Parameters<OrgMembersListReq>) -> Result<CallToolResult, ErrorData> {
        let members = org::list_members(Some(req.org_id)).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&members)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Add organization member")]
    async fn org_members_add(&self, Parameters(req): Parameters<OrgMembersAddReq>) -> Result<CallToolResult, ErrorData> {
        let role = m87_shared::roles::Role::from_str(&req.role)
            .map_err(|e| ErrorData::invalid_request(format!("Invalid role: {}", e), None))?;
        org::add_member(Some(req.org_id), &req.email, role).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "added"}).to_string())]))
    }

    #[tool(description = "Remove organization member")]
    async fn org_members_remove(&self, Parameters(req): Parameters<OrgMembersRemoveReq>) -> Result<CallToolResult, ErrorData> {
        org::remove_member(Some(req.org_id), &req.email).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "removed"}).to_string())]))
    }

    #[tool(description = "List organization devices")]
    async fn org_devices_list(&self, Parameters(req): Parameters<OrgDevicesListReq>) -> Result<CallToolResult, ErrorData> {
        let devices = org::list_devices(Some(req.org_id)).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&devices)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Add device to organization")]
    async fn org_devices_add(&self, Parameters(req): Parameters<OrgDevicesAddReq>) -> Result<CallToolResult, ErrorData> {
        org::add_device(Some(req.org_id), &req.device).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "added"}).to_string())]))
    }

    #[tool(description = "Remove device from organization")]
    async fn org_devices_remove(&self, Parameters(req): Parameters<OrgDevicesRemoveReq>) -> Result<CallToolResult, ErrorData> {
        org::remove_device(Some(req.org_id), &req.device).await
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(serde_json::json!({"status": "removed"}).to_string())]))
    }

    // Forward management

    #[tool(description = "Start port/socket forwarding to a device. Returns a session ID for lifecycle management. Forwarding runs in the background until stopped.")]
    async fn forward_start(&self, Parameters(req): Parameters<ForwardStartReq>) -> Result<CallToolResult, ErrorData> {
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed).to_string();
        let cancel = SHUTDOWN.child_token();

        let targets = start_forward(&req.device, req.specs.clone(), cancel.clone())
            .await
            .map_err(internal_err)?;

        let target_descriptions: Vec<String> = targets.iter().map(|t| format!("{:?}", t)).collect();

        self.forward_sessions.insert(id.clone(), ForwardSession {
            id: id.clone(),
            device: req.device.clone(),
            specs: req.specs,
            targets: targets.clone(),
            cancel,
        });

        let text = serde_json::json!({
            "session_id": id,
            "device": req.device,
            "targets": target_descriptions,
            "status": "started"
        }).to_string();
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Stop a running forward session by session ID")]
    async fn forward_stop(&self, Parameters(req): Parameters<ForwardStopReq>) -> Result<CallToolResult, ErrorData> {
        let session = self.forward_sessions.remove(&req.session_id)
            .map(|(_, s)| s)
            .ok_or_else(|| ErrorData::invalid_request(format!("No session with id '{}'", req.session_id), None))?;

        session.cancel.cancel();

        let text = serde_json::json!({
            "session_id": req.session_id,
            "status": "stopped"
        }).to_string();
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "List all active forward sessions")]
    async fn forward_list(&self, Parameters(_req): Parameters<ForwardListReq>) -> Result<CallToolResult, ErrorData> {
        let sessions: Vec<serde_json::Value> = self.forward_sessions.iter()
            .map(|entry| {
                let s = entry.value();
                serde_json::json!({
                    "session_id": s.id,
                    "device": s.device,
                    "specs": s.specs,
                    "targets": s.targets.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>(),
                })
            })
            .collect();

        let text = serde_json::to_string(&sessions).map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    // Docker

    #[tool(description = "Run a docker command on a device and capture output. The docker socket is forwarded via QUIC automatically. For long-running containers use '-d' flag — the output will include the container ID for lifecycle management. Non-zero exit codes are returned as data, not errors.")]
    async fn docker_exec(&self, Parameters(req): Parameters<DockerExecReq>) -> Result<CallToolResult, ErrorData> {
        let timeout = req.timeout_secs.unwrap_or(60);

        let output = device::docker::run_docker_capture(&req.device, req.args, timeout)
            .await
            .map_err(internal_err)?;

        let text = serde_json::json!({
            "stdout": output.stdout,
            "stderr": output.stderr,
            "exit_code": output.exit_code,
        }).to_string();
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }
}

#[tool_handler]
impl ServerHandler for M87McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            server_info: Implementation {
                name: "m87-mcp".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                ..Default::default()
            },
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            instructions: Some(
                "m87 platform CLI — device management, deployments, file ops, exec, port forwarding, docker, and org management. Use forward_start/forward_stop to manage persistent port forwards. Use docker_exec to run docker commands on devices (use -d for long-running containers).".into(),
            ),
            ..Default::default()
        }
    }
}

/// Start the MCP server on stdio transport
pub async fn run_mcp_server() -> anyhow::Result<()> {
    let server = M87McpServer::new();
    let transport = rmcp::transport::io::stdio();
    let service = rmcp::serve_server(server, transport).await?;
    service.waiting().await?;
    Ok(())
}
