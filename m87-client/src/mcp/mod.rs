//! MCP (Model Context Protocol) server for m87 CLI
//!
//! Exposes m87 platform commands as MCP tools via stdio transport.
//! This allows AI agents to programmatically call m87 operations.

use crate::{auth, device, devices, org, tui};
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

/// MCP Server for m87 platform commands
#[derive(Clone)]
pub struct M87McpServer {
    tool_router: ToolRouter<Self>,
}

fn internal_err(e: impl std::fmt::Display) -> ErrorData {
    ErrorData::internal_error(e.to_string(), None)
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
    /// Device name or ID
    device: String,
}

#[derive(Deserialize, JsonSchema)]
struct DeviceAuditLogsReq {
    /// Device name or ID
    device: String,
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
    /// Device name or ID
    device: String,
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
    /// Device name or ID
    device: String,
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
    /// Device name or ID
    device: String,
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

#[tool_router]
impl M87McpServer {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
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

    #[tool(description = "Get device status and health")]
    async fn device_status(&self, Parameters(req): Parameters<DeviceStatusReq>) -> Result<CallToolResult, ErrorData> {
        let status = devices::get_device_status(&req.device).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&status)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Get audit logs for a device")]
    async fn device_audit_logs(&self, Parameters(req): Parameters<DeviceAuditLogsReq>) -> Result<CallToolResult, ErrorData> {
        let max = req.max.unwrap_or(100);
        let logs = devices::get_audit_logs(&req.device, req.until, req.since, max).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&logs)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
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

    #[tool(description = "Execute a command on a device and return output")]
    async fn device_exec(&self, Parameters(req): Parameters<DeviceExecReq>) -> Result<CallToolResult, ErrorData> {
        let timeout_secs = req.timeout_secs.unwrap_or(30);
        let output = tui::exec::run_exec_capture(&req.device, req.command, timeout_secs).await
            .map_err(internal_err)?;
        let text = serde_json::json!({"output": output}).to_string();
        Ok(CallToolResult::success(vec![Content::text(text)]))
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

    #[tool(description = "List all deployments for a device")]
    async fn device_deployment_list(&self, Parameters(req): Parameters<DeviceDeploymentListReq>) -> Result<CallToolResult, ErrorData> {
        let revs = device::deploy::get_deployments(&req.device).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&revs)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
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

    #[tool(description = "Get deployment status")]
    async fn device_deployment_status(&self, Parameters(req): Parameters<DeviceDeploymentStatusReq>) -> Result<CallToolResult, ErrorData> {
        let id = if let Some(id) = req.deployment_id {
            id
        } else {
            device::deploy::get_active_deployment_id(&req.device).await
                .map_err(internal_err)?
                .ok_or_else(|| ErrorData::internal_error("No active deployment".to_string(), None))?
        };
        let snapshot = device::deploy::get_deployment_snapshot(&req.device, &id).await
            .map_err(internal_err)?;
        let text = serde_json::to_string(&snapshot)
            .map_err(internal_err)?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
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
                "m87 platform CLI — device management, deployments, file ops, exec, and org management".into(),
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
