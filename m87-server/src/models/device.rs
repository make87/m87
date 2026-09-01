use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use m87_shared::deploy_spec::{
    DeployReportKind, DeploymentRevision, LifecycleUpdate, build_instruction_hash,
};
use m87_shared::device::DeviceStatus;
use m87_shared::roles::Role;
use m87_shared::users::User;
use mongodb::bson::{DateTime, Document, doc, oid::ObjectId, to_bson};

use serde::{Deserialize, Serialize};

// Import shared types
pub use m87_shared::config::DeviceClientConfig;
pub use m87_shared::device::{DeviceSystemInfo, PublicDevice, short_device_id};
pub use m87_shared::heartbeat::{HeartbeatRequest, HeartbeatResponse};
use tokio_stream::StreamExt;

use crate::config::AppConfig;
use crate::models::audit_logs::AuditLogDoc;
use crate::models::deploy_spec::{
    CreateDeployReportBody, DeployReportDoc, DeployRevisionDoc, JobRunDoc,
};
use crate::models::org;
use crate::models::roles::{CreateRoleBinding, RoleDoc};
use crate::models::user::UserDoc;
use crate::{
    auth::{access_control::AccessControlled, claims::Claims},
    db::Mongo,
    response::{ServerError, ServerResult},
};

fn default_stable_version() -> String {
    "latest".to_string()
}

/// MongoDB duplicate-key error code (E11000). A unique-index violation on
/// insert surfaces here, which is how a colliding `short_id` (or `_id`) is
/// rejected race-safely at enrollment.
const MONGO_DUPLICATE_KEY_CODE: i32 = 11000;

/// True if `err` is a MongoDB duplicate-key (E11000) error. `insert_one`
/// funnels write failures into `ErrorKind::Write(WriteError)`, but we also
/// match the bulk shape defensively in case a future path reports it that way.
fn is_duplicate_key_error(err: &mongodb::error::Error) -> bool {
    use mongodb::error::{ErrorKind, WriteFailure};
    match err.kind.as_ref() {
        ErrorKind::Write(WriteFailure::WriteError(we)) => we.code == MONGO_DUPLICATE_KEY_CODE,
        ErrorKind::InsertMany(ime) => ime
            .write_errors
            .as_ref()
            .map(|errs| errs.iter().any(|e| e.code == MONGO_DUPLICATE_KEY_CODE))
            .unwrap_or(false),
        _ => false,
    }
}

#[derive(Deserialize, Serialize, Default)]
pub struct UpdateDeviceBody {
    pub system_info: Option<DeviceSystemInfo>,
    pub version: Option<String>,
    pub target_version: Option<String>,
    #[serde(default)]
    pub config: Option<DeviceClientConfig>,
    #[serde(default)]
    pub owner_scope: Option<String>,
    #[serde(default)]
    pub allowed_scopes: Option<Vec<String>>,
}

impl Display for UpdateDeviceBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let json = serde_json::to_string_pretty(self).unwrap();
        write!(f, "{}", json)
    }
}

impl UpdateDeviceBody {
    /// True if this update would change access-control fields — device
    /// ownership (`owner_scope`) or the shared-access ACL (`allowed_scopes`).
    ///
    /// These are privileged: `POST /device/{id}` is otherwise reachable with
    /// Editor, but rewriting who can reach a device must require Admin (the
    /// same bar as `add_device_access`). Without this gate an Editor could
    /// seize ownership or silently grant the device to arbitrary scopes.
    pub fn touches_access_control(&self) -> bool {
        self.owner_scope.is_some() || self.allowed_scopes.is_some()
    }

    pub fn to_update_doc(&self) -> Document {
        let mut update_fields = doc! {};

        if let Some(system_info) = &self.system_info {
            update_fields.insert("system_info", mongodb::bson::to_bson(system_info).unwrap());
        }

        if let Some(owner_scope) = &self.owner_scope {
            update_fields.insert("owner_scope", owner_scope);
        }

        if let Some(allowed_scopes) = &self.allowed_scopes {
            update_fields.insert("allowed_scopes", allowed_scopes);
        }

        if let Some(version) = &self.version {
            update_fields.insert("version", version);
        }

        if let Some(target_version) = &self.target_version {
            update_fields.insert("target_version", target_version);
        }

        if let Some(config) = &self.config {
            update_fields.insert("config", mongodb::bson::to_bson(config).unwrap());

            let new_hash = config.get_hash().to_string();
            update_fields.insert("last_instruction_hash", new_hash);
        }

        // Always set these system timestamps
        update_fields.insert("updated_at", DateTime::now());

        doc! { "$set": update_fields }
    }
}

#[derive(Deserialize, Serialize, Default)]
pub struct CreateDeviceBody {
    pub id: Option<String>,
    pub name: String,
    pub target_version: Option<String>,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
    pub api_key_id: ObjectId,
    pub system_info: DeviceSystemInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub short_id: String,
    pub name: String,
    pub updated_at: DateTime,
    pub created_at: DateTime,
    #[serde(default = "String::new")]
    pub version: String,
    #[serde(default = "default_stable_version")]
    pub target_version: String,
    #[serde(default)]
    pub config: DeviceClientConfig,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
    pub system_info: DeviceSystemInfo,
    pub api_key_id: ObjectId,
    #[serde(default)]
    pub last_config_hash: String,
    #[serde(default)]
    pub last_deployment_hash: String,
    #[serde(default)]
    pub pending_lifecycle_updates: Vec<LifecycleUpdate>,
}

impl DeviceDoc {
    pub fn scope_for_device(device_id: &ObjectId) -> String {
        Self::create_device_scope(&device_id.to_string())
    }
    pub fn create_device_scope(device_id: &str) -> String {
        format!("device:{}", device_id.to_string())
    }

    pub async fn create_from(db: &Arc<Mongo>, create_body: CreateDeviceBody) -> ServerResult<()> {
        let device_id = match create_body.id {
            Some(id) => ObjectId::parse_str(&id)?,
            None => ObjectId::new(),
        };
        let self_scope = Self::scope_for_device(&device_id);
        let allowed_scopes = match create_body.allowed_scopes.contains(&self_scope) {
            true => create_body.allowed_scopes,
            false => {
                let mut allowed_scopes = create_body.allowed_scopes;
                allowed_scopes.push(self_scope);
                allowed_scopes
            }
        };

        // The relay addresses every tunnel solely by `short_id` — the first 6
        // hex chars of SHA256(device_id), a 24-bit space grindable in seconds.
        // Enrollment lets the client choose `device_id`, so without this guard
        // an attacker could register a device whose `short_id` collides with a
        // victim's, seize the victim's relay slot on connect, and MITM its
        // traffic. `short_id` must therefore be unique across devices.
        //
        // This pre-check gives a clean error in the common case; the unique
        // index on `short_id` (see `Mongo::ensure_indexes`) is the race-safe
        // backstop, enforced again when we handle the insert result below.
        let short_id = short_device_id(&device_id.to_string());
        if db
            .devices()
            .find_one(doc! { "short_id": &short_id })
            .await?
            .is_some()
        {
            return Err(ServerError::bad_request(
                "device short id collision; regenerate the device id and retry",
            ));
        }

        let now = DateTime::now();
        let node = DeviceDoc {
            id: Some(device_id.clone()),
            short_id,
            name: create_body.name,
            updated_at: now,
            created_at: now,
            version: "".to_string(),
            target_version: create_body
                .target_version
                .unwrap_or(default_stable_version()),
            config: DeviceClientConfig::default(),
            owner_scope: create_body.owner_scope,
            allowed_scopes,
            system_info: create_body.system_info,
            api_key_id: create_body.api_key_id,
            last_config_hash: "".to_string(),
            last_deployment_hash: "".to_string(),
            pending_lifecycle_updates: vec![],
        };
        // Race-safe backstop: even if two colliding enrollments pass the
        // pre-check concurrently, the unique `short_id` (and `_id`) index makes
        // exactly one insert win. Surface the loser as a clean 400 rather than
        // a generic 500 so it never looks like a server fault.
        match db.devices().insert_one(node.clone()).await {
            Ok(_) => Ok(()),
            Err(e) if is_duplicate_key_error(&e) => Err(ServerError::bad_request(
                "device id or short id already exists; regenerate the device id and retry",
            )),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn invalidate_deployment_hash(
        db: &Arc<Mongo>,
        device_oid: &ObjectId,
    ) -> ServerResult<()> {
        let _ = db
            .devices()
            .update_one(
                doc! { "_id": device_oid },
                doc! { "$set": { "last_deployment_hash": "" } },
            )
            .await?;
        Ok(())
    }

    pub async fn push_lifecycle_update(
        db: &Arc<Mongo>,
        device_oid: &ObjectId,
        update: LifecycleUpdate,
    ) -> ServerResult<()> {
        db.devices()
            .update_one(
                doc! { "_id": device_oid },
                doc! { "$push": { "pending_lifecycle_updates": to_bson(&update).map_err(|e| ServerError::internal_error(&e.to_string()))? } },
            )
            .await?;
        DeviceDoc::invalidate_deployment_hash(db, device_oid).await?;
        Ok(())
    }

    pub async fn remove_device(&self, claims: &Claims, db: &Arc<Mongo>) -> ServerResult<()> {
        let api_keys_col = db.api_keys();
        let roles_col = db.roles();

        // Delete associated API keys
        api_keys_col
            .delete_many(doc! { "_id": self.api_key_id })
            .await
            .map_err(|_| ServerError::internal_error("Failed to delete API keys"))?;

        // Delete any roles scoped to this node
        roles_col
            .delete_many(doc! { "reference_id": self.api_key_id })
            .await
            .map_err(|_| ServerError::internal_error("Failed to delete roles"))?;

        // Check access and delete device
        let success = claims
            .delete_one_with_access(&db.devices(), doc! { "_id": &self.id.clone().unwrap() })
            .await?;

        if !success {
            return Err(ServerError::not_found(
                "Node you are trying to remove does not exist",
            ));
        }
        Ok(())
    }

    pub async fn handle_heartbeat(
        &self,
        claims: Claims,
        db: &Arc<Mongo>,
        payload: HeartbeatRequest,
        config: &Arc<AppConfig>,
    ) -> ServerResult<HeartbeatResponse> {
        // Determine the wire format this device understands.
        // None / 1 = legacy (flat "jobs" list); 2+ = new (services/observers/job_defs).
        // DeploymentRevision's custom Serialize always emits both formats, so this flag
        // is informational and available for future selective serialization.
        let use_new_format = payload.supported_revision_format.unwrap_or(1) >= 2;
        let _ = use_new_format; // currently both paths receive the same serialized value

        let mut update_fields = doc! {};
        if let Some(sys_info) = payload.system_info {
            update_fields.insert("system_info", mongodb::bson::to_bson(&sys_info).unwrap());
        }
        if let Some(client_version) = payload.client_version {
            update_fields.insert("version", client_version);
        }

        if !update_fields.is_empty() {
            update_fields.insert("updated_at", DateTime::now());
        }

        let _ = db
            .devices()
            .update_one(
                doc! {
                    "_id": &self.id.unwrap()
                },
                doc! {
                    "$set": update_fields
                },
            )
            .await;

        let mut ack_report_hash = None;
        if let Some(deploy_report) = payload.deploy_report {
            let body = CreateDeployReportBody {
                device_id: self.id.clone().unwrap(),
                revision_id: deploy_report
                    .get_revision_id()
                    .unwrap_or_default()
                    .to_string(),
                kind: deploy_report.clone(),
                expires_at: Some(DateTime::from_system_time(
                    SystemTime::now()
                        + Duration::from_hours(24 * config.report_retention_days as u64),
                )),
            };
            let res = DeployReportDoc::create_or_update(db, body).await;
            if let Err(err) = res {
                tracing::error!("Failed to create deploy report: {}", err);
            }
            ack_report_hash = Some(deploy_report.get_hash().to_string());

            match deploy_report {
                DeployReportKind::RollbackReport(_rollback) => {
                    // Single-revision model: rollback events from old
                    // devices are accepted and stored as part of the deploy
                    // report stream (for visibility in `logs`) but no
                    // longer drive any server-side revision flip — there's
                    // only one revision per device.
                }
                DeployReportKind::JobRunReport(report) => {
                    let _ = JobRunDoc::update_status(
                        db,
                        &report.run_id,
                        report.status.clone(),
                        report.error.clone(),
                    )
                    .await;
                }
                _ => {}
            }
        }

        let ack_hash_list = match ack_report_hash {
            Some(hash) => Some(vec![hash]),
            None => None,
        };

        // Load and clear pending lifecycle updates
        let pending_updates = self.pending_lifecycle_updates.clone();
        if !pending_updates.is_empty() {
            let _ = db
                .devices()
                .update_one(
                    doc! { "_id": self.id.unwrap() },
                    doc! { "$set": { "pending_lifecycle_updates": [] } },
                )
                .await;
        }

        // Load pending job runs and mark them as running
        let pending_job_runs = JobRunDoc::get_pending_for_device(db, self.id.unwrap())
            .await
            .unwrap_or_default();
        for run in &pending_job_runs {
            let _ = JobRunDoc::mark_running(db, &run.run_id).await;
        }

        let target_hash =
            build_instruction_hash(&self.last_deployment_hash, &self.last_config_hash);
        if payload.last_instruction_hash == target_hash {
            return Ok(HeartbeatResponse {
                up_to_date: true,
                config: None,
                instruction_hash: target_hash.clone(),
                target_revision: None,
                received_report_hashes: ack_hash_list,
                lifecycle_updates: pending_updates.clone(),
                pending_job_runs: pending_job_runs.clone(),
            });
        }

        let out = DeployRevisionDoc::get_active_device_deployment(&db, self.id.unwrap()).await;
        let target_revision = match out {
            Ok(Some(revision)) => Some(revision.revision),
            Ok(None) => Some(DeploymentRevision::empty()),
            _ => None,
        };

        let new_deployment_hash = match &target_revision {
            Some(revision) => revision.get_hash(),
            None => "".to_string(),
        };
        let config_hash = self.config.get_hash().to_string();
        // update last_deployment_hash in database
        let _ = db
            .devices()
            .update_one(
                doc! {
                    "_id": &self.id.unwrap()
                },
                doc! {
                    "$set": doc! {
                        "last_deployment_hash": &new_deployment_hash,
                        "last_config_hash": &config_hash,
                    }
                },
            )
            .await;

        let resp = HeartbeatResponse {
            up_to_date: false,
            config: Some(self.config.clone()),
            instruction_hash: build_instruction_hash(&new_deployment_hash, &config_hash),
            target_revision,
            received_report_hashes: ack_hash_list,
            lifecycle_updates: pending_updates,
            pending_job_runs,
        };
        Ok(resp)
    }

    pub async fn get_status(&self, db: &Arc<Mongo>) -> ServerResult<DeviceStatus> {
        let active_revision =
            DeployRevisionDoc::get_active_device_deployment(db, self.id.clone().unwrap()).await?;
        let observations = match active_revision {
            Some(revision) => {
                let observations = DeployReportDoc::get_device_observations(
                    db,
                    &self.id.clone().unwrap(),
                    &revision.revision.id.unwrap(),
                )
                .await?;
                observations
            }
            None => vec![],
        };
        let status = DeviceStatus {
            incidents: vec![],
            device_id: Some(self.id.clone().unwrap().to_string()),
            observations,
        };
        Ok(status)
    }

    pub async fn get_statuses(
        device_ids: &[ObjectId],
        db: &Arc<Mongo>,
    ) -> ServerResult<Vec<DeviceStatus>> {
        let active_revision_ids =
            DeployRevisionDoc::get_active_devices_deployment_ids(db, device_ids).await?;
        let observations =
            DeployReportDoc::get_devices_observations(db, &active_revision_ids).await?;

        let statuses = device_ids
            .iter()
            .map(|id| {
                let observations = observations
                    .get(&id.to_string())
                    .cloned()
                    .unwrap_or_default();
                let status = DeviceStatus {
                    incidents: vec![],
                    device_id: Some(id.to_string()),
                    observations,
                };
                status
            })
            .collect();
        Ok(statuses)
    }

    pub async fn add_or_update_device_access(
        &self,
        db: &Arc<Mongo>,
        email: &str,
        role: Role,
    ) -> ServerResult<()> {
        let reference_id = UserDoc::create_reference_id(email);
        let scope = Self::scope_for_device(&self.id.clone().unwrap());

        // Create binding (your RoleDoc::create already encodes "role + scope" binding).
        // If it should be idempotent, implement create-as-upsert inside RoleDoc::create or handle dup key errors here.
        RoleDoc::create(
            db,
            CreateRoleBinding {
                reference_id,
                role,
                scope,
            },
        )
        .await?;

        Ok(())
    }

    pub async fn remove_device_access(&self, db: &Arc<Mongo>, email: &str) -> ServerResult<()> {
        let reference_id = UserDoc::create_reference_id(email);
        let scope = Self::scope_for_device(&self.id.clone().unwrap());

        // If your RoleDoc has a delete helper, call it. Otherwise do a direct delete.
        let roles = db.roles(); // adjust to your collection getter
        let res = roles
            .delete_one(doc! { "reference_id": &reference_id, "scope": &scope })
            .await?;

        if res.deleted_count == 0 {
            return Err(ServerError::not_found("Access binding not found"));
        }
        Ok(())
    }

    pub async fn add_allowed_scope(&self, db: &Arc<Mongo>, scope: &str) -> ServerResult<()> {
        // update device in db
        let device = self.clone();
        let res = db
            .devices()
            .update_one(
                doc! { "_id": &device.id.unwrap() },
                doc! { "$addToSet": { "allowed_scopes": scope } },
            )
            .await?;

        if res.modified_count == 0 {
            return Err(ServerError::not_found("Device not found"));
        }
        Ok(())
    }

    pub async fn remove_allowed_scope(&self, db: &Arc<Mongo>, scope: &str) -> ServerResult<()> {
        // update device in db
        let device = self.clone();
        let res = db
            .devices()
            .update_one(
                doc! { "_id": &device.id.unwrap() },
                doc! { "$pull": { "allowed_scopes": scope } },
            )
            .await?;

        if res.modified_count == 0 {
            return Err(ServerError::not_found("Device not found"));
        }
        Ok(())
    }

    pub async fn list_users_with_access(&self, db: &Arc<Mongo>) -> ServerResult<Vec<User>> {
        let scopes = self.allowed_scopes.clone();

        // Fetch all role bindings for this device scope.
        // Assumes RoleDoc stored fields: reference_id, scope, role.
        let roles = db.roles(); // adjust
        let mut cursor = roles.find(doc! { "scope": { "$in": scopes } }).await?;

        let mut emails: HashMap<String, Role> = HashMap::new();
        let mut org_ids: Vec<String> = Vec::new();

        while let Some(role_doc) = cursor.try_next().await? {
            if let Some(email) = role_doc.reference_id.strip_prefix("user:") {
                emails.insert(email.to_string(), role_doc.role);
            } else if let Some(org) = role_doc.reference_id.strip_prefix("org:") {
                org_ids.push(org.to_string());
            }
        }

        let org_members = match !org_ids.is_empty() {
            true => org::get_org_members(db, org_ids).await?,
            false => Vec::new(),
        };

        let mut users_out: Vec<User> = Vec::new();
        if !emails.is_empty() {
            let email_vec = emails
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<String>>();
            let mut c = db
                .users() // adjust to your users collection getter
                .find(doc! { "email": { "$in": &email_vec } })
                .await?;

            while let Some(udoc) = c.try_next().await? {
                if let Some(email) = &udoc.email {
                    if let Some(user_role) = &emails.get(email) {
                        users_out.push(udoc.to_public_user(user_role)); // adjust mapping to shared User
                    }
                }
            }
        }
        // also add owner
        if let Some(user_owner) = self.owner_scope.strip_prefix("user:") {
            let owner = db
                .users() // adjust to your users collection getter
                .find_one(doc! { "email": user_owner })
                .await?;
            if let Some(owner) = owner {
                users_out.push(owner.to_public_user(&Role::Owner));
            }
        }

        let mut merged: HashMap<String, User> = HashMap::new();

        for u in users_out.into_iter() {
            merged.insert(u.email.clone(), u);
        }

        for u in org_members.into_iter() {
            match merged.get_mut(&u.email) {
                None => {
                    merged.insert(u.email.clone(), u);
                }
                Some(existing) => {
                    if u.role.rank() > existing.role.rank() {
                        existing.role = u.role;
                    }
                }
            }
        }

        Ok(merged.into_values().collect())
    }
}

impl DeviceDoc {
    pub fn to_public_devices(devices: Vec<(DeviceDoc, Role)>) -> Vec<PublicDevice> {
        devices
            .into_iter()
            .map(|(device, role)| device.to_public_device(&role))
            .collect()
    }

    pub fn to_public_device(self, role: &Role) -> PublicDevice {
        PublicDevice {
            id: self.id.unwrap().to_string(),
            name: self.name.clone(),
            short_id: self.short_id.clone(),
            updated_at: self.updated_at.try_to_rfc3339_string().unwrap(),
            created_at: self.created_at.try_to_rfc3339_string().unwrap(),
            last_connection: Some("".to_string()),
            online: false,
            version: self.version.clone(),
            target_version: self.target_version.clone(),
            config: self.config.clone(),
            system_info: self.system_info.clone(),
            role: role.clone(),
        }
    }
}

impl AccessControlled for DeviceDoc {
    fn owner_scope_field() -> &'static str {
        "owner_scope"
    }
    fn allowed_scopes_field() -> Option<&'static str> {
        Some("allowed_scopes")
    }
    fn owner_scope(&self) -> &str {
        &self.owner_scope
    }
    fn allowed_scopes(&self) -> Option<Vec<String>> {
        Some(self.allowed_scopes.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Critical-2 regression: only `owner_scope` / `allowed_scopes` count as
    /// access-control changes. The device update handler uses this to require
    /// Admin (not just Editor) before touching ownership or the ACL — otherwise
    /// an Editor could seize the device or share it with arbitrary scopes.
    #[test]
    fn touches_access_control_flags_only_privileged_fields() {
        // Non-privileged fields never trip the gate.
        assert!(!UpdateDeviceBody::default().touches_access_control());

        let benign = UpdateDeviceBody {
            version: Some("1.2.3".to_string()),
            target_version: Some("latest".to_string()),
            ..Default::default()
        };
        assert!(!benign.touches_access_control());

        // Ownership transfer requires Admin.
        let owner = UpdateDeviceBody {
            owner_scope: Some("user:attacker@evil.com".to_string()),
            ..Default::default()
        };
        assert!(owner.touches_access_control());

        // ACL change requires Admin.
        let acl = UpdateDeviceBody {
            allowed_scopes: Some(vec!["org:victim".to_string()]),
            ..Default::default()
        };
        assert!(acl.touches_access_control());
    }

    /// Critical-1 threat model: `short_id` is only the first 6 hex chars of
    /// SHA256(device_id) — a 24-bit space where collisions are trivially found
    /// (here, among sequential ids). Because the relay routes tunnels solely by
    /// `short_id`, two devices sharing one lets a newcomer hijack the other's
    /// tunnel. This pins WHY enrollment must reject colliding short_ids
    /// (`DeviceDoc::create_from` + the unique index); if someone widens the
    /// namespace enough that this stops finding a collision, revisit that guard.
    #[test]
    fn short_ids_collide_and_therefore_must_be_unique() {
        use std::collections::HashMap;

        let mut seen: HashMap<String, String> = HashMap::new();
        for i in 0..200_000u64 {
            // Any 24-hex string is a valid device id for short_id purposes.
            let device_id = format!("{:024x}", i);
            let short = short_device_id(&device_id);
            if let Some(prev) = seen.insert(short, device_id.clone()) {
                assert_ne!(
                    prev, device_id,
                    "distinct device ids must be what collided"
                );
                return; // found two distinct ids sharing a routing key
            }
        }
        panic!("expected a short_id collision within the 24-bit space");
    }
}
