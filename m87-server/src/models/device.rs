use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use m87_shared::deploy_spec::{DeployReportKind, DeploymentRevision, build_instruction_hash};
use m87_shared::device::DeviceStatus;
use m87_shared::roles::Role;
use m87_shared::users::User;
use mongodb::bson::{DateTime, Document, doc, oid::ObjectId};

use serde::{Deserialize, Serialize};

// Import shared types
pub use m87_shared::config::DeviceClientConfig;
pub use m87_shared::device::{DeviceSystemInfo, PublicDevice, short_device_id};
pub use m87_shared::heartbeat::{HeartbeatRequest, HeartbeatResponse};
use tokio_stream::StreamExt;

use crate::config::AppConfig;
use crate::models::audit_logs::AuditLogDoc;
use crate::models::deploy_spec::{CreateDeployReportBody, DeployReportDoc, DeployRevisionDoc};
use crate::models::org;
use crate::models::roles::{CreateRoleBinding, RoleDoc, reference_id_for_subject};
use crate::{
    auth::{access_control::AccessControlled, claims::Claims},
    db::Mongo,
    response::{ServerError, ServerResult},
};

fn default_stable_version() -> String {
    "latest".to_string()
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

        let now = DateTime::now();
        let node = DeviceDoc {
            id: Some(device_id.clone()),
            short_id: short_device_id(&device_id.to_string()),
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
        };
        let _ = db.devices().insert_one(node.clone()).await?;
        Ok(())
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

        if let Some(deploy_report) = payload.deploy_report {
            let body = CreateDeployReportBody {
                device_id: self.id.clone().unwrap(),
                revision_id: deploy_report.get_revision_id().to_string(),
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

            if let DeployReportKind::RollbackReport(rollback) = deploy_report {
                // change active deplotment to rollback.new_revision_id
                let device_oid = self.id.clone().unwrap();
                let out = DeployRevisionDoc::get_active_device_deployment(&db, device_oid.clone())
                    .await?;

                if let Some(new_id) = &rollback.new_revision_id {
                    let _ = db
                        .deploy_revisions()
                        .update_one(
                            doc! { "revision.id": new_id, "device_id": &device_oid },
                            doc! { "$set": { "active": true } },
                        )
                        .await?;
                }

                match out {
                    Some(doc) => {
                        let filter = doc! { "revision.id": &doc.id, "device_id": &device_oid };
                        let update_doc = doc! { "active": false };
                        let _ = db.deploy_revisions().update_one(filter, update_doc).await?;
                    }
                    None => {}
                };

                let _ = AuditLogDoc::add(
                    &db,
                    &claims,
                    &config,
                    &format!(
                        "Rolled back deployment to {} for device {}",
                        &rollback.new_revision_id.unwrap_or("None".to_string()),
                        &device_oid
                    ),
                    "",
                    Some(device_oid.clone()),
                )
                .await;
            }
        }

        let target_hash =
            build_instruction_hash(&self.last_deployment_hash, &self.last_config_hash);
        if payload.last_instruction_hash == target_hash {
            return Ok(HeartbeatResponse {
                up_to_date: true,
                config: None,
                instruction_hash: target_hash.clone(),
                target_revision: None,
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
        };
        Ok(resp)
    }

    pub async fn get_status(&self, db: &Arc<Mongo>) -> ServerResult<DeviceStatus> {
        let active_revision =
            DeployRevisionDoc::get_active_device_deployment(db, self.id.clone().unwrap()).await?;
        let observations = match active_revision {
            Some(revision) => {
                let observations = DeployReportDoc::get_device_observations_since(
                    db,
                    &revision.revision.id.unwrap(),
                    &self.id.clone().unwrap(),
                    // since,
                )
                .await?;
                observations
            }
            None => vec![],
        };
        let status = DeviceStatus {
            incidents: vec![],
            observations,
        };
        Ok(status)
    }

    pub async fn add_or_update_device_access(
        &self,
        db: &Arc<Mongo>,
        email_or_org_id: &str,
        role: Role,
    ) -> ServerResult<()> {
        let reference_id = reference_id_for_subject(email_or_org_id);
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

    pub async fn remove_device_access(
        &self,
        db: &Arc<Mongo>,
        email_or_org_id: &str,
    ) -> ServerResult<()> {
        let reference_id = reference_id_for_subject(email_or_org_id);
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

    pub async fn list_users_with_access(&self, db: &Arc<Mongo>) -> ServerResult<Vec<User>> {
        let scope = Self::scope_for_device(&self.id.clone().unwrap());

        // Fetch all role bindings for this device scope.
        // Assumes RoleDoc stored fields: reference_id, scope, role.
        let roles = db.roles(); // adjust
        let mut cursor = roles.find(doc! { "scope": &scope }).await?;

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
