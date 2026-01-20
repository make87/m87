use crate::{
    db::Mongo,
    models::{org, user::UserDoc},
    response::{ServerError, ServerResult},
};
use futures::StreamExt;
use mongodb::{
    bson::{Bson, DateTime, doc, oid::ObjectId},
    options::{FindOneAndUpdateOptions, ReturnDocument},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Import shared Role type
pub use m87_shared::roles::Role;

// Helper function to convert Role to Bson (can't use From trait due to orphan rules)
pub fn role_to_bson(role: &Role) -> Bson {
    mongodb::bson::to_bson(role).unwrap()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    /// API key id or jwt user sub
    pub reference_id: String,

    /// Scope identifier: "org:<id>", "user:<reference_id>"
    pub scope: String,

    pub role: Role,

    #[serde(default)]
    pub created_at: Option<DateTime>,
}

/// Create request body used in admin APIs or backend sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRoleBinding {
    pub reference_id: String,
    pub scope: String,
    pub role: Role,
}

impl RoleDoc {
    /// Create or update a role binding for a user and scope
    pub async fn create(db: &Arc<Mongo>, body: CreateRoleBinding) -> ServerResult<Self> {
        let now = DateTime::now();

        let filter = doc! {
            "reference_id": &body.reference_id,
            "scope": &body.scope,
        };

        // ensure complete doc and clean updates
        let update = doc! {
            "$set": {
                "role": role_to_bson(&body.role),
                "updated_at": now,
            },
            "$setOnInsert": {
                "reference_id": &body.reference_id,
                "scope": &body.scope,
                "created_at": now,
            }
        };

        let options = FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::After)
            .build();

        let coll = db.roles();
        let updated = coll
            .find_one_and_update(filter, update)
            .with_options(options)
            .await
            .map_err(|_| ServerError::internal_error("Failed to upsert role binding"))?;

        Ok(updated.unwrap())
    }

    pub async fn check_if_exists(db: &Arc<Mongo>, body: CreateRoleBinding) -> ServerResult<bool> {
        let filter = doc! {
            "reference_id": &body.reference_id,
            "scope": &body.scope,
            "role": role_to_bson(&body.role),
        };

        let coll = db.roles();
        let exists = coll
            .find_one(filter)
            .await
            .map_err(|_| ServerError::internal_error("Failed to check if role binding exists"))?;

        Ok(exists.is_some())
    }

    /// List all bindings (used for admin views)
    pub async fn list_all(db: &Arc<Mongo>) -> ServerResult<Vec<RoleDoc>> {
        let mut cursor = db
            .roles()
            .find(doc! {})
            .await
            .map_err(|_| ServerError::internal_error("Failed to list role bindings"))?;

        let mut items = Vec::new();
        while let Some(res) = cursor.next().await {
            match res {
                Ok(doc) => items.push(doc),
                Err(_) => return Err(ServerError::internal_error("Failed to decode role binding")),
            }
        }
        Ok(items)
    }

    /// List all bindings for a specific user (JWT sub)
    pub async fn list_for_reference(
        db: &Arc<Mongo>,
        reference_id: &str,
    ) -> ServerResult<Vec<RoleDoc>> {
        let mut cursor = db
            .roles()
            .find(doc! { "reference_id": reference_id })
            .await
            .map_err(|_| ServerError::internal_error("Failed to list user role bindings"))?;

        let mut items = Vec::new();
        while let Some(res) = cursor.next().await {
            match res {
                Ok(doc) => items.push(doc),
                Err(_) => return Err(ServerError::internal_error("Failed to decode role binding")),
            }
        }
        Ok(items)
    }

    /// Delete a specific binding (admin)
    pub async fn delete(db: &Arc<Mongo>, reference_id: &str, scope: &str) -> ServerResult<()> {
        db.roles()
            .delete_one(doc! { "reference_id": reference_id, "scope": scope })
            .await
            .map_err(|_| ServerError::internal_error("Failed to delete role binding"))?;
        Ok(())
    }
}

pub fn reference_id_for_subject(email_or_org_id: &str) -> String {
    if email_or_org_id.contains('@') {
        UserDoc::create_reference_id(email_or_org_id)
    } else {
        org::org_ref(email_or_org_id)
    }
}
