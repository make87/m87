use crate::{
    db::Mongo,
    response::{ServerError, ServerResult},
};
use futures::StreamExt;
use mongodb::{
    bson::{doc, oid::ObjectId, Bson, DateTime},
    options::{FindOneAndUpdateOptions, ReturnDocument},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Owner,
    Admin,
    Editor,
    Viewer,
}

impl From<Role> for Bson {
    fn from(role: Role) -> Self {
        mongodb::bson::to_bson(&role).unwrap()
    }
}

impl Role {
    pub fn allows(have: &Role, need: &Role) -> bool {
        use Role::*;
        matches!(
            (have, need),
            (Owner, _)
                | (Admin, Viewer | Editor | Admin)
                | (Editor, Viewer | Editor)
                | (Viewer, Viewer)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    /// API key id or jwt user sub
    pub reference_id: String,

    /// Scope identifier: "nexus:*", "org:<id>", "user:<reference_id>"
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
        // Avoid duplicates for same (user, scope)
        let filter = doc! {
            "reference_id": &body.reference_id,
            "scope": &body.scope,
        };

        let update = doc! {
            "$set": {
                "scope": &body.scope,
                "role": &body.role,
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

        // If Mongo didn’t return the new doc (shouldn’t happen with ReturnDocument::After),
        // synthesize it locally.
        Ok(updated.unwrap())
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
    pub async fn delete(db: &Arc<Mongo>, user_id: &str, scope: &str) -> ServerResult<()> {
        db.roles()
            .delete_one(doc! { "user_id": user_id, "scope": scope })
            .await
            .map_err(|_| ServerError::internal_error("Failed to delete role binding"))?;
        Ok(())
    }
}
