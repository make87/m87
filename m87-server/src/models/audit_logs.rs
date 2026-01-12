use std::sync::Arc;

use futures::TryStreamExt;
use m87_shared::device::AuditLog;
use mongodb::{
    bson::{DateTime, doc, oid::ObjectId},
    options::FindOptions,
};
use serde::{Deserialize, Serialize};

use crate::{
    db::Mongo,
    response::{ServerError, ServerResult},
    util::pagination::RequestPagination,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AuditLogDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub timestamp: DateTime,
    pub user_id: ObjectId,
    pub user_name: String,
    pub action: String,
    pub details: String,
    pub device_id: Option<ObjectId>,
    #[serde(default)]
    pub expires_at: Option<DateTime>,
}

impl AuditLogDoc {
    pub async fn add(
        db: &Arc<Mongo>,
        user_id: ObjectId,
        user_name: String,
        action: String,
        details: String,
        expires_at: Option<DateTime>,
        device_id: Option<ObjectId>,
    ) -> ServerResult<()> {
        let doc: AuditLogDoc = Self {
            id: None,
            timestamp: DateTime::now(),
            user_id,
            user_name,
            action,
            details,
            device_id,
            expires_at,
        };
        db.audit_logs()
            .insert_one(&doc)
            .await
            .map_err(|_| ServerError::internal_error("Failed to insert API key"))?;
        Ok(())
    }

    pub async fn list_for_device(
        db: &Arc<Mongo>,
        device_id: ObjectId,
        pagination: &RequestPagination,
    ) -> ServerResult<Vec<AuditLogDoc>> {
        let options = FindOptions::builder()
            .skip(Some(pagination.offset))
            .limit(Some(pagination.limit as i64))
            // sort by index descending
            .build();
        let cursor = db
            .audit_logs()
            .find(doc! { "device_id": device_id })
            .with_options(options)
            .await?;
        let results: Vec<AuditLogDoc> = cursor
            .try_collect()
            .await
            .map_err(|_| ServerError::internal_error("Cursor decode failed"))?;
        Ok(results)
    }

    pub fn to_audit_log(&self) -> AuditLog {
        AuditLog {
            user_name: self.user_name.clone(),
            timestamp: self.timestamp.try_to_rfc3339_string().unwrap(),
            action: self.action.clone(),
            details: self.details.clone(),
        }
    }
}
