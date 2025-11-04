use crate::{
    db::Mongo,
    models::roles::{CreateRoleBinding, Role, RoleDoc},
    response::{ServerError, ServerResult},
};
use argon2::password_hash::{Error as PasswordHashError, SaltString};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use mongodb::bson::{doc, oid::ObjectId, DateTime};
use rand::{distributions::Alphanumeric, Rng};
use rand_core::OsRng; // secure random salt source
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    pub key_id: String,   // short random prefix (public identifier)
    pub key_hash: String, // Argon2 hash of full secret
    pub name: String,     // display label
    #[serde(default)]
    pub created_at: Option<DateTime>,
    #[serde(default)]
    pub expires_at: Option<DateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateApiKey {
    pub name: String,
    pub ttl_secs: Option<i64>,
    pub scopes: Vec<String>,
}

impl ApiKeyDoc {
    pub async fn create(db: &Arc<Mongo>, req: CreateApiKey) -> ServerResult<(Self, String)> {
        // 1. Generate key parts
        let (key_id, full_key, hashed_secret) = generate_api_key()
            .map_err(|_| ServerError::internal_error("Failed to generate key"))?;

        // 2. Compute expiration
        let now = DateTime::now();
        let expires_at = req
            .ttl_secs
            .map(|secs| DateTime::from_millis(now.timestamp_millis() + secs * 1000));

        // 3. Build document
        let doc = ApiKeyDoc {
            id: Some(ObjectId::new()),
            key_id: key_id.clone(),
            key_hash: hashed_secret,
            name: req.name,
            created_at: Some(now),
            expires_at,
        };

        // 4. Store in Mongo
        db.api_keys()
            .insert_one(&doc)
            .await
            .map_err(|_| ServerError::internal_error("Failed to insert API key"))?;

        // for each scope add a role in parallel
        let _ = futures::future::join_all(req.scopes.into_iter().map(|scope| {
            RoleDoc::create(
                db,
                CreateRoleBinding {
                    reference_id: doc.key_id.clone(),
                    role: Role::Editor,
                    scope,
                },
            )
        }))
        .await;

        // 5. Return both DB doc and plaintext key (show once to user)
        Ok((doc, full_key))
    }

    pub async fn delete(db: &Arc<Mongo>, key_id: &str) -> ServerResult<()> {
        db.api_keys()
            .delete_one(doc! { "key_id": key_id })
            .await
            .map_err(|_| ServerError::internal_error("Failed to delete API key"))?;
        Ok(())
    }

    pub async fn find_and_validate_key(db: &Arc<Mongo>, api_key: &str) -> ServerResult<ApiKeyDoc> {
        let (key_id, secret) =
            split_api_key(api_key).ok_or_else(|| ServerError::unauthorized("Malformed API key"))?;

        let key_doc = db
            .api_keys()
            .find_one(doc! { "key_id": &key_id })
            .await
            .map_err(|_| ServerError::internal_error("DB lookup failed"))?
            .ok_or_else(|| ServerError::unauthorized("Invalid key ID"))?;

        if !verify_api_key(&secret, &key_doc.key_hash) {
            return Err(ServerError::unauthorized("Invalid API key"));
        }

        if let Some(exp) = key_doc.expires_at {
            if exp < mongodb::bson::DateTime::now() {
                return Err(ServerError::unauthorized("API key expired"));
            }
        }

        Ok(key_doc)
    }
}

fn hash_api_key(key: &str) -> Result<String, PasswordHashError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2.hash_password(key.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

/// Verify a plaintext API key against a stored Argon2 hash.
///
/// Returns `true` if the key is valid.
fn verify_api_key(key: &str, hash: &str) -> bool {
    let parsed = PasswordHash::new(hash);
    match parsed {
        Ok(ph) => Argon2::default()
            .verify_password(key.as_bytes(), &ph)
            .is_ok(),
        Err(_) => false,
    }
}

fn generate_api_key() -> Result<(String, String, String), PasswordHashError> {
    let key_id: String = format!(
        "m87_{}",
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect::<String>()
    );

    let secret: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(40)
        .map(char::from)
        .collect();

    let full_key = format!("{}.{}", key_id, secret);
    let hashed_secret = hash_api_key(&secret)?;

    Ok((key_id, full_key, hashed_secret))
}

fn split_api_key(key: &str) -> Option<(String, String)> {
    let mut parts = key.splitn(2, '.');
    Some((parts.next()?.to_string(), parts.next()?.to_string()))
}
