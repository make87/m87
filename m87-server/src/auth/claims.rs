use std::sync::Arc;

use axum::{extract::FromRequestParts, http::request::Parts};
use axum_extra::TypedHeader;
use futures::TryStreamExt;
use headers::{authorization::Bearer, Authorization};
use mongodb::{bson::Document, options::FindOptions, Collection};
use tracing::info;

use crate::{
    auth::access_control::AccessControlled,
    config::AppConfig,
    db::Mongo,
    models::{
        api_key::ApiKeyDoc,
        roles::{Role, RoleDoc},
        user::UserDoc,
    },
    response::{ServerError, ServerResult},
    util::{app_state::AppState, pagination::RequestPagination},
};

#[derive(Debug, Clone)]
pub struct Claims {
    pub roles: Vec<RoleDoc>,
    pub is_admin: bool,
}

impl FromRequestParts<AppState> for Claims {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) =
            TypedHeader::<Authorization<Bearer>>::from_request_parts(parts, state)
                .await
                .map_err(|_| ServerError::missing_token("missing API key"))?;

        let token = bearer.token();
        Claims::from_bearer_or_key(token, &state.db, &state.config).await
    }
}

impl Claims {
    pub async fn from_bearer_or_key(
        token: &str,
        db: &Arc<Mongo>,
        config: &Arc<AppConfig>,
    ) -> ServerResult<Self> {
        let is_jwt = token.matches('.').count() == 2;

        if is_jwt {
            // Handle JWT
            let user = UserDoc::get_or_create(&token, db, config).await?;

            if !user.approved {
                return Err(ServerError::unauthorized("user not approved"));
            }

            let reference_id = user.get_reference_id();
            let mut roles = RoleDoc::list_for_reference(db, &reference_id).await?;
            roles.push(RoleDoc {
                id: None,
                reference_id: reference_id.clone(),
                scope: format!("user:{}", reference_id),
                role: Role::Owner,
                created_at: None,
            });

            let is_admin = match user.email {
                Some(email) => config.admin_emails.contains(&email),
                None => false,
            };
            Ok(Self { roles, is_admin })
        } else {
            // check if token is config.admin_key
            match &config.admin_key {
                Some(admin_key) => {
                    if token == admin_key {
                        return Ok(Self {
                            roles: vec![],
                            is_admin: true,
                        });
                    }
                }
                _ => {}
            }

            // Handle API key
            let key_doc = ApiKeyDoc::find_and_validate_key(db, token).await?;
            let roles = RoleDoc::list_for_reference(db, &key_doc.key_id).await?;
            Ok(Self {
                roles,
                is_admin: false,
            })
        }
    }

    pub async fn find_one_with_access<T>(
        &self,
        coll: &Collection<T>,
        filter: Document,
    ) -> ServerResult<Option<T>>
    where
        T: AccessControlled + Unpin + Send + Sync + serde::de::DeserializeOwned,
    {
        let mut combined = filter.clone();
        combined.extend(T::access_filter(&self.get_scopes()));
        let doc = coll
            .find_one(combined)
            .await
            .map_err(|_| ServerError::internal_error("DB query failed"))?;
        Ok(doc)
    }

    pub async fn count_with_access<T>(&self, coll: &Collection<T>) -> ServerResult<u64>
    where
        T: AccessControlled + Unpin + Send + Sync + serde::de::DeserializeOwned,
    {
        let filter = T::access_filter(&self.get_scopes());
        let count = coll
            .count_documents(filter)
            .await
            .map_err(|_| ServerError::internal_error("Count failed"))?;
        Ok(count)
    }

    pub async fn list_with_access<T>(
        &self,
        coll: &Collection<T>,
        pagination: &RequestPagination,
    ) -> ServerResult<Vec<T>>
    where
        T: AccessControlled + Unpin + Send + Sync + serde::de::DeserializeOwned,
    {
        let filter = T::access_filter(&self.get_scopes());

        let options = FindOptions::builder()
            .skip(Some(pagination.offset))
            .limit(Some(pagination.limit as i64))
            .build();

        let cursor = coll
            .find(filter)
            .with_options(options)
            .await
            .map_err(|_| ServerError::internal_error("Query failed"))?;

        // Collect all documents directly (futures::TryStreamExt)
        let results: Vec<T> = cursor
            .try_collect()
            .await
            .map_err(|_| ServerError::internal_error("Cursor decode failed"))?;

        Ok(results)
    }

    /// Generic update with access control.
    pub async fn update_one_with_access<T>(
        &self,
        coll: &Collection<T>,
        id_filter: Document,
        update: Document,
    ) -> ServerResult<bool>
    where
        T: AccessControlled + Unpin + Send + Sync + serde::de::DeserializeOwned,
    {
        let mut filter = id_filter.clone();
        filter.extend(T::access_filter(&self.get_scopes()));
        let res = coll
            .update_one(filter, update)
            .await
            .map_err(|_| ServerError::internal_error("Update failed"))?;
        if res.matched_count == 0 {
            return Err(ServerError::forbidden(
                "Not authorized or document not found",
            ));
        }
        Ok(true)
    }

    fn get_scopes(&self) -> Vec<String> {
        self.roles.iter().map(|role| role.scope.clone()).collect()
    }

    /// Delete with access enforcement.
    pub async fn delete_one_with_access<T>(
        &self,
        coll: &Collection<T>,
        id_filter: Document,
    ) -> ServerResult<bool>
    where
        T: AccessControlled + Unpin + Send + Sync + serde::de::DeserializeOwned,
    {
        let mut filter = id_filter.clone();
        filter.extend(T::access_filter(&self.get_scopes()));
        let res = coll
            .delete_one(filter)
            .await
            .map_err(|_| ServerError::internal_error("Delete failed"))?;
        if res.deleted_count == 0 {
            return Err(ServerError::forbidden(
                "Not authorized or document not found",
            ));
        }
        Ok(true)
    }

    pub async fn find_one_with_scope_and_role<T>(
        &self,
        coll: &Collection<T>,
        base_filter: Document,
        required_role: Role,
    ) -> ServerResult<Option<T>>
    where
        T: AccessControlled + serde::de::DeserializeOwned + Unpin + Send + Sync,
    {
        // Get all scopes for which user has >= required_role
        let allowed_scopes: Vec<String> = self
            .roles
            .iter()
            .filter(|r| Role::allows(&r.role, &required_role))
            .map(|r| r.scope.clone())
            .collect();

        if allowed_scopes.is_empty() {
            return Err(ServerError::forbidden("Insufficient role for any scope"));
        }

        let mut filter = base_filter.clone();
        filter.extend(T::access_filter(&allowed_scopes));

        let doc = coll
            .find_one(filter)
            .await
            .map_err(|_| ServerError::internal_error("DB query failed"))?;

        if doc.is_none() {
            return Err(ServerError::forbidden("Not found or access denied"));
        }

        Ok(doc)
    }

    pub fn has_scope_and_role(&self, scope: &str, required_role: Role) -> bool {
        self.roles
            .iter()
            .any(|r| r.scope == scope && Role::allows(&r.role, &required_role))
    }
}
