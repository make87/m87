use crate::{
    auth::jwk::{get_email_and_name_from_token, validate_token},
    config::AppConfig,
    db::Mongo,
    response::ServerResult,
};
use mongodb::bson::{doc, oid::ObjectId, DateTime};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    pub name: Option<String>,
    pub email: Option<String>,
    pub sub: String,

    pub approved: bool,

    #[serde(default)]
    pub created_at: Option<DateTime>,
    pub last_login: Option<DateTime>,
    pub total_logins: u64,
}

impl UserDoc {
    pub async fn get_or_create(
        token: &str,
        db: &Arc<Mongo>,
        config: &Arc<AppConfig>,
    ) -> ServerResult<UserDoc> {
        let collection = db.users();

        let claims = validate_token(token, config).await?;
        // find by sub
        let user = collection.find_one(doc! { "sub": &claims.sub }).await?;

        let user = match user {
            Some(user) => user,
            None => {
                let (email, name) = get_email_and_name_from_token(token, config).await?;

                let new_user = UserDoc {
                    id: None,
                    name,
                    email,
                    sub: claims.sub.clone(),
                    approved: !config.users_need_approval,
                    created_at: Some(DateTime::now()),
                    last_login: None,
                    total_logins: 0,
                };

                collection.insert_one(new_user.clone()).await?;
                new_user
            }
        };

        Ok(user)
    }

    pub fn get_reference_id(&self) -> String {
        self.email.clone().unwrap_or(self.sub.clone())
    }
}
