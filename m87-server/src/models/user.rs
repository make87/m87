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

        // Prepare update
        let now = DateTime::now();
        let filter = doc! { "sub": &claims.sub };
        let update = doc! {
            "$set": { "last_login": &now },
            "$inc": { "total_logins": 1 }
        };

        // Try to update existing user and return it
        if let Some(user) = collection
            .find_one_and_update(filter.clone(), update.clone())
            .await?
        {
            return Ok(user);
        }

        // Otherwise create new user
        let (email, name) = get_email_and_name_from_token(token, config).await?;
        // check if user domain is in config.user_auto_accept_domains if users_need_approval is true
        let approved = match config.users_need_approval {
            true => {
                if let Some(mail) = &email {
                    !config
                        .user_auto_accept_domains
                        .contains(&mail.split('@').last().unwrap().to_string())
                } else {
                    false
                }
            }
            false => true,
        };

        let new_user = UserDoc {
            id: None,
            name,
            email,
            sub: claims.sub.clone(),
            approved,
            created_at: Some(now.clone()),
            last_login: Some(now),
            total_logins: 1,
        };

        collection.insert_one(new_user.clone()).await?;
        Ok(new_user)
    }

    pub fn get_reference_id(&self) -> String {
        self.email.clone().unwrap_or(self.sub.clone())
    }
}
