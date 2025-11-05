use std::time::Duration;

use crate::{
    models::{
        api_key::ApiKeyDoc, device::DeviceDoc, device_auth_request::DeviceAuthRequestDoc,
        roles::RoleDoc, ssh_key::SSHPubKeyDoc, user::UserDoc,
    },
    response::ServerResult,
};
use mongodb::{bson::doc, options::IndexOptions};
use mongodb::{options::ClientOptions, Client, Collection, IndexModel};

#[derive(Clone)]
pub struct Mongo {
    pub client: Client,
    pub db_name: String,
}

impl Mongo {
    pub async fn connect(url: &str, db_name: &str) -> ServerResult<Self> {
        let mut opts = ClientOptions::parse(url).await?;
        opts.app_name = Some("nexus".into());
        let client = Client::with_options(opts)?;
        Ok(Self {
            client,
            db_name: db_name.into(),
        })
    }

    fn col<T: Send + Sync>(&self, name: &str) -> Collection<T> {
        self.client.database(&self.db_name).collection(name)
    }

    pub fn devices(&self) -> Collection<DeviceDoc> {
        self.col("devices")
    }

    pub fn users(&self) -> Collection<UserDoc> {
        self.col("users")
    }

    pub fn device_auth_requests(&self) -> Collection<DeviceAuthRequestDoc> {
        self.col("device_auth_requests")
    }

    pub fn roles(&self) -> Collection<RoleDoc> {
        self.col("roles")
    }

    pub fn api_keys(&self) -> Collection<ApiKeyDoc> {
        self.col("api_keys")
    }

    pub fn ssh_keys(&self) -> Collection<SSHPubKeyDoc> {
        self.col("ssh_keys")
    }

    pub async fn ensure_indexes(&self) -> ServerResult<()> {
        // Add indexes as needed later (expires_at TTL, etc.)
        self.roles()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "key_id": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await?;

        self.device_auth_requests()
            .create_index(IndexModel::builder().keys(doc! { "request_id": 1 }).build())
            .await?;

        // TTL index for DeviceAuthRequestDoc (auto-delete after 24 hours)
        self.device_auth_requests()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "created_at": 1 })
                    .options(
                        IndexOptions::builder()
                            .expire_after(Some(Duration::from_secs(60 * 60 * 24 * 2))) // 2 days
                            .build(),
                    )
                    .build(),
            )
            .await?;

        self.device_auth_requests()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "owner_scope": 1 })
                    .build(),
            )
            .await?;

        self.devices()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "owner_scope": 1 })
                    .build(),
            )
            .await?;
        self.devices()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "allowed_scopes": 1 })
                    .build(),
            )
            .await?;

        self.ssh_keys()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "owner_scope": 1 })
                    .build(),
            )
            .await?;
        self.ssh_keys()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "allowed_scopes": 1 })
                    .build(),
            )
            .await?;

        self.api_keys()
            .create_index(IndexModel::builder().keys(doc! { "key_id": 1 }).build())
            .await?;

        // add index to users sub
        self.users()
            .create_index(IndexModel::builder().keys(doc! { "sub": 1 }).build())
            .await?;
        Ok(())
    }
}
