use std::time::Duration;

use crate::{
    models::{
        api_key::ApiKeyDoc, node::NodeDoc, node_auth_request::NodeAuthRequestDoc, roles::RoleDoc,
        ssh_key::SSHPubKeyDoc,
    },
    response::NexusResult,
};
use mongodb::{bson::doc, options::IndexOptions};
use mongodb::{options::ClientOptions, Client, Collection, IndexModel};

#[derive(Clone)]
pub struct Mongo {
    pub client: Client,
    pub db_name: String,
}

impl Mongo {
    pub async fn connect(url: &str, db_name: &str) -> NexusResult<Self> {
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

    pub fn nodes(&self) -> Collection<NodeDoc> {
        self.col("nodes")
    }

    pub fn node_auth_requests(&self) -> Collection<NodeAuthRequestDoc> {
        self.col("node_auth_requests")
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

    pub async fn ensure_indexes(&self) -> NexusResult<()> {
        // Add indexes as needed later (expires_at TTL, etc.)
        self.roles()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "key_id": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await?;

        self.node_auth_requests()
            .create_index(IndexModel::builder().keys(doc! { "request_id": 1 }).build())
            .await?;

        // TTL index for NodeAuthRequestDoc (auto-delete after 24 hours)
        self.node_auth_requests()
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

        self.node_auth_requests()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "owner_scope": 1 })
                    .build(),
            )
            .await?;

        self.nodes()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "owner_scope": 1 })
                    .build(),
            )
            .await?;
        self.nodes()
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
        Ok(())
    }
}
