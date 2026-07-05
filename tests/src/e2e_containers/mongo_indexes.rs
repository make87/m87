//! E2E: the m87-server must create its MongoDB indexes at startup.
//!
//! `Mongo::ensure_indexes` runs on server boot. Missing/mis-shaped indexes on
//! the heartbeat-report hot path turn the deployment-status snapshot query into
//! a full collection scan + blocking in-memory sort in `mongod`, which drives
//! CPU and memory up under the report flood. This test brings up the real stack
//! (the server runs `ensure_indexes`), then connects to the Mongo container
//! directly and asserts the hot-path indexes exist.

use super::containers::E2EInfra;
use super::helpers::E2EError;
use mongodb::bson::Document;
use mongodb::Client;

/// Index names for a collection. MongoDB auto-names an index by joining
/// `<field>_<direction>` with `_`, so these names are deterministic.
async fn index_names(client: &Client, db: &str, collection: &str) -> Result<Vec<String>, E2EError> {
    client
        .database(db)
        .collection::<Document>(collection)
        .list_index_names()
        .await
        .map_err(|e| E2EError::Setup(format!("list_index_names({collection}): {e}")))
}

#[tokio::test]
async fn server_creates_expected_indexes() -> Result<(), E2EError> {
    let infra = E2EInfra::init().await?;

    // The server ran `ensure_indexes` against DB "e2e-tests" (see
    // containers.rs: MONGO_DB) at boot. Connect to the same Mongo container
    // directly via its mapped host port.
    let port = infra
        .mongo
        .get_host_port_ipv4(27017)
        .await
        .map_err(|e| E2EError::Setup(e.to_string()))?;
    let client = Client::with_uri_str(format!("mongodb://localhost:{port}"))
        .await
        .map_err(|e| E2EError::Setup(e.to_string()))?;
    let db = "e2e-tests";

    // deploy_reports: the NON-partial {device_id, revision_id, created_at, _id}
    // index the snapshot / RunState-list reads rely on. If this is missing (or
    // reverts to being partial on `kind.type`), the unfiltered snapshot query
    // can't use it and falls back to a scan + in-memory sort.
    let deploy_reports = index_names(&client, db, "deploy_reports").await?;
    assert!(
        deploy_reports
            .iter()
            .any(|n| n == "device_id_1_revision_id_1_created_at_-1__id_-1"),
        "deploy_reports is missing the non-partial created_at index; have: {deploy_reports:?}"
    );

    // deploy_revisions: `get_by_revision_id` filters `revision.id` alone.
    let deploy_revisions = index_names(&client, db, "deploy_revisions").await?;
    assert!(
        deploy_revisions.iter().any(|n| n == "revision.id_1"),
        "deploy_revisions is missing the revision.id index; have: {deploy_revisions:?}"
    );

    // audit_logs: `list_for_device` filters device_id, sorts timestamp desc.
    let audit_logs = index_names(&client, db, "audit_logs").await?;
    assert!(
        audit_logs.iter().any(|n| n == "device_id_1_timestamp_-1"),
        "audit_logs is missing the {{device_id, timestamp}} index; have: {audit_logs:?}"
    );

    // users: lookups by email (`{email}` and `{email: {$in}}`).
    let users = index_names(&client, db, "users").await?;
    assert!(
        users.iter().any(|n| n == "email_1"),
        "users is missing the email index; have: {users:?}"
    );

    tracing::info!("All expected MongoDB indexes are present");
    Ok(())
}
