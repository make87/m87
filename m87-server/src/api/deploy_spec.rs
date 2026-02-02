use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use m87_shared::deploy_spec::{
    CreateDeployRevisionBody, DeployReport, DeploymentRevision, DeploymentStatusSnapshot,
    UpdateDeployRevisionBody,
};
use mongodb::bson::{doc, oid::ObjectId};

use crate::auth::claims::Claims;
use crate::models::audit_logs::AuditLogDoc;
use crate::models::deploy_spec::{
    DeployReportDoc, DeployRevisionDoc, to_report_delete_doc, to_update_doc,
};
use crate::models::device::DeviceDoc;
use crate::response::{ResponsePagination, ServerAppResult, ServerError, ServerResponse};
use crate::util::app_state::AppState;
use crate::util::pagination::RequestPagination;

pub fn create_route() -> Router<AppState> {
    // This router is mounted under /devices already.
    Router::new()
        // Revisions
        .route(
            "/{device_id}/revisions",
            get(list_device_revisions).post(create_device_revision),
        )
        .route(
            "/{device_id}/revisions/{id}",
            get(get_revision_by_id)
                .post(update_revision_by_id)
                .delete(delete_revision),
        )
        .route(
            "/{device_id}/revisions/active",
            get(get_device_active_revision_id),
        )
        .route(
            "/{device_id}/revisions/{revision_id}/reports",
            get(list_device_revision_reports),
        )
        //get deployment snapshot
        .route(
            "/{device_id}/revisions/{revision_id}/snapshot",
            get(get_device_revision_snapshot),
        )
}

async fn list_device_revisions(
    claims: Claims,
    State(state): State<AppState>,
    Path(device_id): Path<String>,
    pagination: RequestPagination,
) -> ServerAppResult<Vec<DeploymentRevision>> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    // Ensure caller can access the device
    let dev_opt = claims
        .find_one_with_access(
            &state.db.deploy_revisions(),
            doc! { "device_id": device_oid },
        )
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    let docs = DeployRevisionDoc::list_for_device(&state.db, device_oid, &pagination).await?;
    let total_count = docs.len() as u64;
    let out: Vec<DeploymentRevision> = docs.into_iter().map(|doc| doc.revision).collect();

    Ok(ServerResponse::builder()
        .body(out)
        .status_code(axum::http::StatusCode::OK)
        .pagination(ResponsePagination {
            count: total_count,
            offset: pagination.offset,
            limit: pagination.limit,
        })
        .build())
}

async fn get_device_active_revision_id(
    claims: Claims,
    State(state): State<AppState>,
    Path(device_id): Path<String>,
) -> ServerAppResult<Option<String>> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    // Ensure caller can access the device
    let dev_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": device_oid })
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    let out = DeployRevisionDoc::get_active_device_deployment(&state.db, device_oid).await?;

    Ok(ServerResponse::builder()
        .body(out.map(|d| d.revision.id.unwrap()))
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn create_device_revision(
    claims: Claims,
    State(state): State<AppState>,
    Path(device_id): Path<String>,
    Json(payload): Json<CreateDeployRevisionBody>,
) -> ServerAppResult<DeploymentRevision> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Requested deployment revision creation for {}", &device_oid),
        &format!("{}", &payload),
        Some(device_oid.clone()),
    )
    .await;

    // Ensure caller can access the device
    let dev_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": device_oid })
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }
    let device = dev_opt.unwrap();

    let revision: m87_shared::deploy_spec::DeploymentRevision =
        m87_shared::deploy_spec::DeploymentRevision::from_yaml(&payload.revision)
            .map_err(|e| ServerError::internal_error(&format!("{:?}", e)))?;

    let doc = DeployRevisionDoc::create(
        &state.db,
        revision,
        Some(device_oid),
        None,
        payload.active.unwrap_or(true),
        device.owner_scope,
        device.allowed_scopes,
    )
    .await?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Added deployment revision for {}", &device_oid),
        &format!("{}", &doc.revision),
        Some(device_oid.clone()),
    )
    .await;

    Ok(ServerResponse::builder()
        .body(doc.revision)
        .status_code(axum::http::StatusCode::CREATED)
        .build())
}

async fn get_revision_by_id(
    claims: Claims,
    State(state): State<AppState>,
    Path((device_id, id)): Path<(String, String)>,
) -> ServerAppResult<DeploymentRevision> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;
    // Ensure caller can access device
    let doc_opt = claims
        .find_one_with_access(
            &state.db.deploy_revisions(),
            doc! { "revision.id": id, "device_id": device_oid},
        )
        .await?;
    if doc_opt.is_none() {
        return Err(ServerError::not_found("Deployment Revision not found"));
    }

    let doc = doc_opt.ok_or_else(|| ServerError::not_found("Revision not found"))?;
    Ok(ServerResponse::builder()
        .body(doc.revision)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn update_revision_by_id(
    claims: Claims,
    State(state): State<AppState>,
    Path((device_id, id)): Path<(String, String)>,
    Json(payload): Json<UpdateDeployRevisionBody>,
) -> ServerAppResult<()> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!(
            "Requested deployment revision update on {} for device {}",
            &id, &device_oid
        ),
        &format!("{}", &payload),
        Some(device_oid.clone()),
    )
    .await;

    let (update_doc, extra_filter) = to_update_doc(&payload)?;
    let report_delete_doc = to_report_delete_doc(&payload, &id, &device_oid)?;

    // if its an update with a new revision that is set as active. set the old active to false
    let set_inactive = match &payload.active {
        Some(true) => {
            let out =
                DeployRevisionDoc::get_active_device_deployment(&state.db, device_oid).await?;
            match out {
                Some(doc) => {
                    let filter = doc! { "revision.id": &doc.id, "device_id": &device_oid };
                    let update_doc = doc! { "active": false };
                    Some((filter, update_doc))
                }
                None => None,
            }
        }
        _ => None,
    };

    let mut filter = doc! { "revision.id": &id, "device_id": &device_oid };
    if let Some(extra) = extra_filter {
        filter.extend(extra);
    }
    let success = claims
        .update_one_with_access::<DeployRevisionDoc>(
            &state.db.deploy_revisions(),
            filter,
            update_doc,
        )
        .await?;

    if !success {
        return Err(ServerError::not_found("Revision not found"));
    }
    if let Some((filter, update_doc)) = set_inactive {
        let success = claims
            .update_one_with_access::<DeployRevisionDoc>(
                &state.db.deploy_revisions(),
                filter,
                update_doc,
            )
            .await?;
        if !success {
            // TODO: check if and how we might need to recover to a stable state
            return Err(ServerError::not_found("Revision not found"));
        }
    }

    // update device last_deployment_hash. Pesimistic update as we might update an inactive revision. TODO for later
    let _ = DeviceDoc::invalidate_deployment_hash(&state.db, &device_oid).await?;

    if let Some(delete_doc) = report_delete_doc {
        let res = state.db.deploy_reports().delete_many(delete_doc).await?;
        tracing::info!("Deleted {} deploy reports", res.deleted_count);
    }

    // if we removed a run_id also remove form current_run_states
    if let Some(run_id) = &payload.remove_run_spec_id {
        let _ = state
            .db
            .current_run_states()
            .delete_many(doc! { "revision_id": &id, "device_id": &device_oid, "run_id": run_id })
            .await?;
    }

    let latest_doc = state
        .db
        .deploy_revisions()
        .find_one(doc! { "revision.id": &id, "device_id": &device_oid })
        .await?;
    if let Some(doc) = latest_doc {
        let _ = AuditLogDoc::add(
            &state.db,
            &claims,
            &state.config,
            &format!(
                "Updated deployment revision {} for device {}",
                &id, &device_oid
            ),
            &format!("{}", &doc.revision),
            Some(device_oid.clone()),
        )
        .await;
    }

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}

async fn delete_revision(
    claims: Claims,
    State(state): State<AppState>,
    Path((device_id, id)): Path<(String, String)>,
) -> ServerAppResult<()> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!(
            "Requesting deployment revision deletion {} for device {}",
            &id, &device_oid
        ),
        "",
        Some(device_oid.clone()),
    )
    .await;

    // authorize by selecting first
    let success = claims
        .delete_one_with_access(
            &state.db.deploy_revisions(),
            doc! { "revision.id": &id, "device_id": &device_oid },
        )
        .await?;
    if !success {
        return Err(ServerError::not_found("Revision not found"));
    }

    // delete runstate with revision id
    let _ = state
        .db
        .current_run_states()
        .delete_many(doc! { "revision.id": &id, "device_id": &device_oid })
        .await?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!(
            "Deleted deployment revision {} for device {}",
            &id, &device_oid
        ),
        "",
        Some(device_oid.clone()),
    )
    .await;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}

async fn list_device_revision_reports(
    claims: Claims,
    State(state): State<AppState>,
    Path((device_id, revision_id)): Path<(String, String)>,
    pagination: RequestPagination,
) -> ServerAppResult<Vec<DeployReport>> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    // Ensure caller can access the device
    let dev_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": &device_oid })
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    // limit to max 5
    let mut page = pagination.clone();
    page.limit = page.limit.min(5);

    let docs =
        DeployReportDoc::list_for_device(&state.db, &device_oid, &revision_id, &page).await?;
    let reports: Vec<DeployReport> = docs.into_iter().map(|doc| doc.to_pub_report()).collect();
    let total_count = reports.len() as u64;

    Ok(ServerResponse::builder()
        .body(reports)
        .status_code(axum::http::StatusCode::OK)
        .pagination(ResponsePagination {
            count: total_count,
            offset: page.offset,
            limit: page.limit,
        })
        .build())
}

async fn get_device_revision_snapshot(
    claims: Claims,
    State(state): State<AppState>,
    Path((device_id, revision_id)): Path<(String, String)>,
) -> ServerAppResult<DeploymentStatusSnapshot> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    // Ensure caller can access the device
    let dev_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": &device_oid })
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    let snapshot = DeployReportDoc::compute_deployment_status_snapshot_for_device(
        &state.db,
        &device_oid,
        &revision_id,
    )
    .await?;

    Ok(ServerResponse::builder()
        .body(snapshot)
        .status_code(axum::http::StatusCode::OK)
        .build())
}
