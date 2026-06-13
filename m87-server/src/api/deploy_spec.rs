use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use m87_shared::deploy_spec::{
    CreateDeployRevisionBody, DeployReport, DeploymentRevision, DeploymentStatusSnapshot, JobRun,
    Lifecycle, LifecycleUpdate, TriggerJobBody, UpdateDeployRevisionBody,
};
use m87_shared::roles::Role;
use mongodb::bson::{doc, oid::ObjectId};
use serde::Deserialize;

use crate::auth::claims::Claims;
use crate::models::audit_logs::AuditLogDoc;
use crate::models::deploy_spec::{
    DeployReportDoc, DeployRevisionDoc, JobRunDoc, to_report_delete_doc, to_update_doc,
};
use crate::models::device::DeviceDoc;
use crate::response::{ResponsePagination, ServerAppResult, ServerError, ServerResponse};
use crate::util::app_state::AppState;
use crate::util::pagination::RequestPagination;

#[derive(Deserialize)]
struct LifecycleUpdateBody {
    pub lifecycle: Lifecycle,
}

#[derive(Deserialize, Default)]
struct JobRunsQuery {
    job_id: Option<String>,
}

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
            get(list_device_revision_run_states),
        )
        //get deployment snapshot
        .route(
            "/{device_id}/revisions/{revision_id}/snapshot",
            get(get_device_revision_snapshot),
        )
        .route(
            "/{device_id}/units/{unit_id}/lifecycle",
            axum::routing::post(update_unit_lifecycle),
        )
        .route(
            "/{device_id}/revisions/{revision_id}/jobs/{job_id}/trigger",
            axum::routing::post(trigger_job_run),
        )
        .route("/{device_id}/job-runs", axum::routing::get(list_job_runs))
        .route(
            "/{device_id}/job-runs/{run_id}",
            axum::routing::get(get_job_run),
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
        .find_one_with_access(&state.db.devices(), doc! { "_id": &device_oid })
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
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": device_oid },
            Role::Editor,
        )
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }
    let device = dev_opt.unwrap();

    let revision: m87_shared::deploy_spec::DeploymentRevision =
        m87_shared::deploy_spec::DeploymentRevision::from_yaml(&payload.revision)
            .map_err(|e| ServerError::internal_error(&format!("{:?}", e)))?;

    // Single-revision model: if a spec already exists for this device,
    // return it instead of creating a second one. The `payload.active`
    // field is accepted for wire compat but no longer meaningful — every
    // device has exactly one spec which is always the active one.
    if let Some(existing) =
        DeployRevisionDoc::get_active_device_deployment(&state.db, device_oid).await?
    {
        return Ok(ServerResponse::builder()
            .body(existing.revision)
            .status_code(axum::http::StatusCode::OK)
            .build());
    }

    let doc = DeployRevisionDoc::create(
        &state.db,
        revision,
        Some(device_oid),
        None,
        true,
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
    let dev_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": &device_oid })
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    let doc_opt = state
        .db
        .deploy_revisions()
        .find_one(doc! { "revision.id": id, "device_id": device_oid})
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

    // Handle lifecycle_update early – no revision document change needed
    if let Some(upd) = &payload.lifecycle_update {
        let dev_opt = claims
            .find_one_with_scope_and_role(
                &state.db.devices(),
                doc! { "_id": device_oid },
                Role::Editor,
            )
            .await?;
        if dev_opt.is_none() {
            return Err(ServerError::not_found("Device not found"));
        }
        DeviceDoc::push_lifecycle_update(&state.db, &device_oid, upd.clone()).await?;
        return Ok(ServerResponse::builder()
            .status_code(axum::http::StatusCode::NO_CONTENT)
            .build());
    }

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

    let dev_opt = claims
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": device_oid },
            Role::Editor,
        )
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    // Legacy rerun: trigger a new job run for the named job definition.
    if let Some(run_id) = &payload.rerun_run_spec_id {
        let rev_doc = state
            .db
            .deploy_revisions()
            .find_one(doc! { "revision.id": &id, "device_id": &device_oid })
            .await?
            .ok_or_else(|| ServerError::not_found("Revision not found"))?;

        if rev_doc.revision.get_job_by_id(run_id).is_some() {
            let device = dev_opt.unwrap();
            let _ = JobRunDoc::create(
                &state.db,
                device_oid,
                id.clone(),
                run_id.clone(),
                Default::default(),
                device.owner_scope,
                device.allowed_scopes,
            )
            .await?;
            let _ = DeviceDoc::invalidate_deployment_hash(&state.db, &device_oid).await?;
        }

        return Ok(ServerResponse::builder()
            .status_code(axum::http::StatusCode::NO_CONTENT)
            .build());
    }

    let (update_doc, extra_filter) = to_update_doc(&payload)?;
    let report_delete_doc = to_report_delete_doc(&payload, &id, &device_oid)?;

    // `payload.active` is accepted for wire compat but has no effect under
    // the single-revision model — there's nothing to switch away from.

    let mut filter = doc! { "revision.id": &id, "device_id": &device_oid };
    if let Some(extra) = extra_filter {
        filter.extend(extra);
    }

    let res = state
        .db
        .deploy_revisions()
        .update_one(filter, update_doc)
        .await?;

    if res.matched_count == 0 {
        return Err(ServerError::not_found("Revision not found"));
    }

    let _ = DeviceDoc::invalidate_deployment_hash(&state.db, &device_oid).await?;

    if let Some(delete_doc) = report_delete_doc {
        let res = state.db.deploy_reports().delete_many(delete_doc).await?;
        tracing::info!("Deleted {} deploy reports", res.deleted_count);
    }

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

    let dev_opt = claims
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": &device_oid },
            Role::Editor,
        )
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    // authorize by selecting first
    let res = state
        .db
        .deploy_revisions()
        .delete_one(doc! { "revision.id": &id, "device_id": &device_oid })
        .await?;
    if res.deleted_count == 0 {
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

async fn list_device_revision_run_states(
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

    // limit to max 50
    let mut page = pagination.clone();
    page.limit = page.limit.min(50);

    let docs =
        DeployReportDoc::list_run_states_for_device(&state.db, &device_oid, &revision_id, &page)
            .await?;
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

async fn update_unit_lifecycle(
    claims: Claims,
    State(state): State<AppState>,
    Path((device_id, unit_id)): Path<(String, String)>,
    Json(payload): Json<LifecycleUpdateBody>,
) -> ServerAppResult<()> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    let dev_opt = claims
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": device_oid },
            Role::Editor,
        )
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    let update = LifecycleUpdate {
        unit_id: unit_id.clone(),
        lifecycle: payload.lifecycle.clone(),
    };
    DeviceDoc::push_lifecycle_update(&state.db, &device_oid, update).await?;

    // Also persist the lifecycle to the active revision so the desired-state
    // view (`spec`/`units`/`health`) reflects the change. Without this,
    // `RunStatus.enabled` (computed from `revision.lifecycle` in
    // `compute_deployment_status_snapshot_for_device`) keeps reading the
    // original lifecycle from when the unit was deployed, and a `stop` would
    // never flip `enabled` to false on inspection.
    let lifecycle_bson =
        mongodb::bson::to_bson(&payload.lifecycle).map_err(|e| {
            ServerError::internal_error(&format!("lifecycle bson failed: {e}"))
        })?;
    let array_filter = doc! { "elem.id": &unit_id };
    let options = mongodb::options::UpdateOptions::builder()
        .array_filters(vec![array_filter])
        .build();
    for section in ["services", "observers", "jobs"] {
        let path = format!("revision.{section}.$[elem].lifecycle");
        let _ = state
            .db
            .deploy_revisions()
            .update_one(
                doc! { "device_id": &device_oid, "active": true },
                doc! { "$set": { path: &lifecycle_bson } },
            )
            .with_options(options.clone())
            .await?;
    }

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}

async fn trigger_job_run(
    claims: Claims,
    State(state): State<AppState>,
    Path((device_id, revision_id, job_id)): Path<(String, String, String)>,
    Json(payload): Json<TriggerJobBody>,
) -> ServerAppResult<JobRun> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    let dev_opt = claims
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": device_oid },
            Role::Editor,
        )
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }
    let device = dev_opt.unwrap();

    // Verify the job exists in the revision
    let revision_doc = state
        .db
        .deploy_revisions()
        .find_one(doc! { "revision.id": &revision_id, "device_id": &device_oid })
        .await?
        .ok_or_else(|| ServerError::not_found("Revision not found"))?;

    if revision_doc.revision.get_job_by_id(&job_id).is_none() {
        return Err(ServerError::not_found("Job not found in revision"));
    }

    let job_run = JobRunDoc::create(
        &state.db,
        device_oid,
        revision_id,
        job_id,
        payload.env_overrides,
        device.owner_scope,
        device.allowed_scopes,
    )
    .await?;

    DeviceDoc::invalidate_deployment_hash(&state.db, &device_oid).await?;

    Ok(ServerResponse::builder()
        .body(job_run.to_pub_job_run())
        .status_code(axum::http::StatusCode::CREATED)
        .build())
}

async fn list_job_runs(
    claims: Claims,
    State(state): State<AppState>,
    Path(device_id): Path<String>,
    Query(params): Query<JobRunsQuery>,
) -> ServerAppResult<Vec<JobRun>> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    let dev_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": &device_oid })
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    let docs = JobRunDoc::list_for_device(&state.db, &device_oid, params.job_id.as_deref()).await?;
    let runs: Vec<JobRun> = docs.into_iter().map(|d| d.to_pub_job_run()).collect();

    Ok(ServerResponse::builder()
        .body(runs)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn get_job_run(
    claims: Claims,
    State(state): State<AppState>,
    Path((device_id, run_id)): Path<(String, String)>,
) -> ServerAppResult<JobRun> {
    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    let dev_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": &device_oid })
        .await?;
    if dev_opt.is_none() {
        return Err(ServerError::not_found("Device not found"));
    }

    let doc = JobRunDoc::get_by_run_id(&state.db, &device_oid, &run_id)
        .await?
        .ok_or_else(|| ServerError::not_found("Job run not found"))?;

    Ok(ServerResponse::builder()
        .body(doc.to_pub_job_run())
        .status_code(axum::http::StatusCode::OK)
        .build())
}

// `rollback_device` was removed alongside the multi-revision model.
// Each device has a single in-place spec; redeploy is the way to revert.
