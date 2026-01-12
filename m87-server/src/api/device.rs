use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use m87_shared::device::{AuditLog, DeviceStatus};
use m87_shared::roles::Role;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;

use crate::api::deploy_spec::create_route as deploy_spec_route;
use crate::auth::claims::Claims;
use crate::models::audit_logs::AuditLogDoc;
use crate::models::device::{DeviceDoc, PublicDevice, UpdateDeviceBody};
use crate::response::{ResponsePagination, ServerAppResult, ServerError, ServerResponse};
use crate::util::app_state::AppState;
use crate::util::pagination::RequestPagination;

pub fn create_route() -> Router<AppState> {
    Router::new()
        .route("/", get(get_devices))
        .route(
            "/{id}",
            get(get_device_by_id)
                .post(update_device_by_id)
                .delete(delete_device),
        )
        .route("/{id}/status", get(get_device_status))
        .route("/{id}/audit_logs", get(get_audit_logs_by_device_id))
        .merge(deploy_spec_route())
}

async fn get_devices(
    claims: Claims,
    State(state): State<AppState>,
    pagination: RequestPagination,
) -> ServerAppResult<Vec<PublicDevice>> {
    let devices_col = state.db.devices();
    let devices = claims.list_with_access(&devices_col, &pagination).await?;
    let total_count = claims.count_with_access(&devices_col).await?;

    let mut devices = DeviceDoc::to_public_devices(devices);
    // for each check if state.relay.has_tunnel
    for device in &mut devices {
        if state.relay.has_tunnel(&device.short_id).await {
            device.online = true;
        }
    }

    Ok(ServerResponse::builder()
        .body(devices)
        .status_code(axum::http::StatusCode::OK)
        .pagination(ResponsePagination {
            count: total_count,
            offset: pagination.offset,
            limit: pagination.limit,
        })
        .build())
}

async fn get_device_by_id(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<PublicDevice> {
    let device_id =
        ObjectId::parse_str(&id).map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    let device_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": device_id })
        .await?;
    let device = device_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;
    let mut pub_device: PublicDevice = device.into();
    if state.relay.has_tunnel(&pub_device.short_id).await {
        pub_device.online = true;
    }

    Ok(ServerResponse::builder()
        .body(pub_device)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn update_device_by_id(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<UpdateDeviceBody>,
) -> ServerAppResult<PublicDevice> {
    let device_id =
        ObjectId::parse_str(&id).map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    // Build the Mongo update document
    let update_doc = payload.to_update_doc(); // implement this helper on UpdateDeviceBody

    // Execute authorized update
    claims
        .update_one_with_access(&state.db.devices(), doc! { "_id": device_id }, update_doc)
        .await?;

    // Fetch the updated device (using the same access filter)
    let updated_device_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": device_id })
        .await?;

    let updated_device = match updated_device_opt {
        Some(device) => device,
        None => return Err(ServerError::not_found("Device not found after update")),
    };

    Ok(ServerResponse::builder()
        .body(updated_device.into())
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn delete_device(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<()> {
    let device_oid = ObjectId::parse_str(&id)?;
    let device_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": device_oid })
        .await?;
    let device = device_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    let _ = device.remove_device(&claims, &state.db).await?;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}

async fn get_audit_logs_by_device_id(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    pagination: RequestPagination,
) -> ServerAppResult<Vec<AuditLog>> {
    let device_oid = ObjectId::parse_str(&id)?;
    let device_opt = claims
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": &device_oid },
            Role::Admin,
        )
        .await?;
    let _ = device_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    let audit_logs = AuditLogDoc::list_for_device(&state.db, device_oid, &pagination).await?;
    let audit_logs: Vec<AuditLog> = audit_logs.iter().map(|log| log.to_audit_log()).collect();

    Ok(ServerResponse::builder()
        .body(audit_logs)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn get_device_status(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    // Query(since): Query<Option<u32>>,
) -> ServerAppResult<DeviceStatus> {
    let device_oid = ObjectId::parse_str(&id)?;
    let device_opt = claims
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": &device_oid },
            Role::Editor,
        )
        .await?;
    let device = device_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    let status = device.get_status(&state.db, 0).await?;

    Ok(ServerResponse::builder()
        .body(status)
        .status_code(axum::http::StatusCode::OK)
        .build())
}
