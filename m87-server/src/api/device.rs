use axum::extract::{Path, State};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use m87_shared::device::{AddDeviceAccessBody, AuditLog, DeviceStatus};
use m87_shared::roles::Role;
use m87_shared::users::User;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;

use crate::api::deploy_spec::create_route as deploy_spec_route;
use crate::auth::claims::Claims;
use crate::models::audit_logs::AuditLogDoc;
use crate::models::device::{DeviceDoc, PublicDevice, UpdateDeviceBody};
use crate::models::org;
use crate::models::user::UserDoc;
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
        .route("/{id}/users", get(get_device_users))
        .route("/{id}/access", post(add_device_access))
        .route(
            "/{id}/access/{email_or_org_id}",
            delete(remove_device_access),
        )
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

    let mut device_map = Vec::new();
    for device in devices {
        let Ok(role) = claims.get_role(&device) else {
            continue;
        };
        device_map.push((device.clone(), role));
    }

    let mut devices = DeviceDoc::to_public_devices(device_map);
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
    let role = claims.get_role(&device)?;
    let mut pub_device: PublicDevice = device.to_public_device(&role);
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
    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Requested device update {}", &device_id),
        &format!("{}", &payload),
        Some(device_id.clone()),
    )
    .await;
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

    let role = claims.get_role(&updated_device)?;

    let pub_device = updated_device.to_public_device(&role);
    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Updated device {}", &device_id),
        &format!("{}", &pub_device),
        Some(device_id.clone()),
    )
    .await;

    Ok(ServerResponse::builder()
        .body(pub_device)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn delete_device(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<()> {
    let device_oid = ObjectId::parse_str(&id)?;
    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Requested device deletion {}", &device_oid),
        "",
        Some(device_oid.clone()),
    )
    .await;
    let device_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": device_oid })
        .await?;
    let device = device_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    let _ = device.remove_device(&claims, &state.db).await?;
    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Deleted device {}", &device_oid),
        "",
        Some(device_oid.clone()),
    )
    .await;
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

    let status = device.get_status(&state.db).await?;

    Ok(ServerResponse::builder()
        .body(status)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn get_device_users(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<Vec<User>> {
    let device_oid =
        ObjectId::parse_str(&id).map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    // Require Admin on this device
    let device_opt = claims
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": &device_oid },
            Role::Admin,
        )
        .await?;
    let device = device_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    // You need to implement this on DeviceDoc (see below).
    let users = device.list_users_with_access(&state.db).await?;

    Ok(ServerResponse::builder()
        .body(users)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn add_device_access(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<AddDeviceAccessBody>,
) -> ServerAppResult<()> {
    let device_oid =
        ObjectId::parse_str(&id).map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    // Require Admin on this device
    let device_opt = claims
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": &device_oid },
            Role::Admin,
        )
        .await?;
    let device = device_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    let requester_orgs = org::get_user_orgs(&state.db, &claims.user_email).await?;
    let user_orgs = org::get_user_orgs(&state.db, &payload.email_or_org_id).await?;
    let do_share_orgs = requester_orgs.iter().any(|org| user_orgs.contains(org));
    if !do_share_orgs && !state.config.allow_cros_org_device_sharing {
        return Err(ServerError::forbidden(
            "Cannot add member from different organization",
        ));
    }

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Requested add device access {}", &device_oid),
        &format!("{}", &payload),
        Some(device_oid.clone()),
    )
    .await;

    // check if e are owner otherwise check if we have at least the same role rank or higher
    let role = claims.get_role_for_scope(&device.owner_scope);
    let role = match role {
        Ok(role) => role,
        Err(_) => {
            let access_scope = DeviceDoc::create_device_scope(&id);
            claims.get_role_for_scope(&access_scope)?
        }
    };
    if payload.role.rank() > role.rank() {
        return Err(ServerError::forbidden("Cannot add member with higher role"));
    }

    // Implement on DeviceDoc (see below).
    device
        .add_or_update_device_access(&state.db, &payload.email_or_org_id, payload.role.clone())
        .await?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Added device access {}", &device_oid),
        &format!("{}", &payload),
        Some(device_oid.clone()),
    )
    .await;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}

async fn remove_device_access(
    claims: Claims,
    State(state): State<AppState>,
    Path((id, email_or_org_id)): Path<(String, String)>,
) -> ServerAppResult<()> {
    let device_oid =
        ObjectId::parse_str(&id).map_err(|_| ServerError::bad_request("Invalid ObjectId"))?;

    // Require Admin on this device
    let device_opt = claims
        .find_one_with_scope_and_role(
            &state.db.devices(),
            doc! { "_id": &device_oid },
            Role::Admin,
        )
        .await?;
    let device = device_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Requested remove device access {}", &device_oid),
        &format!("subject={}", &email_or_org_id),
        Some(device_oid.clone()),
    )
    .await;

    // Implement on DeviceDoc (see below).
    device
        .remove_device_access(&state.db, &email_or_org_id)
        .await?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        &format!("Removed device access {}", &device_oid),
        &format!("subject={}", &email_or_org_id),
        Some(device_oid.clone()),
    )
    .await;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}
