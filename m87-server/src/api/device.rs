use axum::extract::{Path, State};
use axum::routing::post;
use axum::{routing::get, Json, Router};
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;

use crate::auth::claims::Claims;
use crate::auth::tunnel_token::issue_tunnel_token;
use crate::models::device::{
    DeviceDoc, HeartbeatRequest, HeartbeatResponse, PublicDevice, UpdateDeviceBody,
};
use crate::models::roles::Role;
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
        .route("/{id}/heartbeat", post(post_heartbeat))
        .route("/{id}/logs", get(get_logs_websocket))
        .route("/{id}/terminal", get(get_terminal_websocket))
        .route("/{id}/metrics", get(get_metrics_websocket))
        .route("/{id}/ssh", get(get_device_ssh))
        // .route("/{id}/forward", get(get_port_forward))
        .route("/{id}/token", get(get_tunnel_token))
}

async fn get_tunnel_token(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<String> {
    // only return to the node itself
    if !claims.has_scope_and_role(&format!("device:{}", id), Role::Editor) {
        return Err(ServerError::unauthorized("missing token"));
    }
    // 30s ttl should be enough to open a tunnel
    let token = issue_tunnel_token(&id, 30, &state.config.forward_secret)?;
    Ok(ServerResponse::builder().ok().body(token).build())
}

async fn get_devices(
    claims: Claims,
    State(state): State<AppState>,
    pagination: RequestPagination,
) -> ServerAppResult<Vec<PublicDevice>> {
    let devices_col = state.db.devices();
    let devices = claims.list_with_access(&devices_col, &pagination).await?;
    let total_count = claims.count_with_access(&devices_col).await?;

    let devices = DeviceDoc::to_public_devices(devices);

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

    Ok(ServerResponse::builder()
        .body(device.into())
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

async fn post_heartbeat(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<HeartbeatRequest>,
) -> ServerAppResult<HeartbeatResponse> {
    let device = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let body = device.handle_heartbeat(claims, &state.db, payload).await?;
    let res = ServerResponse::builder().body(body).ok().build();
    Ok(res)
}

async fn get_device_ssh(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<String> {
    let device = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let command = device.request_ssh_command(&state).await?;
    let res = ServerResponse::builder().body(command).ok().build();
    Ok(res)
}

// async fn get_port_forward(
//     claims: Claims,
//     State(state): State<AppState>,
//     Path(id): Path<String>,
// ) -> ServerAppResult<String> {
//     let device = claims
//         .find_one_with_scope_and_role::<DeviceDoc>(
//             &state.db.devices(),
//             doc! { "_id": ObjectId::parse_str(&id)? },
//             Role::Editor,
//         )
//         .await?
//         .ok_or_else(|| ServerError::not_found("Device not found"))?;

//     let command = device.request_public_url(&state).await?;
//     let res = ServerResponse::builder().body(command).ok().build();
//     Ok(res)
// }

async fn get_logs_websocket(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<String> {
    let device = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let command = device.get_logs_url(None, &state).await?;
    let res = ServerResponse::builder().body(command).ok().build();
    Ok(res)
}

async fn get_terminal_websocket(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<String> {
    let device = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let command = device.get_terminal_url(None, &state).await?;
    let res = ServerResponse::builder().body(command).ok().build();
    Ok(res)
}

async fn get_metrics_websocket(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<String> {
    let device = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let command = device.get_metrics_url(None, &state).await?;
    let res = ServerResponse::builder().body(command).ok().build();
    Ok(res)
}
