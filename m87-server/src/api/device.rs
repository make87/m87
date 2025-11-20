use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::Request;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use tokio::io::AsyncWriteExt;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use tracing::warn;

use crate::auth::claims::Claims;
use crate::auth::tunnel_token::issue_tunnel_token;
use crate::models::device::{
    DeviceDoc, HeartbeatRequest, HeartbeatResponse, PublicDevice, UpdateDeviceBody,
};
use crate::models::forward::ForwardDoc;
use crate::models::roles::Role;
use crate::relay::relay_state::RelayState;
use crate::response::{
    ResponsePagination, ServerAppResult, ServerError, ServerResponse, ServerResult,
};
use crate::util::app_state::AppState;
use crate::util::pagination::RequestPagination;
use m87_shared::forward::{CreateForward, ForwardUpdateRequest, PublicForward};

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
        .route("/{id}/token", get(get_tunnel_token))
        .route("/{id}/forward", get(get_forwards).post(create_forward))
        .route(
            "/{id}/forward/{target_port}",
            get(get_forward).delete(delete_forward).post(update_forward),
        )
        .route("/{short-id}/proxy{*path}", any(proxy_device_http))
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

    let mut devices = DeviceDoc::to_public_devices(devices);
    // for each check if state.relay.has_tunnel
    for device in &mut devices {
        if state.relay.has_tunnel(&device.id).await {
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
    if state.relay.has_tunnel(&id).await {
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

async fn get_forwards(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    pagination: RequestPagination,
) -> ServerAppResult<Vec<PublicForward>> {
    let device = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Viewer,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let forwards = ForwardDoc::list_for_device(&state.db, &device.id.unwrap(), &pagination).await?;

    let count = forwards.len() as u64;
    let res: Vec<PublicForward> = forwards.into_iter().map(Into::into).collect();
    Ok(ServerResponse::builder()
        .body(res)
        .pagination(ResponsePagination {
            count,
            offset: pagination.offset,
            limit: pagination.limit,
        })
        .ok()
        .build())
}

async fn create_forward(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<CreateForward>,
) -> ServerAppResult<PublicForward> {
    let _ = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let updated = ForwardDoc::create_or_update(&state.db, body).await?;
    let res: PublicForward = updated.into();

    Ok(ServerResponse::builder().body(res).ok().build())
}

async fn get_forward(
    claims: Claims,
    State(state): State<AppState>,
    Path((id, target_port)): Path<(String, u16)>,
) -> ServerAppResult<PublicForward> {
    let device_id = ObjectId::parse_str(&id)?;
    let _ = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": &device_id },
            Role::Viewer,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let forward = ForwardDoc::get_by_port(&state.db, &device_id, target_port).await?;
    if forward.is_none() {
        return Err(ServerError::not_found("Forward not found"));
    }

    Ok(ServerResponse::builder()
        .body(forward.unwrap().into())
        .ok()
        .build())
}

async fn delete_forward(
    claims: Claims,
    State(state): State<AppState>,
    Path((id, target_port)): Path<(String, u16)>,
) -> ServerAppResult<()> {
    let device = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    ForwardDoc::delete(&state.db, &device.id.unwrap(), target_port).await?;

    Ok(ServerResponse::builder().body(()).ok().build())
}

async fn update_forward(
    claims: Claims,
    State(state): State<AppState>,
    Path((id, target_port)): Path<(String, u16)>,
    Json(body): Json<ForwardUpdateRequest>,
) -> ServerAppResult<()> {
    let device = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let _ = ForwardDoc::update(&state.db, &device.id.unwrap(), target_port, body).await?;

    Ok(ServerResponse::builder().ok().build())
}

pub async fn proxy_device_http(
    claims: Claims,
    State(state): State<AppState>,
    Path(short_id): Path<String>,
    req: Request<Body>,
) -> ServerAppResult<()> {
    // --- 1. Auth check ---
    let device = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "short_id": &short_id },
            Role::Viewer,
        )
        .await
        .and_then(|opt| opt.ok_or_else(|| ServerError::not_found("device not found")))?;

    // Spawn a task to handle upgraded connection
    let relay = state.relay.clone();
    tokio::spawn(async move {
        match hyper::upgrade::on(req).await {
            Ok(upgraded) => {
                let mut client_io = TokioIo::new(upgraded);
                if let Err(e) =
                    handle_upgraded_proxy(&mut client_io, relay, device.id.unwrap().to_string())
                        .await
                {
                    warn!("proxy upgrade failed: {e:?}");
                }
            }
            Err(e) => warn!("upgrade failed: {e:?}"),
        }
    });

    Ok(ServerResponse::builder().switching_protocols().build())
}

// --- internal handler for upgraded I/O ---

async fn handle_upgraded_proxy(
    client_io: &mut TokioIo<Upgraded>,
    relay: Arc<RelayState>,
    device_id: String,
) -> ServerResult<()> {
    let Some(conn_arc) = relay.get_tunnel(&device_id).await else {
        warn!("no active tunnel for {device_id}");
        return Ok(());
    };

    let mut sess = conn_arc.lock().await;
    let mut sub = sess
        .open_stream()
        .await
        .map_err(|_| ServerError::internal_error("Failed to open stream"))?;

    let header = b"80\n";
    sub.write_all(header).await?;

    tokio::io::copy_bidirectional(client_io, &mut sub).await?;
    Ok(())
}
