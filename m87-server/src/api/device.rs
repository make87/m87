use axum::extract::{Path, State};
use axum::routing::post;
use axum::{routing::get, Json, Router};
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;

use crate::auth::claims::Claims;
use crate::auth::tunnel_token::issue_tunnel_token;
use crate::models::device::{
    device_doc_to_public, device_docs_to_public, DeviceDoc, HeartbeatRequest, HeartbeatResponse,
    PublicDevice, UpdateDeviceBody,
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
    let nodes_col = state.db.devices();
    let nodes = claims.list_with_access(&nodes_col, &pagination).await?;
    let total_count = claims.count_with_access(&nodes_col).await?;

    let nodes = device_docs_to_public(&nodes);

    Ok(ServerResponse::builder()
        .body(nodes)
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

    let node_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": device_id })
        .await?;
    let node = node_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    let node = device_doc_to_public(&node);

    Ok(ServerResponse::builder()
        .body(node)
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

    // Fetch the updated node (using the same access filter)
    let updated_node_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": device_id })
        .await?;

    let updated_node = match updated_node_opt {
        Some(node) => node,
        None => return Err(ServerError::not_found("Device not found after update")),
    };

    let node = device_doc_to_public(&updated_node);

    Ok(ServerResponse::builder()
        .body(node)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn delete_device(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<()> {
    let node_oid = ObjectId::parse_str(&id)?;
    let node_opt = claims
        .find_one_with_access(&state.db.devices(), doc! { "_id": node_oid })
        .await?;
    let node = node_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    let _ = node.remove_device(&claims, &state.db).await?;

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
    let node = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let body = node.handle_heartbeat(claims, &state.db, payload).await?;
    let res = ServerResponse::builder().body(body).ok().build();
    Ok(res)
}

async fn get_device_ssh(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<String> {
    let node = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let command = node.request_ssh_command(&state).await?;
    let res = ServerResponse::builder().body(command).ok().build();
    Ok(res)
}

// async fn get_port_forward(
//     claims: Claims,
//     State(state): State<AppState>,
//     Path(id): Path<String>,
// ) -> ServerAppResult<String> {
//     let node = claims
//         .find_one_with_scope_and_role::<DeviceDoc>(
//             &state.db.devices(),
//             doc! { "_id": ObjectId::parse_str(&id)? },
//             Role::Editor,
//         )
//         .await?
//         .ok_or_else(|| ServerError::not_found("Device not found"))?;

//     let command = node.request_public_url(&state).await?;
//     let res = ServerResponse::builder().body(command).ok().build();
//     Ok(res)
// }

async fn get_logs_websocket(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<String> {
    let node = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let command = node.get_logs_url(None, &state).await?;
    let res = ServerResponse::builder().body(command).ok().build();
    Ok(res)
}

async fn get_terminal_websocket(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<String> {
    let node = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let command = node.get_terminal_url(None, &state).await?;
    let res = ServerResponse::builder().body(command).ok().build();
    Ok(res)
}

async fn get_metrics_websocket(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<String> {
    let node = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    let command = node.get_metrics_url(None, &state).await?;
    let res = ServerResponse::builder().body(command).ok().build();
    Ok(res)
}
