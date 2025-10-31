use axum::extract::{Path, State};
use axum::routing::post;
use axum::{routing::get, Json, Router};
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use tokio::join;

use crate::auth::claims::Claims;
use crate::auth::tunnel_token::issue_tunnel_token;
use crate::models::node::{
    HeartbeatRequest, HeartbeatResponse, NodeDoc, PublicNode, UpdateNodeBody,
};
use crate::models::roles::Role;
use crate::response::{NexusAppResult, NexusError, NexusResponse, ResponsePagination};
use crate::util::app_state::AppState;
use crate::util::pagination::RequestPagination;

pub fn create_route() -> Router<AppState> {
    Router::new()
        .route("/", get(get_nodes))
        .route(
            "/{id}",
            get(get_node_by_id)
                .post(update_node_by_id)
                .delete(delete_node),
        )
        .route("/{id}/heartbeat", post(post_heartbeat))
        .route("/{id}/logs", get(get_logs_websocket))
        .route("/{id}/terminal", get(get_terminal_websocket))
        .route("/{id}/metrics", get(get_metrics_websocket))
        .route("/{id}/ssh", get(get_node_ssh))
        // .route("/{id}/forward", get(get_port_forward))
        .route("/{id}/token", get(get_tunnel_token))
}

async fn get_tunnel_token(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> NexusAppResult<String> {
    // only return to the node itself
    if !claims.has_scope_and_role(&format!("node:{}", id), Role::Editor) {
        return Err(NexusError::unauthorized("missing token"));
    }
    // 30s ttl should be enough to open a tunnel
    let token = issue_tunnel_token(&id, 30, &state.config.forward_secret)?;
    Ok(NexusResponse::builder().ok().body(token).build())
}

async fn get_nodes(
    claims: Claims,
    State(state): State<AppState>,
    pagination: RequestPagination,
) -> NexusAppResult<Vec<PublicNode>> {
    let nodes_col = state.db.nodes();
    let nodes_fut = claims.list_with_access(&nodes_col, &pagination);
    let count_fut = claims.count_with_access(&nodes_col);

    let (nodes_res, count_res) = join!(nodes_fut, count_fut);

    let nodes = nodes_res?;
    let total_count = count_res?;
    let nodes = PublicNode::from_nodes(&nodes);

    Ok(NexusResponse::builder()
        .body(nodes)
        .status_code(axum::http::StatusCode::OK)
        .pagination(ResponsePagination {
            count: total_count,
            offset: pagination.offset,
            limit: pagination.limit,
        })
        .build())
}

async fn get_node_by_id(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> NexusAppResult<PublicNode> {
    let node_id =
        ObjectId::parse_str(&id).map_err(|_| NexusError::bad_request("Invalid ObjectId"))?;

    let node_opt = claims
        .find_one_with_access(&state.db.nodes(), doc! { "_id": node_id })
        .await?;
    let node = node_opt.ok_or_else(|| NexusError::not_found("Node not found"))?;

    let node = PublicNode::from_node(&node);

    Ok(NexusResponse::builder()
        .body(node)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn update_node_by_id(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<UpdateNodeBody>,
) -> NexusAppResult<PublicNode> {
    let node_id =
        ObjectId::parse_str(&id).map_err(|_| NexusError::bad_request("Invalid ObjectId"))?;

    // Build the Mongo update document
    let update_doc = payload.to_update_doc(); // implement this helper on UpdateNodeBody

    // Execute authorized update
    claims
        .update_one_with_access(&state.db.nodes(), doc! { "_id": node_id }, update_doc)
        .await?;

    // Fetch the updated node (using the same access filter)
    let updated_node_opt = claims
        .find_one_with_access(&state.db.nodes(), doc! { "_id": node_id })
        .await?;

    let updated_node = match updated_node_opt {
        Some(node) => node,
        None => return Err(NexusError::not_found("Node not found after update")),
    };

    let node = PublicNode::from_node(&updated_node);

    Ok(NexusResponse::builder()
        .body(node)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn delete_node(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> NexusAppResult<()> {
    let node_oid = ObjectId::parse_str(&id)?;
    let node_opt = claims
        .find_one_with_access(&state.db.nodes(), doc! { "_id": node_oid })
        .await?;
    let node = node_opt.ok_or_else(|| NexusError::not_found("Node not found"))?;

    let _ = node.remove_node(&claims, &state.db).await?;

    Ok(NexusResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}

async fn post_heartbeat(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<HeartbeatRequest>,
) -> NexusAppResult<HeartbeatResponse> {
    let node = claims
        .find_one_with_scope_and_role::<NodeDoc>(
            &state.db.nodes(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| NexusError::not_found("Node not found"))?;

    let body = node.handle_heartbeat(claims, &state.db, payload).await?;
    let res = NexusResponse::builder().body(body).ok().build();
    Ok(res)
}

async fn get_node_ssh(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> NexusAppResult<String> {
    let node = claims
        .find_one_with_scope_and_role::<NodeDoc>(
            &state.db.nodes(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| NexusError::not_found("Node not found"))?;

    let command = node.request_ssh_command(&state).await?;
    let res = NexusResponse::builder().body(command).ok().build();
    Ok(res)
}

// async fn get_port_forward(
//     claims: Claims,
//     State(state): State<AppState>,
//     Path(id): Path<String>,
// ) -> NexusAppResult<String> {
//     let node = claims
//         .find_one_with_scope_and_role::<NodeDoc>(
//             &state.db.nodes(),
//             doc! { "_id": ObjectId::parse_str(&id)? },
//             Role::Editor,
//         )
//         .await?
//         .ok_or_else(|| NexusError::not_found("Node not found"))?;

//     let command = node.request_public_url(&state).await?;
//     let res = NexusResponse::builder().body(command).ok().build();
//     Ok(res)
// }

async fn get_logs_websocket(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> NexusAppResult<String> {
    let node = claims
        .find_one_with_scope_and_role::<NodeDoc>(
            &state.db.nodes(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| NexusError::not_found("Node not found"))?;

    let command = node.get_logs_url(None, &state).await?;
    let res = NexusResponse::builder().body(command).ok().build();
    Ok(res)
}

async fn get_terminal_websocket(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> NexusAppResult<String> {
    let node = claims
        .find_one_with_scope_and_role::<NodeDoc>(
            &state.db.nodes(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| NexusError::not_found("Node not found"))?;

    let command = node.get_terminal_url(None, &state).await?;
    let res = NexusResponse::builder().body(command).ok().build();
    Ok(res)
}

async fn get_metrics_websocket(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> NexusAppResult<String> {
    let node = claims
        .find_one_with_scope_and_role::<NodeDoc>(
            &state.db.nodes(),
            doc! { "_id": ObjectId::parse_str(&id)? },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| NexusError::not_found("Node not found"))?;

    let command = node.get_metrics_url(None, &state).await?;
    let res = NexusResponse::builder().body(command).ok().build();
    Ok(res)
}
