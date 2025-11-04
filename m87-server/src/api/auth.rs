use axum::extract::{Path, State};
use axum::{
    routing::{get, post},
    Json, Router,
};
use mongodb::bson::doc;
use tokio::join;

use crate::auth::claims::Claims;
use crate::models::device::{DeviceDoc, CreateDeviceBody};
use crate::models::device_auth_request::{
    DeviceAuthRequestBody, DeviceAuthRequestCheckResponse, DeviceAuthRequestDoc, AuthRequestAction,
    CheckAuthRequest, PublicDeviceAuthRequest,
};
use crate::models::api_key::{ApiKeyDoc, CreateApiKey};
use crate::models::roles::Role;
use crate::models::ssh_key::{SSHPubKeyCreateRequest, SSHPubKeyDoc};
use crate::response::{ResponsePagination, ServerAppResult, ServerError, ServerResponse};
use crate::util::app_state::AppState;
use crate::util::pagination::RequestPagination;

pub fn create_route() -> Router<AppState> {
    Router::new()
        .route("/request", get(get_auth_requests).post(post_auth_request))
        .route("/request/check", post(check_auth_request))
        .route("/request/approve", post(handle_auth_request))
        .route("/ssh", post(add_ssh_pub_key).get(get_ssh_keys))
}

async fn post_auth_request(
    State(state): State<AppState>,
    Json(payload): Json<DeviceAuthRequestBody>,
) -> ServerAppResult<String> {
    let request_id = DeviceAuthRequestDoc::create(&state.db, payload).await?;
    Ok(ServerResponse::builder()
        .body(request_id)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn get_auth_requests(
    claims: Claims,
    State(state): State<AppState>,
    pagination: RequestPagination,
) -> ServerAppResult<Vec<PublicDeviceAuthRequest>> {
    let devices_col = state.db.device_auth_requests();
    let devices_fut = claims.list_with_access(&devices_col, &pagination);
    let count_fut = claims.count_with_access(&devices_col);

    let (devices_res, count_res) = join!(devices_fut, count_fut);

    let devices = devices_res?;
    let total_count = count_res?;
    let devices = PublicDeviceAuthRequest::from_vec(devices);

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

async fn check_auth_request(
    State(state): State<AppState>,
    Json(payload): Json<CheckAuthRequest>,
) -> ServerAppResult<DeviceAuthRequestCheckResponse> {
    let requests_col = state.db.device_auth_requests();

    let request = requests_col
        .find_one(doc! { "request_id": &payload.request_id })
        .await
        .map_err(|_| ServerError::internal_error("DB lookup failed"))?;

    if request.is_none() {
        return Err(ServerError::not_found("Auth request not found"));
    }

    let request = request.unwrap();

    // if request not yet approved, return pending
    if !request.approved {
        return Ok(ServerResponse::builder()
            .body(DeviceAuthRequestCheckResponse {
                state: "pending".to_string(),
                api_key: None,
            })
            .status_code(axum::http::StatusCode::OK)
            .build());
    }

    // Delete the request now that it's processed
    let _ = requests_col
        .delete_one(doc! { "request_id": &payload.request_id })
        .await
        .map_err(|_| ServerError::internal_error("Failed to delete request"))?;

    // split owner_scope by : and take second part as owner_id
    let owner_id = request.owner_scope.split(':').nth(1).unwrap().to_string();

    let (api_key_doc, api_key) = ApiKeyDoc::create(
        &state.db,
        CreateApiKey {
            name: format!("{}-device", request.hostname),
            ttl_secs: None, // for now never expire
            scopes: vec![
                format!("device:{}", request.device_id.clone()),
                // grant access to all the owners pub ssh keys
                format!("ssh:{}", owner_id),
            ],
        },
    )
    .await?;

    // request approved -> create device + API key, then delete request
    let _ = DeviceDoc::create_from(
        &state.db,
        CreateDeviceBody {
            id: Some(request.device_id.clone()),
            name: request.hostname.clone(),
            owner_scope: request.owner_scope.clone(),
            allowed_scopes: vec![],
            target_client_version: Some("latest".to_string()),
            api_key_id: api_key_doc.id.clone().unwrap(),
        },
    )
    .await?;

    Ok(ServerResponse::builder()
        .body(DeviceAuthRequestCheckResponse {
            state: "approved".to_string(),
            api_key: Some(api_key),
        })
        .ok()
        .build())
}

async fn handle_auth_request(
    claims: Claims,
    State(state): State<AppState>,
    Json(payload): Json<AuthRequestAction>,
) -> ServerAppResult<()> {
    let requests_col = state.db.device_auth_requests();

    let _ = claims
        .find_one_with_access(&requests_col, doc! { "request_id": &payload.request_id })
        .await?
        .ok_or_else(|| ServerError::not_found("Auth request not found"))?;

    match payload.accept {
        true => {
            // Update request to mark as approved
            claims
                .update_one_with_access(
                    &requests_col,
                    doc! { "request_id": &payload.request_id },
                    doc! { "$set": { "approved": true } },
                )
                .await?;
            Ok(ServerResponse::builder().ok().build())
        }
        false => {
            // Delete or mark declined
            claims
                .delete_one_with_access(&requests_col, doc! { "request_id": &payload.request_id })
                .await?;
            Ok(ServerResponse::builder().ok().build())
        }
    }
}

async fn add_ssh_pub_key(
    claims: Claims,
    State(state): State<AppState>,
    Json(payload): Json<SSHPubKeyCreateRequest>,
) -> ServerAppResult<()> {
    if !claims.has_scope_and_role(&payload.owner_scope, Role::Admin) {
        return Err(ServerError::forbidden(
            "You don't have admin role in this scope",
        ));
    }

    let _ = SSHPubKeyDoc::create(&state.db, payload).await?;

    Ok(ServerResponse::builder().ok().build())
}

async fn get_ssh_keys(
    claims: Claims,
    State(state): State<AppState>,
    pagination: RequestPagination,
) -> ServerAppResult<Vec<SSHPubKeyDoc>> {
    let col = state.db.ssh_keys();

    let list_fut = claims.list_with_access(&col, &pagination);
    let count_fut = claims.count_with_access(&col);

    let (list_res, count_res) = join!(list_fut, count_fut);

    let ssh_keys = list_res?;
    let total_count = count_res?;

    Ok(ServerResponse::builder()
        .body(ssh_keys)
        .pagination(ResponsePagination {
            count: total_count,
            offset: pagination.offset,
            limit: pagination.limit,
        })
        .ok()
        .build())
}
