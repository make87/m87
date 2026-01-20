use axum::extract::{Path, State};
use axum::routing::{delete, get};
use axum::{Json, Router};

use mongodb::bson::{doc, oid::ObjectId};

use m87_shared::device::PublicDevice;
use m87_shared::org::{
    AddDeviceBody, CreateOrganizationBody, InviteMemberBody, Organization, UpdateOrganizationBody,
};
use m87_shared::roles::Role;
use m87_shared::users::User;

use crate::auth::claims::Claims;
use crate::models::audit_logs::AuditLogDoc;
use crate::models::device::DeviceDoc;
use crate::models::org;
use crate::models::roles::{CreateRoleBinding, RoleDoc};
use crate::models::user::UserDoc;
use crate::response::{ServerAppResult, ServerError, ServerResponse};
use crate::util::app_state::AppState;

pub fn create_route() -> Router<AppState> {
    Router::new()
        .route("/", get(list_organizations).post(create_organization))
        .route(
            "/{id}",
            delete(delete_organization).put(update_organization),
        )
        .route(
            "/{id}/members",
            get(list_organization_members).post(add_organization_member),
        )
        .route("/{id}/members/{member}", delete(remove_organization_member))
        .route("/{id}/devices", get(list_org_devices).post(add_org_device))
        .route("/{id}/devices/{device_id}", delete(remove_org_device))
}

async fn list_organizations(claims: Claims) -> ServerAppResult<Vec<Organization>> {
    let mut orgs = std::collections::BTreeMap::<String, Role>::new();

    for r in &claims.roles {
        if r.scope.starts_with("org:") && Role::allows(&r.role, &Role::Viewer) {
            if let Some(id) = r.scope.strip_prefix("org:") {
                orgs.insert(id.to_string(), r.role.clone());
            }
        }
    }

    Ok(ServerResponse::builder()
        .body(
            orgs.into_iter()
                .map(|(id, role)| Organization { id, role })
                .collect(),
        )
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn create_organization(
    claims: Claims,
    State(state): State<AppState>,
    Json(payload): Json<CreateOrganizationBody>,
) -> ServerAppResult<()> {
    if !claims.is_admin {
        return Err(ServerError::forbidden(
            "Only admins can create organizations",
        ));
    }

    let org_id = payload.id.trim();
    if org_id.is_empty() {
        return Err(ServerError::bad_request(
            "Organization id must not be empty",
        ));
    }
    if org_id.contains(':') {
        return Err(ServerError::bad_request(
            "Organization id must not contain ':'",
        ));
    }

    let scope = org::org_scope(org_id);

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Requested create organization",
        &format!("id={} owner_email={}", org_id, payload.owner_email),
        None,
    )
    .await;

    // check if its exists.
    //
    let body = CreateRoleBinding {
        reference_id: UserDoc::create_reference_id(&payload.owner_email),
        role: Role::Owner,
        scope: scope.clone(),
    };
    if RoleDoc::check_if_exists(&state.db, body.clone()).await? {
        return Err(ServerError::bad_request("Organization already exists"));
    }

    RoleDoc::create(&state.db, body).await?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Created organization",
        &format!("id={} owner_email={}", org_id, payload.owner_email),
        None,
    )
    .await;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn delete_organization(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<()> {
    let scope = org::org_scope(&id);
    if !claims.has_scope_and_role(&scope, Role::Admin) {
        return Err(ServerError::forbidden("Not authorized for organization"));
    }

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Requested delete organization",
        &format!("id={}", id),
        None,
    )
    .await;

    // 1) Delete all roles with this org scope (memberships, etc)
    state
        .db
        .roles()
        .delete_many(doc! { "scope": &scope })
        .await?;

    // 2) Delete all org->device bindings where reference_id = "org:<id>"
    state
        .db
        .roles()
        .delete_many(doc! { "reference_id": org::org_ref(&id) })
        .await?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Deleted organization",
        &format!("id={}", id),
        None,
    )
    .await;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}

async fn update_organization(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<UpdateOrganizationBody>,
) -> ServerAppResult<()> {
    let old_scope = org::org_scope(&id);
    if !claims.has_scope_and_role(&old_scope, Role::Admin) {
        return Err(ServerError::forbidden("Not authorized for organization"));
    }

    let new_id = payload.new_id.trim();
    if new_id.is_empty() {
        return Err(ServerError::bad_request("new_id must not be empty"));
    }
    if new_id.contains(':') {
        return Err(ServerError::bad_request("new_id must not contain ':'"));
    }

    let new_scope = org::org_scope(new_id);
    let old_ref = org::org_ref(&id);
    let new_ref = org::org_ref(new_id);

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Requested update organization",
        &format!("id={} new_id={}", id, new_id),
        None,
    )
    .await;

    // Rename org scope for all org-scoped role docs (memberships).
    state
        .db
        .roles()
        .update_many(
            doc! { "scope": &old_scope },
            doc! { "$set": { "scope": &new_scope } },
        )
        .await?;

    // Rename org reference id for all org->device role docs.
    state
        .db
        .roles()
        .update_many(
            doc! { "reference_id": &old_ref },
            doc! { "$set": { "reference_id": &new_ref } },
        )
        .await?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Updated organization",
        &format!("id={} new_id={}", id, new_id),
        None,
    )
    .await;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn list_organization_members(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<Vec<User>> {
    let scope = org::org_scope(&id);
    if !claims.has_scope_and_role(&scope, Role::Viewer) {
        return Err(ServerError::forbidden("Not authorized for organization"));
    }

    let users_out: Vec<User> = org::get_org_members(&state.db, vec![id.clone()]).await?;

    Ok(ServerResponse::builder()
        .body(users_out)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn add_organization_member(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<InviteMemberBody>,
) -> ServerAppResult<()> {
    let scope = org::org_scope(&id);
    if !claims.has_scope_and_role(&scope, Role::Admin) {
        return Err(ServerError::forbidden("Not authorized for organization"));
    }
    // check role has to be same or lower as user's role
    if payload.role.rank() > claims.get_role_for_scope(&scope)?.rank() {
        return Err(ServerError::forbidden("Cannot add member with higher role"));
    }

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Requested add org member",
        &format!("org={} email={} role={:?}", id, payload.email, payload.role),
        None,
    )
    .await;

    RoleDoc::create(
        &state.db,
        CreateRoleBinding {
            reference_id: UserDoc::create_reference_id(&payload.email),
            role: payload.role.clone(),
            scope,
        },
    )
    .await?;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn remove_organization_member(
    claims: Claims,
    State(state): State<AppState>,
    Path((id, email)): Path<(String, String)>,
) -> ServerAppResult<()> {
    let scope = org::org_scope(&id);
    // if admin or if users want to remove themselves
    if !claims.has_scope_and_role(&scope, Role::Admin) && email != claims.user_email {
        return Err(ServerError::forbidden("Not authorized for organization"));
    }

    if email.is_empty() {
        return Err(ServerError::bad_request("Could not resolve member email"));
    }

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Requested remove org member",
        &format!("org={} email={}", id, email),
        None,
    )
    .await;

    state
        .db
        .roles()
        .delete_one(doc! { "scope": &scope, "reference_id": UserDoc::create_reference_id(&email) })
        .await?;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn list_org_devices(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ServerAppResult<Vec<PublicDevice>> {
    let scope = org::org_scope(&id);
    if !claims.has_scope_and_role(&scope, Role::Viewer) {
        return Err(ServerError::forbidden("Not authorized for organization"));
    }

    let mut out: Vec<PublicDevice> = org::get_org_devices(&state.db, &id).await?;

    for d in &mut out {
        if state.relay.has_tunnel(&d.short_id).await {
            d.online = true;
        }
    }

    Ok(ServerResponse::builder()
        .body(out)
        .status_code(axum::http::StatusCode::OK)
        .build())
}

async fn add_org_device(
    claims: Claims,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<AddDeviceBody>,
) -> ServerAppResult<()> {
    let scope = org::org_scope(&id);
    if !claims.has_scope_and_role(&scope, Role::Admin) {
        return Err(ServerError::forbidden("Not authorized for organization"));
    }

    let device_oid = ObjectId::parse_str(&payload.device_id)
        .map_err(|_| ServerError::bad_request("Invalid device ObjectId"))?;

    // Validate device exists (clean error)
    let device_opt = state
        .db
        .devices()
        .find_one(doc! { "_id": &device_oid })
        .await?;
    let _ = device_opt.ok_or_else(|| ServerError::not_found("Device not found"))?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Requested add org device",
        &format!("org={} device_id={}", id, payload.device_id),
        Some(device_oid.clone()),
    )
    .await;

    RoleDoc::create(
        &state.db,
        CreateRoleBinding {
            reference_id: org::org_ref(&id),
            role: payload.role,
            scope: DeviceDoc::scope_for_device(&device_oid),
        },
    )
    .await?;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}

// --------------------
// DELETE /organizations/{id}/devices/{device_id}
// --------------------

async fn remove_org_device(
    claims: Claims,
    State(state): State<AppState>,
    Path((id, device_id)): Path<(String, String)>,
) -> ServerAppResult<()> {
    let scope = org::org_scope(&id);
    if !claims.has_scope_and_role(&scope, Role::Admin) {
        return Err(ServerError::forbidden("Not authorized for organization"));
    }

    let device_oid = ObjectId::parse_str(&device_id)
        .map_err(|_| ServerError::bad_request("Invalid device ObjectId"))?;

    let _ = AuditLogDoc::add(
        &state.db,
        &claims,
        &state.config,
        "Requested remove org device",
        &format!("org={} device_id={}", id, device_id),
        Some(device_oid.clone()),
    )
    .await;

    state
        .db
        .roles()
        .delete_one(doc! { "reference_id": org::org_ref(&id), "scope": DeviceDoc::scope_for_device(&device_oid) })
        .await?;

    Ok(ServerResponse::builder()
        .status_code(axum::http::StatusCode::NO_CONTENT)
        .build())
}
