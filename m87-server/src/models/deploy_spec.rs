use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use futures::TryStreamExt;
use m87_shared::{
    deploy_spec::{
        BucketRow, BucketTotals, DeployReport, DeployReportKind, DeploymentRevision,
        DeploymentStatusSnapshot, FailureAggResponse, JobDef, JobRun, JobRunStatus,
        ObserveStatusItem, Outcome, RollbackStatus, RunStatus, ServiceSpec, SliceLevel, Step,
        StepAttemptStatus, StepState, StepStatus, UnitKind, UpdateDeployRevisionBody,
    },
    device::ObserveStatus,
};
use mongodb::{
    bson::{Bson, DateTime as BsonDateTime, Document, doc, oid::ObjectId, to_bson},
    error::{ErrorKind, WriteFailure},
    options::{FindOptions, UpdateOptions},
};
use serde::{Deserialize, Serialize};

use crate::{
    auth::access_control::AccessControlled,
    db::Mongo,
    response::{ServerError, ServerResult},
    util::pagination::RequestPagination,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployRevisionDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub revision: DeploymentRevision,
    #[serde(default)]
    pub device_id: Option<ObjectId>,
    // placeholder for later
    #[serde(default)]
    pub group_id: Option<ObjectId>,

    pub active: bool,
    pub dirty: bool,
    pub index: u32,

    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
}

impl AccessControlled for DeployRevisionDoc {
    fn owner_scope_field() -> &'static str {
        "owner_scope"
    }
    fn allowed_scopes_field() -> Option<&'static str> {
        Some("allowed_scopes")
    }
    fn owner_scope(&self) -> &str {
        &self.owner_scope
    }
    fn allowed_scopes(&self) -> Option<Vec<String>> {
        Some(self.allowed_scopes.clone())
    }
}

pub fn to_update_doc(
    body: &UpdateDeployRevisionBody,
) -> ServerResult<(Document, Option<Document>)> {
    let mut which = 0;
    if body.revision.is_some() {
        which += 1;
    }
    if body.add_service.is_some() {
        which += 1;
    }
    if body.add_observer.is_some() {
        which += 1;
    }
    if body.add_job.is_some() {
        which += 1;
    }
    if body.remove_unit_id.is_some() {
        which += 1;
    }
    if body.lifecycle_update.is_some() {
        which += 1;
    }
    if body.active.is_some() {
        which += 1;
    }
    // legacy fields
    if body.add_run_spec.is_some() {
        which += 1;
    }
    if body.update_run_spec.is_some() {
        which += 1;
    }
    if body.remove_run_spec_id.is_some() {
        which += 1;
    }

    if which == 0 {
        return Err(ServerError::bad_request("Missing fields"));
    }
    if which > 1 {
        return Err(ServerError::bad_request(
            "only one field may be set per update",
        ));
    }

    if let Some(yaml) = &body.revision {
        // DeploymentRevision::from_yaml ensures id is set on the server side
        let rev: DeploymentRevision = DeploymentRevision::from_yaml(yaml)
            .map_err(|e| ServerError::bad_request(&format!("invalid YAML in `revision`: {}", e)))?;
        return Ok((
            doc! { "$set": { "revision": to_bson(&rev).map_err(|e| ServerError::bad_request(&format!("revision -> bson failed: {}", e)))? } },
            None,
        ));
    }

    if let Some(yaml) = &body.add_service {
        let spec: ServiceSpec = ServiceSpec::from_yaml(yaml).map_err(|e| {
            ServerError::bad_request(&format!("invalid YAML in `add_service`: {}", e))
        })?;
        return Ok((
            doc! { "$push": { "revision.services": to_bson(&spec).map_err(|e| ServerError::bad_request(&format!("ServiceSpec -> bson failed: {}", e)))? } },
            None,
        ));
    }

    if let Some(yaml) = &body.add_observer {
        let spec: ServiceSpec = ServiceSpec::from_yaml(yaml).map_err(|e| {
            ServerError::bad_request(&format!("invalid YAML in `add_observer`: {}", e))
        })?;
        return Ok((
            doc! { "$push": { "revision.observers": to_bson(&spec).map_err(|e| ServerError::bad_request(&format!("ServiceSpec -> bson failed: {}", e)))? } },
            None,
        ));
    }

    if let Some(yaml) = &body.add_job {
        let job: JobDef = JobDef::from_yaml(yaml)
            .map_err(|e| ServerError::bad_request(&format!("invalid YAML in `add_job`: {}", e)))?;
        return Ok((
            doc! { "$push": { "revision.jobs": to_bson(&job).map_err(|e| ServerError::bad_request(&format!("JobDef -> bson failed: {}", e)))? } },
            None,
        ));
    }

    if let Some(id) = &body.remove_unit_id {
        // Remove from all three arrays by id
        return Ok((
            doc! {
                "$pull": {
                    "revision.services": { "id": id },
                    "revision.observers": { "id": id },
                    "revision.jobs": { "id": id },
                }
            },
            None,
        ));
    }

    if body.lifecycle_update.is_some() {
        // Lifecycle updates are delivered to the device via heartbeat.
        // No revision document change is needed here; return a no-op set.
        // TODO: persist lifecycle_update to a pending queue for heartbeat delivery.
        return Ok((doc! { "$set": { "dirty": true } }, None));
    }

    if let Some(active) = body.active {
        return Ok((doc! { "$set": { "active": active } }, None));
    }

    // Legacy backward-compat fields
    if let Some(yaml) = &body.add_run_spec {
        // Parse the type field to route to the correct collection.
        let raw: serde_yaml::Value = serde_yaml::from_str(yaml).map_err(|e| {
            ServerError::bad_request(&format!("invalid YAML in `add_run_spec`: {}", e))
        })?;

        let run_type = raw
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("service");

        match run_type {
            "observe" => {
                let spec: ServiceSpec = serde_yaml::from_str(yaml).map_err(|e| {
                    ServerError::bad_request(&format!(
                        "invalid YAML in `add_run_spec` (observer): {}",
                        e
                    ))
                })?;
                return Ok((
                    doc! { "$push": { "revision.observers": to_bson(&spec).map_err(|e| ServerError::bad_request(&e.to_string()))? } },
                    None,
                ));
            }
            "job" => {
                let jd: JobDef = serde_yaml::from_str(yaml).map_err(|e| {
                    ServerError::bad_request(&format!(
                        "invalid YAML in `add_run_spec` (job): {}",
                        e
                    ))
                })?;
                return Ok((
                    doc! { "$push": { "revision.jobs": to_bson(&jd).map_err(|e| ServerError::bad_request(&e.to_string()))? } },
                    None,
                ));
            }
            _ => {
                // Default: service
                let spec: ServiceSpec = serde_yaml::from_str(yaml).map_err(|e| {
                    ServerError::bad_request(&format!(
                        "invalid YAML in `add_run_spec` (service): {}",
                        e
                    ))
                })?;
                return Ok((
                    doc! { "$push": { "revision.services": to_bson(&spec).map_err(|e| ServerError::bad_request(&e.to_string()))? } },
                    None,
                ));
            }
        }
    }

    if body.update_run_spec.is_some() {
        return Err(ServerError::bad_request(
            "update_run_spec is not supported in this version",
        ));
    }

    if let Some(id) = &body.remove_run_spec_id {
        // Legacy: remove from jobs only (backward compat)
        return Ok((doc! { "$pull": { "revision.jobs": { "id": id } } }, None));
    }

    Err(ServerError::internal_error("This should be unreachable"))
}

pub fn to_report_delete_doc(
    body: &UpdateDeployRevisionBody,
    revision_id: &str,
    device_id: &ObjectId,
) -> ServerResult<Option<Document>> {
    let mut which = 0;
    if body.revision.is_some() {
        which += 1;
    }
    if body.remove_run_spec_id.is_some() {
        which += 1;
    }

    if which == 0 {
        return Ok(None);
    }
    if which > 1 {
        return Err(ServerError::bad_request(
            "only one field may be set per update",
        ));
    }

    if let Some(_) = &body.revision {
        return Ok(Some(
            doc! {"revision_id": revision_id, "device_id": device_id },
        ));
    }

    if let Some(id) = &body.remove_run_spec_id {
        return Ok(Some(
            doc! { "kind.data.run_id": id, "revision_id": revision_id, "device_id": device_id },
        ));
    }

    Err(ServerError::internal_error("This should be unreachable"))
}

impl DeployRevisionDoc {
    pub async fn create(
        db: &Arc<Mongo>,
        revision: DeploymentRevision,
        device_id: Option<ObjectId>,
        group_id: Option<ObjectId>,
        active: bool,
        owner_scope: String,
        allowed_scopes: Vec<String>,
    ) -> ServerResult<Self> {
        // index is the cnt of current docs for the dive or group
        let index = match (device_id, group_id) {
            (Some(device_id), _) => db
                .deploy_revisions()
                .count_documents(doc! {"device_id": device_id})
                .await
                .unwrap_or(0) as u32,
            (None, Some(group_id)) => db
                .deploy_revisions()
                .count_documents(doc! {"group_id": group_id})
                .await
                .unwrap_or(0) as u32,
            _ => {
                return Err(ServerError::bad_request(
                    "Either device_id or group_id must be provided",
                ));
            }
        };

        let doc = Self {
            id: None,
            revision,
            device_id,
            group_id,
            active,
            dirty: false,
            index,
            owner_scope,
            allowed_scopes,
        };
        db.deploy_revisions()
            .insert_one(&doc)
            .await
            .map_err(|_| ServerError::internal_error("Failed to insert API key"))?;
        Ok(doc)
    }

    pub async fn get_active_device_deployment(
        db: &Arc<Mongo>,
        device_id: ObjectId,
    ) -> ServerResult<Option<Self>> {
        let doc_opt = db
            .deploy_revisions()
            .find_one(doc! { "device_id": device_id, "active": true })
            .await?;

        match doc_opt {
            Some(d) => Ok(Some(d)),
            None => Ok(None),
        }
    }

    pub async fn get_active_devices_deployment_ids(
        db: &Arc<Mongo>,
        device_ids: &[ObjectId],
    ) -> ServerResult<Vec<(ObjectId, String)>> {
        let mut cursor = db
            .deploy_revisions()
            .find(doc! { "device_id": { "$in": device_ids }, "active": true })
            .await?;

        // create list of device_id, id tuples
        let mut tuples = Vec::new();
        while let Some(doc) = cursor.try_next().await? {
            if let Some(device_id) = doc.device_id {
                tuples.push((device_id, doc.id.unwrap().to_string()));
            }
        }

        Ok(tuples)
    }

    pub async fn list_for_device(
        db: &Arc<Mongo>,
        device_id: ObjectId,
        pagination: &RequestPagination,
    ) -> ServerResult<Vec<DeployRevisionDoc>> {
        let options = FindOptions::builder()
            .skip(Some(pagination.offset))
            .limit(Some(pagination.limit as i64))
            // sort by index descending
            .sort(doc! {"index": -1})
            .build();
        let cursor = db
            .deploy_revisions()
            .find(doc! { "device_id": device_id })
            .with_options(options)
            .await?;
        let results: Vec<DeployRevisionDoc> = cursor
            .try_collect()
            .await
            .map_err(|_| ServerError::internal_error("Cursor decode failed"))?;
        Ok(results)
    }

    pub async fn get_by_revision_id(
        db: &Arc<Mongo>,
        revision_id: String,
    ) -> ServerResult<DeployRevisionDoc> {
        let doc = db
            .deploy_revisions()
            .find_one(doc! { "revision.id": revision_id })
            .await?;
        doc.ok_or(ServerError::not_found("Deploy revision not found"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployReportDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    pub device_id: ObjectId,
    pub revision_id: String,

    pub kind: DeployReportKind,

    /// TTL target (Mongo will delete when this time is reached)
    pub expires_at: Option<BsonDateTime>,

    /// When the report was received/created
    pub created_at: BsonDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDeployReportBody {
    pub device_id: ObjectId,
    pub revision_id: String,
    pub kind: DeployReportKind,

    /// Optional TTL (server can set a default if None)
    #[serde(default)]
    pub expires_at: Option<BsonDateTime>,
}

impl DeployReportDoc {
    pub async fn get_device_observations(
        db: &Arc<Mongo>,
        device_id: &ObjectId,
        revision_id: &str,
    ) -> ServerResult<Vec<ObserveStatus>> {
        let filter = doc! {
            "device_id": device_id,
            "revision_id": revision_id,
        };

        let mut cursor = db
            .current_run_states()
            .find(filter)
            .await
            .map_err(|e| ServerError::internal_error(&format!("Mongo find failed: {e}")))?;

        let mut out = Vec::new();

        while let Some(doc) = cursor.try_next().await? {
            out.push(ObserveStatus {
                name: doc.run_id,
                alive: doc.alive,
                healthy: doc.healthy,
                crashes: doc.crashes as u32,
                unhealthy_checks: doc.unhealthy_checks as u32,
            });
        }

        Ok(out)
    }
    pub async fn get_devices_observations(
        db: &Arc<Mongo>,
        device_and_revision_ids: &[(ObjectId, String)],
    ) -> ServerResult<BTreeMap<String, Vec<ObserveStatus>>> {
        // Build OR filter: [{device_id: X, revision_id: Y}, ...]
        let or_filters: Vec<_> = device_and_revision_ids
            .iter()
            .map(|(device_id, revision_id)| {
                doc! {
                    "device_id": device_id,
                    "revision_id": revision_id,
                }
            })
            .collect();

        if or_filters.is_empty() {
            return Ok(BTreeMap::new());
        }

        let filter = doc! {
            "$or": or_filters
        };

        let mut cursor = db
            .current_run_states()
            .find(filter)
            .await
            .map_err(|e| ServerError::internal_error(&format!("Mongo find failed: {e}")))?;

        let mut out: BTreeMap<String, Vec<ObserveStatus>> = BTreeMap::new();

        while let Some(doc) = cursor.try_next().await? {
            let device_id = doc.device_id.to_string();
            let status = ObserveStatus {
                name: doc.run_id,
                alive: doc.alive,
                healthy: doc.healthy,
                crashes: doc.crashes as u32,
                unhealthy_checks: doc.unhealthy_checks as u32,
            };

            out.entry(device_id).or_insert_with(Vec::new).push(status);
        }

        Ok(out)
    }

    pub async fn create_or_update(
        db: &Arc<Mongo>,
        body: CreateDeployReportBody,
    ) -> ServerResult<Self> {
        // let filter = Self::upsert_filter(&body)?;
        // check if device id + revision + optional kind.data.run_id still exist. If not ignore
        let mut check_doc = doc! {
            "device_id": &body.device_id,
            "revision.id": &body.revision_id,
        };
        if let Some(run_id) = body.kind.get_run_id() {
            // check if and revision.jobs lsit entry has id == run_id
            check_doc.insert("revision.jobs.id", run_id);
        }
        let exists = db
            .deploy_revisions()
            .find_one(check_doc)
            .await
            .map_err(|e| {
                ServerError::internal_error(&format!(
                    "Failed to check deploy revision existence: {:?}",
                    e
                ))
            })
            .map(|doc| doc.is_some())
            .unwrap_or(false);
        if !exists {
            return Err(ServerError::not_found(&format!(
                "Deploy revision {} {} {} not found",
                &body.device_id,
                &body.revision_id,
                &body.kind.get_run_id().unwrap_or_default(),
            )));
        }

        let now = BsonDateTime::now();

        // let kind_bson = to_bson(&body.kind)
        //     .map_err(|_| ServerError::internal_error("Failed to serialize deploy report kind"))?;

        // Overwrite the report doc (except _id) on every update.
        // created_at becomes "received_at" semantics (latest receive time).
        //
        let mut doc = Self {
            id: None,
            device_id: body.device_id,
            revision_id: body.revision_id,
            kind: body.kind,
            expires_at: body.expires_at,
            created_at: now,
        };
        let res = db.deploy_reports().insert_one(&doc).await.map_err(|e| {
            ServerError::internal_error(&format!("Failed to create deploy report: {:?}", e))
        })?;
        if let Err(e) = CurrentRunStateDoc::upsert_from_deploy_report(db, &doc).await {
            tracing::error!("Failed to upsert current run state: {:?}", e);
        }
        doc.id = res.inserted_id.as_object_id();
        Ok(doc)
    }

    pub fn to_pub_report(&self) -> DeployReport {
        DeployReport {
            device_id: self.device_id.to_string(),
            revision_id: self.revision_id.clone(),
            kind: self.kind.clone(),
            expires_at: self.expires_at.map(|dt| dt.timestamp_millis() as u64),
            created_at: self.created_at.timestamp_millis() as u64,
        }
    }

    pub async fn delete(db: &Arc<Mongo>, id: ObjectId) -> ServerResult<bool> {
        let res = db
            .deploy_reports()
            .delete_one(doc! { "_id": id })
            .await
            .map_err(|_| ServerError::internal_error("Failed to delete deploy report"))?;
        Ok(res.deleted_count == 1)
    }

    pub async fn list_run_states_for_device(
        db: &Arc<Mongo>,
        device_id: &ObjectId,
        revision_id: &str,
        pagination: &RequestPagination,
    ) -> ServerResult<Vec<DeployReportDoc>> {
        let limit = pagination.limit.min(5) as i64;

        let mut filter = doc! {
            "device_id": device_id,
            "revision_id": revision_id,
            "kind.type": "RunState",
        };

        let mut created_at_filter = Document::new();

        if let Some(until) = pagination.until {
            created_at_filter.insert("$lt", Bson::DateTime(until));
        }

        if let Some(since) = pagination.since {
            created_at_filter.insert("$gt", Bson::DateTime(since));
        }

        if !created_at_filter.is_empty() {
            filter.insert("created_at", created_at_filter);
        }

        let options = FindOptions::builder()
            .sort(doc! { "created_at": -1, "_id": -1 })
            .limit(Some(limit))
            .build();

        let cursor = db
            .deploy_reports()
            .find(filter)
            .with_options(options)
            .await?;

        let results: Vec<DeployReportDoc> = cursor
            .try_collect()
            .await
            .map_err(|_| ServerError::internal_error("Cursor decode failed"))?;

        Ok(results)
    }

    pub async fn compute_deployment_status_snapshot_for_device(
        db: &Arc<Mongo>,
        device_id: &ObjectId,
        revision_id: &str,
    ) -> ServerResult<DeploymentStatusSnapshot> {
        // 1) Pre-build runs/steps from spec using Vec indexing only.
        // Keep a *small* run_id -> run_idx map (jobs count is small; this is not the memory problem).
        let deployment_doc = db
            .deploy_revisions()
            .find_one(doc! { "revision.id": revision_id, "device_id": device_id})
            .await?
            .ok_or(ServerError::not_found("Deployment not found"))?;
        let deployment = deployment_doc.revision;
        let total_units =
            deployment.services.len() + deployment.observers.len() + deployment.jobs.len();
        let mut runs: Vec<RunStatus> = Vec::with_capacity(total_units);
        let mut run_id_to_idx: HashMap<String, usize> = HashMap::with_capacity(total_units);

        // Helper closure: build step slot vec for any unit type's steps list.
        let build_steps = |unit_id: &str, spec_steps: &[Step]| -> Vec<StepStatus> {
            let mut steps = Vec::with_capacity(step_slots(spec_steps.len()));
            for (i, st) in spec_steps.iter().enumerate() {
                let name = st.name.clone().unwrap_or_else(|| format!("step {}", i + 1));
                steps.push(StepStatus {
                    step_id: step_id(unit_id, i, false),
                    name: name.clone(),
                    is_undo: false,
                    defined_in_spec: true,
                    state: StepState::Pending,
                    last_update: None,
                    attempt: None,
                    attempts_total: 0,
                    exit_code: None,
                    error: None,
                });
                steps.push(StepStatus {
                    step_id: step_id(unit_id, i, true),
                    name,
                    is_undo: true,
                    defined_in_spec: st.undo.is_some(),
                    state: StepState::Pending,
                    last_update: None,
                    attempt: None,
                    attempts_total: 0,
                    exit_code: None,
                    error: None,
                });
            }
            steps
        };

        for svc in deployment.services.iter() {
            let ri = runs.len();
            run_id_to_idx.insert(svc.id.clone(), ri);
            let steps = build_steps(&svc.id, &svc.steps);
            runs.push(RunStatus {
                run_id: svc.id.clone(),
                enabled: !svc.lifecycle.is_stopped(),
                unit_kind: UnitKind::Service,
                outcome: Outcome::Unknown,
                last_update: 0,
                error: None,
                alive: None,
                healthy: None,
                steps,
            });
        }

        for obs in deployment.observers.iter() {
            let ri = runs.len();
            run_id_to_idx.insert(obs.id.clone(), ri);
            let steps = build_steps(&obs.id, &obs.steps);
            runs.push(RunStatus {
                run_id: obs.id.clone(),
                enabled: !obs.lifecycle.is_stopped(),
                unit_kind: UnitKind::Observer,
                outcome: Outcome::Unknown,
                last_update: 0,
                error: None,
                alive: None,
                healthy: None,
                steps,
            });
        }

        for job in deployment.jobs.iter() {
            let ri = runs.len();
            run_id_to_idx.insert(job.id.clone(), ri);
            let steps = build_steps(&job.id, &job.steps);
            runs.push(RunStatus {
                run_id: job.id.clone(),
                enabled: !job.lifecycle.is_stopped(),
                unit_kind: UnitKind::Job,
                outcome: Outcome::Unknown,
                last_update: 0,
                error: None,
                alive: None,
                healthy: None,
                steps,
            });
        }

        // 2) Query Mongo with a cursor; stream docs and update snapshot in place.
        // Sort by report_time ascending so “latest” comparisons are cheap and predictable.
        let options = FindOptions::builder()
            .sort(doc! { "report_time": 1i32 })
            .batch_size(Some(256))
            .build();

        let mut cursor = db
            .deploy_reports()
            .find(doc! { "device_id": device_id, "revision_id": revision_id })
            .with_options(options)
            .await?;

        // Top-level revision fields
        let mut dirty = false;
        let mut rev_error: Option<String> = None;
        let mut rev_outcome = Outcome::Unknown;
        let mut rollback: Option<RollbackStatus> = None;

        while let Some(doc) = cursor
            .try_next()
            .await
            .map_err(|_| ServerError::internal_error("Cursor decode failed"))?
        {
            let r = doc.to_pub_report();

            match r.kind {
                DeployReportKind::DeploymentRevisionReport(x) => {
                    dirty = x.dirty;
                    rev_error = x
                        .error
                        .map(|e| e.trim().to_string())
                        .filter(|s| !s.is_empty());
                    rev_outcome = x.outcome;
                }
                DeployReportKind::RollbackReport(x) => {
                    rollback = Some(RollbackStatus {
                        new_revision_id: x.new_revision_id,
                        report_time: None,
                    });
                }
                DeployReportKind::RunReport(x) => {
                    if let Some(&ri) = run_id_to_idx.get(&x.run_id) {
                        let run = &mut runs[ri];
                        let t = x.report_time as u64;
                        run.last_update = run.last_update.max(t);
                        if let Some(e) =
                            x.error.as_ref().map(|e| e.trim()).filter(|s| !s.is_empty())
                        {
                            run.error = Some(e.to_string());
                        }
                    }
                }
                DeployReportKind::RunState(x) => {
                    if let Some(&ri) = run_id_to_idx.get(&x.run_id) {
                        let run = &mut runs[ri];
                        let t = x.report_time as u64;
                        run.last_update = run.last_update.max(t);

                        // Update alive/healthy with latest only; no per-run Vec<RunState>.
                        // Adapt these fields to your RunState shape.
                        let rs = x.as_observe_update();
                        if let Some(ok) = rs.alive {
                            let item = ObserveStatusItem {
                                report_time: t,
                                ok,
                                log_tail: rs.log_tail.clone(),
                            };
                            if run.alive.as_ref().map(|a| a.report_time).unwrap_or(0) <= t {
                                run.alive = Some(item);
                            }
                        }
                        if let Some(ok) = rs.healthy {
                            let item = ObserveStatusItem {
                                report_time: t,
                                ok,
                                log_tail: rs.log_tail.clone(),
                            };
                            if run.healthy.as_ref().map(|a| a.report_time).unwrap_or(0) <= t {
                                run.healthy = Some(item);
                            }
                        }
                    }
                }
                DeployReportKind::StepReport(s) => {
                    if let Some(&ri) = run_id_to_idx.get(&s.run_id) {
                        let run = &mut runs[ri];
                        let t = s.report_time as u64;
                        run.last_update = run.last_update.max(t);
                        // Look up step index from whichever unit section owns this run_id.
                        let idx = {
                            let found = if let Some(job) = deployment.get_job_by_id(&s.run_id) {
                                job.steps.iter().position(|step| step.name == s.name)
                            } else if let Some(svc) = deployment.get_service_by_id(&s.run_id) {
                                svc.steps.iter().position(|step| step.name == s.name)
                            } else if let Some(obs) = deployment.get_observer_by_id(&s.run_id) {
                                obs.steps.iter().position(|step| step.name == s.name)
                            } else {
                                None
                            };
                            match found {
                                Some(i) => i,
                                None => continue,
                            }
                        };

                        // Critical: use step_index from the report (store it in DB).
                        let idx = idx as usize;
                        let slot = if s.is_undo {
                            undo_slot(idx)
                        } else {
                            main_slot(idx)
                        };
                        if slot >= run.steps.len() {
                            continue;
                        }

                        let st = &mut run.steps[slot];
                        st.attempts_total = st.attempts_total.max(s.attempts);
                        st.exit_code = s.exit_code;
                        st.error = s
                            .error
                            .as_ref()
                            .map(|e| e.trim().to_string())
                            .filter(|x| !x.is_empty());
                        st.last_update = Some(st.last_update.unwrap_or(0).max(t));
                        st.state = if s.success {
                            StepState::Success
                        } else {
                            StepState::Failed
                        };

                        let attempt = StepAttemptStatus {
                            n: s.attempts,
                            report_time: t,
                            success: s.success,
                            exit_code: s.exit_code,
                            error: s
                                .error
                                .as_ref()
                                .map(|e| e.trim().to_string())
                                .filter(|x| !x.is_empty()),
                            log_tail: s.log_tail.clone(),
                        };

                        if st.attempt.as_ref().map(|a| a.report_time).unwrap_or(0) <= t {
                            st.attempt = Some(attempt);
                        }
                    }
                }
                DeployReportKind::JobRunReport(_) => {
                    // Job run reports are handled separately; ignore in snapshot.
                }
            }
        }

        // 3) Derive run outcomes without building any more maps.
        for run in &mut runs {
            if run.error.is_some() {
                run.outcome = Outcome::Failed;
                continue;
            }
            run.outcome = outcome_from_steps(&run.steps);
        }

        // 4) Derive overall outcome.
        let outcome = if rev_outcome != Outcome::Unknown {
            rev_outcome
        } else if runs.iter().any(|r| r.outcome == Outcome::Failed) {
            Outcome::Failed
        } else if runs.iter().any(|r| r.outcome == Outcome::Unknown) {
            Outcome::Unknown
        } else if runs.iter().any(|r| r.outcome == Outcome::Success) {
            Outcome::Success
        } else {
            Outcome::Unknown
        };

        Ok(DeploymentStatusSnapshot {
            revision_id: revision_id.to_string(),
            outcome,
            dirty,
            error: rev_error,
            rollback,
            runs,
        })
    }

    pub async fn agg_failures_buckets(
        db: &Arc<Mongo>,
        device_id: &ObjectId,
        revision_id: &str,
        from_ts_ms: u64,
        to_ts_ms: u64,
        bucket_ms: u64,
        level: SliceLevel,
    ) -> ServerResult<FailureAggResponse> {
        use futures::TryStreamExt;
        use mongodb::bson::{Document, doc};
        use std::collections::{BTreeMap, BTreeSet};

        if to_ts_ms <= from_ts_ms {
            return Err(ServerError::bad_request("to_ts_ms must be > from_ts_ms"));
        }
        if bucket_ms == 0 {
            return Err(ServerError::bad_request("bucket_ms must be > 0"));
        }

        // Cast to i64 for use in BSON pipeline expressions (BSON has no u64).
        let from_ms: i64 = from_ts_ms as i64;
        let to_ms: i64 = to_ts_ms as i64;
        let bucket: i64 = bucket_ms as i64;

        let from_dt = mongodb::bson::DateTime::from_millis(from_ms);
        let to_dt = mongodb::bson::DateTime::from_millis(to_ms);

        let match_doc = doc! {
            "device_id": device_id,
            "revision_id": revision_id,
            "kind.type": "RunState",
            "created_at": { "$gte": from_dt, "$lt": to_dt },
        };

        // created_at_ms = toLong(created_at)
        let created_ms = doc! { "$toLong": "$created_at" };

        // raw_bucket_start = from + floor((created_ms - from)/bucket_ms)*bucket_ms
        let raw_bucket_start = doc! {
            "$add": [
                from_ms,
                {
                    "$multiply": [
                        bucket,
                        {
                            "$floor": {
                                "$divide": [
                                    { "$subtract": [ created_ms, from_ms ] },
                                    bucket
                                ]
                            }
                        }
                    ]
                }
            ]
        };

        // bucket_start_ms = toLong(raw_bucket_start)
        let bucket_start_ms = doc! { "$toLong": raw_bucket_start };

        // bucket_end_ms = toLong(bucket_start_ms + bucket_ms)
        let bucket_end_ms = doc! {
            "$toLong": { "$add": [ bucket_start_ms.clone(), bucket ] }
        };

        // Count events (not 0/1):
        // - crash event: alive == Some(false)
        // - unhealthy event: healthy == Some(false)
        // null/missing => 0
        let crash_evt = doc! {
            "$cond": [
                { "$eq": [ "$kind.data.alive", false ] },
                1,
                0
            ]
        };

        let unhealthy_evt = doc! {
            "$cond": [
                { "$eq": [ "$kind.data.healthy", false ] },
                1,
                0
            ]
        };

        let pipeline: Vec<Document> = vec![
            doc! { "$match": match_doc },
            doc! {
                "$addFields": {
                    "bucket_start_ms": bucket_start_ms,
                    "bucket_end_ms": bucket_end_ms
                }
            },
            // per (bucket, run): SUM counts within the bucket for that run
            doc! {
                "$group": {
                    "_id": { "bucket_start_ms": "$bucket_start_ms", "run_id": "$kind.data.run_id" },
                    "bucket_start_ms": { "$first": "$bucket_start_ms" },
                    "bucket_end_ms": { "$first": "$bucket_end_ms" },
                    "crashes": { "$sum": crash_evt },
                    "unhealthy_checks": { "$sum": unhealthy_evt },
                }
            },
            // per bucket: totals are sums of per-run counts
            doc! {
                "$group": {
                    "_id": "$bucket_start_ms",
                    "bucket_start_ms": { "$first": "$bucket_start_ms" },
                    "bucket_end_ms": { "$first": "$bucket_end_ms" },
                    "pairs": {
                        "$push": {
                            "k": "$_id.run_id",
                            "v": { "crashes": "$crashes", "unhealthy_checks": "$unhealthy_checks" }
                        }
                    },
                    "total_crashes": { "$sum": "$crashes" },
                    "total_unhealthy_checks": { "$sum": "$unhealthy_checks" }
                }
            },
            doc! { "$sort": { "bucket_start_ms": 1 } },
            doc! {
                "$project": {
                    "_id": 0,
                    "start_ts_ms": "$bucket_start_ms",
                    "end_ts_ms": "$bucket_end_ms",
                    "total": {
                        "crashes": "$total_crashes",
                        "unhealthy_checks": "$total_unhealthy_checks"
                    },
                    "by_run": { "$arrayToObject": "$pairs" }
                }
            },
        ];

        let mut cursor =
            db.deploy_reports().aggregate(pipeline).await.map_err(|e| {
                ServerError::internal_error(&format!("Mongo aggregate failed: {e}"))
            })?;

        #[derive(serde::Deserialize)]
        struct Row {
            start_ts_ms: i64,
            end_ts_ms: i64,
            total: BucketTotals,
            by_run: BTreeMap<String, BucketTotals>,
        }

        let mut buckets: Vec<BucketRow> = Vec::new();
        let mut run_set: BTreeSet<String> = BTreeSet::new();

        while let Some(d) = cursor.try_next().await? {
            let row: Row = mongodb::bson::from_document(d)
                .map_err(|e| ServerError::internal_error(&format!("BSON decode failed: {e}")))?;

            for k in row.by_run.keys() {
                run_set.insert(k.clone());
            }

            buckets.push(BucketRow {
                start_ts_ms: row.start_ts_ms as u64,
                end_ts_ms: row.end_ts_ms.min(to_ms) as u64,
                total: row.total,
                by_run: row.by_run,
            });
        }

        let runs: Vec<String> = run_set.into_iter().collect();

        Ok(FailureAggResponse {
            level,
            runs,
            buckets,
        })
    }
}

fn undo_slot(step_index: usize) -> usize {
    step_index * 2 + 1
}
fn main_slot(step_index: usize) -> usize {
    step_index * 2
}

fn step_slots(step_count: usize) -> usize {
    // allocate 2 per step (main + undo) to allow O(1) indexing;
    // you can still hide undo in rendering if never executed.
    step_count * 2
}

fn step_id(run_id: &str, idx: usize, is_undo: bool) -> String {
    if is_undo {
        format!("{run_id}:{idx}:undo")
    } else {
        format!("{run_id}:{idx}")
    }
}

fn outcome_from_steps(steps: &[StepStatus]) -> Outcome {
    // Only consider non-undo steps as "expected" for outcome; undo is remedial.
    let mut any_failed = false;
    let mut any_success = false;
    let mut any_pending = false;

    for s in steps.iter().filter(|s| !s.is_undo) {
        match s.state {
            StepState::Failed => any_failed = true,
            StepState::Success => any_success = true,
            StepState::Pending => any_pending = true,
            StepState::Running => {}
            StepState::Skipped => {}
        }
    }

    if any_failed {
        Outcome::Failed
    } else if any_pending {
        Outcome::Unknown
    } else if any_success {
        Outcome::Success
    } else {
        Outcome::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentRunStateDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    /// Device this run belongs to
    pub device_id: ObjectId,

    /// Deployment / revision identifier
    pub revision_id: String,

    /// Run identifier (per deployment execution)
    pub run_id: String,

    /// Last known liveness state
    #[serde(default)]
    pub alive: bool,

    /// Last known health state
    #[serde(default)]
    pub healthy: bool,

    /// Timestamp reported by the runtime (monotonic per run)
    #[serde(default)]
    pub last_report_time: u64,

    /// Total number of unhealthy checks observed so far
    #[serde(default)]
    pub unhealthy_checks: u64,

    /// Total number of crash events observed so far
    #[serde(default)]
    pub crashes: u64,

    /// When this state was last updated (server time)
    pub updated_at: BsonDateTime,
}

impl CurrentRunStateDoc {
    pub async fn upsert_from_deploy_report(
        db: &Arc<Mongo>,
        report: &DeployReportDoc,
    ) -> ServerResult<()> {
        let run_state = match &report.kind {
            DeployReportKind::RunState(rs) => rs,
            _ => return Ok(()),
        };

        let now = BsonDateTime::now();

        let report_time_i64: i64 = run_state
            .report_time
            .try_into()
            .map_err(|_| ServerError::internal_error("report_time overflows i64"))?;

        // Build $set dynamically
        let mut set_doc = doc! {
            "last_report_time": Bson::Int64(report_time_i64),
            "updated_at": now,
        };

        if let Some(alive) = run_state.alive {
            set_doc.insert("alive", alive);
        }
        if let Some(healthy) = run_state.healthy {
            set_doc.insert("healthy", healthy);
        }

        // Build $inc dynamically (or keep zeros out)
        let mut inc_doc = doc! {};
        if let Some(healthy) = run_state.healthy {
            if !healthy {
                inc_doc.insert("unhealthy_checks", 1i64);
            }
        }
        if let Some(alive) = run_state.alive {
            if !alive {
                inc_doc.insert("crashes", 1i64);
            }
        }

        let mut update = doc! {
            "$set": set_doc,
            "$setOnInsert": {
                "device_id": &report.device_id,
                "revision_id": &report.revision_id,
                "run_id": &run_state.run_id,
                "created_at": now,
            },
        };

        if !inc_doc.is_empty() {
            update.insert("$inc", inc_doc);
        }

        let filter = doc! {
            "device_id": &report.device_id,
            "revision_id": &report.revision_id,
            "run_id": &run_state.run_id,
            "$or": [
                { "last_report_time": { "$lt": Bson::Int64(report_time_i64) } },
                { "last_report_time": { "$exists": false } }, // allows first insert
            ],
        };

        let res = db
            .current_run_states()
            .update_one(filter, update)
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await;

        match res {
            Ok(_) => Ok(()),

            Err(e) => match e.kind.as_ref() {
                // This is the case you want to ignore
                ErrorKind::Write(WriteFailure::WriteError(we)) if we.code == 11000 => {
                    // Existing doc + older report → ignored
                    Ok(())
                }

                // Everything else is real failure
                _ => Err(ServerError::internal_error(&format!(
                    "Failed to upsert CurrentRunStateDoc: {e}"
                ))),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// JobRunDoc
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub run_id: String,
    pub device_id: ObjectId,
    pub revision_id: String,
    pub job_def_id: String,
    #[serde(default)]
    pub env_overrides: BTreeMap<String, String>,
    pub status: JobRunStatus,
    pub enqueued_at: BsonDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<BsonDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<BsonDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
}

impl JobRunDoc {
    pub fn to_pub_job_run(&self) -> JobRun {
        JobRun {
            run_id: self.run_id.clone(),
            job_def_id: self.job_def_id.clone(),
            revision_id: self.revision_id.clone(),
            env_overrides: self.env_overrides.clone(),
            status: self.status.clone(),
            enqueued_at: self.enqueued_at.timestamp_millis() as u64,
            started_at: self.started_at.map(|t| t.timestamp_millis() as u64),
            completed_at: self.completed_at.map(|t| t.timestamp_millis() as u64),
            error: self.error.clone(),
        }
    }

    pub async fn create(
        db: &Arc<Mongo>,
        device_id: ObjectId,
        revision_id: String,
        job_def_id: String,
        env_overrides: BTreeMap<String, String>,
        owner_scope: String,
        allowed_scopes: Vec<String>,
    ) -> ServerResult<Self> {
        let doc = Self {
            id: None,
            run_id: uuid::Uuid::new_v4().to_string(),
            device_id,
            revision_id,
            job_def_id,
            env_overrides,
            status: JobRunStatus::Queued,
            enqueued_at: BsonDateTime::now(),
            started_at: None,
            completed_at: None,
            error: None,
            owner_scope,
            allowed_scopes,
        };
        db.job_runs().insert_one(doc.clone()).await?;
        Ok(doc)
    }

    pub async fn get_pending_for_device(
        db: &Arc<Mongo>,
        device_id: ObjectId,
    ) -> ServerResult<Vec<JobRun>> {
        let docs: Vec<JobRunDoc> = db
            .job_runs()
            .find(doc! { "device_id": device_id, "status": "queued" })
            .await?
            .try_collect()
            .await?;
        Ok(docs.into_iter().map(|d| d.to_pub_job_run()).collect())
    }

    pub async fn mark_running(db: &Arc<Mongo>, run_id: &str) -> ServerResult<()> {
        db.job_runs()
            .update_one(
                doc! { "run_id": run_id },
                doc! { "$set": {
                    "status": "running",
                    "started_at": BsonDateTime::now(),
                }},
            )
            .await?;
        Ok(())
    }

    pub async fn update_status(
        db: &Arc<Mongo>,
        run_id: &str,
        status: JobRunStatus,
        error: Option<String>,
    ) -> ServerResult<()> {
        let status_str = match status {
            JobRunStatus::Success => "success",
            JobRunStatus::Failed => "failed",
            JobRunStatus::Running => "running",
            JobRunStatus::Queued => "queued",
        };
        let mut set_doc = doc! {
            "status": status_str,
            "completed_at": BsonDateTime::now(),
        };
        if let Some(e) = error {
            set_doc.insert("error", e);
        }
        db.job_runs()
            .update_one(doc! { "run_id": run_id }, doc! { "$set": set_doc })
            .await?;
        Ok(())
    }

    pub async fn list_for_device(
        db: &Arc<Mongo>,
        device_id: &ObjectId,
        job_def_id: Option<&str>,
    ) -> ServerResult<Vec<JobRunDoc>> {
        let mut filter = doc! { "device_id": device_id };
        if let Some(id) = job_def_id {
            filter.insert("job_def_id", id);
        }
        let docs: Vec<JobRunDoc> = db.job_runs().find(filter).await?.try_collect().await?;
        Ok(docs)
    }

    pub async fn get_by_run_id(
        db: &Arc<Mongo>,
        device_id: &ObjectId,
        run_id: &str,
    ) -> ServerResult<Option<JobRunDoc>> {
        let doc = db
            .job_runs()
            .find_one(doc! { "device_id": device_id, "run_id": run_id })
            .await?;
        Ok(doc)
    }
}
