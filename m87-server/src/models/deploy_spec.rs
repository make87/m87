use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use futures::TryStreamExt;
use m87_shared::{
    deploy_spec::{
        BucketRow, BucketTotals, DeployReport, DeployReportKind, DeploymentRevision,
        DeploymentStatusSnapshot, FailureAggResponse, ObserveKind, ObserveStatusItem, Outcome,
        RollbackStatus, RunSpec, RunStatus, SliceLevel, StepAttemptStatus, StepState, StepStatus,
        UpdateDeployRevisionBody,
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
    if body.add_run_spec.is_some() {
        which += 1;
    }
    if body.update_run_spec.is_some() {
        which += 1;
    }
    if body.remove_run_spec_id.is_some() {
        which += 1;
    }
    if body.active.is_some() {
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

    if let Some(yaml) = &body.add_run_spec {
        let rs: RunSpec = serde_yaml::from_str(yaml).map_err(|e| {
            ServerError::bad_request(&format!("invalid YAML in `add_run_spec`: {}", e))
        })?;
        return Ok((
            doc! { "$push": { "revision.jobs": to_bson(&rs).map_err(|e| ServerError::bad_request(&format!("RunSpec -> bson failed: {}", e)))? } },
            None,
        ));
    }

    if let Some(yaml) = &body.update_run_spec {
        let rs: RunSpec = serde_yaml::from_str(yaml).map_err(|e| {
            ServerError::bad_request(&format!("invalid YAML in `update_run_spec`: {}", e))
        })?;
        return Ok((
            doc! { "$set": { "revision.jobs.$": to_bson(&rs).map_err(|e| ServerError::bad_request(&format!("RunSpec -> bson failed: {}", e)))? } },
            Some(doc! { "revision.jobs.id": &rs.id }),
        ));
    }

    if let Some(id) = &body.remove_run_spec_id {
        return Ok((doc! { "$pull": { "revision.jobs": { "id": id } } }, None));
    }

    if let Some(active) = body.active {
        return Ok((doc! { "$set": { "active": active } }, None));
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
        let mut runs: Vec<RunStatus> = Vec::with_capacity(deployment.jobs.len());
        let mut run_id_to_idx: HashMap<String, usize> =
            HashMap::with_capacity(deployment.jobs.len());

        for (ri, job) in deployment.jobs.iter().enumerate() {
            run_id_to_idx.insert(job.id.clone(), ri);

            let mut steps: Vec<StepStatus> = Vec::with_capacity(step_slots(job));
            for (i, st) in job.steps.iter().enumerate() {
                let name = st.name.clone().unwrap_or_else(|| format!("step {}", i + 1));
                // main row
                steps.push(StepStatus {
                    step_id: step_id(&job.id, i, false),
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
                // undo row (allocated but “expected” only if undo exists)
                steps.push(StepStatus {
                    step_id: step_id(&job.id, i, true),
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

            runs.push(RunStatus {
                run_id: job.id.clone(),
                enabled: job.enabled,
                run_type: job.run_type.clone(),
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
                        if let Some((kind, ok, log_tail)) = x.as_observe_update() {
                            let item = ObserveStatusItem {
                                report_time: t,
                                ok,
                                log_tail,
                            };
                            match kind {
                                ObserveKind::Alive => {
                                    if run.alive.as_ref().map(|a| a.report_time).unwrap_or(0) <= t {
                                        run.alive = Some(item);
                                    }
                                }
                                ObserveKind::Healthy => {
                                    if run.healthy.as_ref().map(|a| a.report_time).unwrap_or(0) <= t
                                    {
                                        run.healthy = Some(item);
                                    }
                                }
                            }
                        }
                    }
                }
                DeployReportKind::StepReport(s) => {
                    if let Some(&ri) = run_id_to_idx.get(&s.run_id) {
                        let run = &mut runs[ri];
                        let t = s.report_time as u64;
                        run.last_update = run.last_update.max(t);
                        let job = deployment.get_job_by_id(&s.run_id);
                        if job.is_none() {
                            continue;
                        }
                        let job = job.unwrap();
                        let idx = job.steps.iter().position(|step| step.name == s.name);
                        if idx.is_none() {
                            continue;
                        }
                        let idx = idx.unwrap();

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
                            log_tail: Some(s.log_tail.clone()),
                        };

                        if st.attempt.as_ref().map(|a| a.report_time).unwrap_or(0) <= t {
                            st.attempt = Some(attempt);
                        }
                    }
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
        from_ts_ms: i64,
        to_ts_ms: i64,
        bucket_ms: i64,
        level: SliceLevel,
    ) -> ServerResult<FailureAggResponse> {
        use futures::TryStreamExt;
        use mongodb::bson::{Document, doc};
        use std::collections::{BTreeMap, BTreeSet};

        // Validate
        if to_ts_ms <= from_ts_ms {
            return Err(ServerError::bad_request("to_ts_ms must be > from_ts_ms"));
        }
        if bucket_ms <= 0 {
            return Err(ServerError::bad_request("bucket_ms must be > 0"));
        }

        let from_dt = mongodb::bson::DateTime::from_millis(from_ts_ms);
        let to_dt = mongodb::bson::DateTime::from_millis(to_ts_ms);

        let match_doc = doc! {
            "device_id": device_id,
            "revision_id": revision_id,
            "kind.type": "RunState",
            "created_at": { "$gte": from_dt, "$lt": to_dt },
        };

        // bucket_start_ms = from + floor((created_at_ms - from)/bucket_ms)*bucket_ms
        let created_ms = doc! { "$toLong": "$created_at" };
        let bucket_start_ms = doc! {
            "$add": [
                from_ts_ms,
                {
                    "$multiply": [
                        bucket_ms,
                        {
                            "$floor": {
                                "$divide": [
                                    { "$subtract": [ created_ms, from_ts_ms ] },
                                    bucket_ms
                                ]
                            }
                        }
                    ]
                }
            ]
        };

        let pipeline: Vec<Document> = vec![
            doc! { "$match": match_doc },
            doc! {
                "$addFields": {
                    "bucket_start_ms": bucket_start_ms,
                    "bucket_end_ms": { "$add": ["$bucket_start_ms", bucket_ms] }
                }
            },
            // per (bucket, run)
            doc! {
                "$group": {
                    "_id": { "bucket_start_ms": "$bucket_start_ms", "run_id": "$kind.data.run_id" },
                    "bucket_start_ms": { "$first": "$bucket_start_ms" },
                    "bucket_end_ms": { "$first": "$bucket_end_ms" },
                    "crashes": { "$sum": "$kind.data.crashes" },
                    "unhealthy_checks": { "$sum": "$kind.data.unhealthy_checks" },
                }
            },
            // per bucket
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
            by_run: Option<BTreeMap<String, BucketTotals>>,
        }

        let mut buckets: Vec<BucketRow> = Vec::new();
        let mut run_set: BTreeSet<String> = BTreeSet::new();

        while let Some(d) = cursor.try_next().await? {
            let row: Row = mongodb::bson::from_document(d)
                .map_err(|e| ServerError::internal_error(&format!("BSON decode failed: {e}")))?;

            if let Some(ref m) = row.by_run {
                for k in m.keys() {
                    run_set.insert(k.clone());
                }
            }

            buckets.push(BucketRow {
                start_ts_ms: row.start_ts_ms,
                end_ts_ms: row.end_ts_ms.min(to_ts_ms),
                total: row.total,
                by_run: row.by_run,
            });
        }

        let runs: Vec<String> = run_set.into_iter().collect();

        Ok(FailureAggResponse {
            level,
            runs: Some(runs),
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

fn step_slots(job: &RunSpec) -> usize {
    // allocate 2 per step (main + undo) to allow O(1) indexing;
    // you can still hide undo in rendering if never executed.
    job.steps.len() * 2
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
