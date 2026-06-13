//! Unified event view across `services` / `observers` / `jobs`.
//!
//! The server stores everything as a flat list of `DeployReport`s. The CLI
//! used to filter aggressively (e.g. `logs --steps` showed only `StepReport`),
//! which made observe failure tails and job-run history unreachable. This
//! module collapses every report kind into a single `UnitEvent` shape and
//! applies the user's filters (kind / id / failed / time window) in one
//! place. Everything here is pure — no IO — so it's easy to unit-test.

use m87_shared::deploy_spec::{DeployReport, DeployReportKind, JobRunStatus};
use serde::Serialize;

/// Which broad category an event belongs to. Drives the `--services`
/// and `--jobs` filter flags on `m87 <dev> logs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventCategory {
    Service,
    Job,
    /// Deployment-level events (revision applied, rollback). These are
    /// included regardless of `--services` / `--jobs` because they aren't
    /// scoped to a single unit and the user generally wants them in the
    /// timeline.
    Deployment,
}

/// What kind of activity within a category.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSubKind {
    /// A step (start / stop / undo). `name` is the step name from the spec.
    Step { name: Option<String>, is_undo: bool },
    /// An observe check.
    Observe { kind: ObserveKind },
    /// A run-level outcome aggregate (RunReport).
    RunOutcome,
    /// Job run terminal status.
    JobTerminal { status: JobRunStatus },
    /// Deployment revision report (apply outcome).
    RevisionOutcome,
    /// Rollback report.
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ObserveKind {
    Liveness,
    Health,
}

/// Flattened event used by the `logs` command. Serialised directly as
/// NDJSON when `--json` is set.
#[derive(Debug, Clone, Serialize)]
pub struct UnitEvent {
    /// Milliseconds since the Unix epoch.
    pub ts: u64,
    pub category: EventCategory,
    pub sub_kind: EventSubKind,
    /// Service id, observer id, job-def id, or `None` for deployment-level
    /// events that don't bind to a unit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit_id: Option<String>,
    /// Job run id (only set for events from a specific job run).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Revision the event was emitted under.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
}

impl UnitEvent {
    pub fn from_report(report: &DeployReport) -> Self {
        let revision_id = report
            .kind
            .get_revision_id()
            .map(|s| s.to_string());
        let unit_id = report.kind.get_run_id().map(|s| s.to_string());
        let fallback_ts = report.created_at;

        match &report.kind {
            DeployReportKind::StepReport(r) => UnitEvent {
                ts: r.report_time,
                category: EventCategory::Service,
                sub_kind: EventSubKind::Step {
                    name: r.name.clone(),
                    is_undo: r.is_undo,
                },
                unit_id,
                run_id: None,
                revision_id,
                success: r.success,
                exit_code: r.exit_code,
                error: r.error.clone(),
                log_tail: r.log_tail.clone(),
            },
            DeployReportKind::RunState(r) => {
                // RunState carries either alive=Some(bool) or healthy=Some(bool)
                // (and the absent one is None) — derive the observe kind +
                // success state from whichever is populated.
                let (kind, success) = match (r.alive, r.healthy) {
                    (Some(a), _) => (ObserveKind::Liveness, a),
                    (_, Some(h)) => (ObserveKind::Health, h),
                    _ => (ObserveKind::Liveness, true),
                };
                UnitEvent {
                    ts: r.report_time,
                    category: EventCategory::Service,
                    sub_kind: EventSubKind::Observe { kind },
                    unit_id,
                    run_id: None,
                    revision_id,
                    success,
                    exit_code: None,
                    error: None,
                    log_tail: r.log_tail.clone(),
                }
            }
            DeployReportKind::JobRunReport(r) => UnitEvent {
                ts: r.report_time,
                category: EventCategory::Job,
                sub_kind: EventSubKind::JobTerminal {
                    status: r.status.clone(),
                },
                unit_id: Some(r.job_def_id.clone()),
                run_id: Some(r.run_id.clone()),
                revision_id,
                success: matches!(r.status, JobRunStatus::Success),
                exit_code: None,
                error: r.error.clone(),
                log_tail: None,
            },
            DeployReportKind::RunReport(r) => UnitEvent {
                ts: r.report_time,
                category: EventCategory::Service,
                sub_kind: EventSubKind::RunOutcome,
                unit_id,
                run_id: None,
                revision_id,
                success: matches!(
                    r.outcome,
                    m87_shared::deploy_spec::Outcome::Success
                ),
                exit_code: None,
                error: r.error.clone(),
                log_tail: None,
            },
            DeployReportKind::DeploymentRevisionReport(r) => UnitEvent {
                ts: fallback_ts,
                category: EventCategory::Deployment,
                sub_kind: EventSubKind::RevisionOutcome,
                unit_id: None,
                run_id: None,
                revision_id,
                success: matches!(
                    r.outcome,
                    m87_shared::deploy_spec::Outcome::Success
                ),
                exit_code: None,
                error: r.error.clone(),
                log_tail: None,
            },
            DeployReportKind::RollbackReport(_) => UnitEvent {
                ts: fallback_ts,
                category: EventCategory::Deployment,
                sub_kind: EventSubKind::Rollback,
                unit_id: None,
                run_id: None,
                revision_id,
                // Rollback is a recovery action — neutral. We render it as
                // "info" in the table. Mark success=true so --failed
                // doesn't surface it.
                success: true,
                exit_code: None,
                error: None,
                log_tail: None,
            },
        }
    }

    /// True when this event corresponds to the job-run id given. Used to
    /// support `logs <run-id>` where the id can be either a unit or a run.
    pub fn matches_run_id(&self, id: &str) -> bool {
        self.run_id.as_deref() == Some(id)
    }

    /// True when the event is bound to the unit id given.
    pub fn matches_unit_id(&self, id: &str) -> bool {
        self.unit_id.as_deref() == Some(id)
    }
}

/// Filter knobs that come from CLI flags. Used by `aggregate_events`.
#[derive(Debug, Clone, Default)]
pub struct EventFilter<'a> {
    /// Positional id — matches `unit_id` OR `run_id`.
    pub id: Option<&'a str>,
    /// If `services` and `jobs` are both true OR both false, all categories
    /// are included. Otherwise only the requested category (plus
    /// `Deployment` always passes — see `category_passes`).
    pub services: bool,
    pub jobs: bool,
    pub failed_only: bool,
    /// Inclusive lower bound, milliseconds since epoch.
    pub since_ms: Option<u64>,
    /// Inclusive upper bound, milliseconds since epoch.
    pub until_ms: Option<u64>,
}

impl<'a> EventFilter<'a> {
    fn category_passes(&self, c: EventCategory) -> bool {
        // Deployment-level events are always included so revision applies
        // and rollbacks show up in the timeline.
        if matches!(c, EventCategory::Deployment) {
            return true;
        }
        // Both flags off OR both on = no kind restriction.
        if self.services == self.jobs {
            return true;
        }
        match c {
            EventCategory::Service => self.services,
            EventCategory::Job => self.jobs,
            EventCategory::Deployment => true,
        }
    }

    fn id_passes(&self, ev: &UnitEvent) -> bool {
        match self.id {
            None => true,
            Some(id) => ev.matches_unit_id(id) || ev.matches_run_id(id),
        }
    }

    fn time_passes(&self, ev: &UnitEvent) -> bool {
        if let Some(since) = self.since_ms {
            if ev.ts < since {
                return false;
            }
        }
        if let Some(until) = self.until_ms {
            if ev.ts > until {
                return false;
            }
        }
        true
    }

    fn failure_passes(&self, ev: &UnitEvent) -> bool {
        !self.failed_only || !ev.success
    }
}

/// Apply the filter, sort by timestamp ascending, and keep at most
/// `tail_n` entries (the tail — i.e. the most recent N).
pub fn aggregate_events(
    reports: impl IntoIterator<Item = DeployReport>,
    filter: &EventFilter<'_>,
    tail_n: usize,
) -> Vec<UnitEvent> {
    let mut out: Vec<UnitEvent> = reports
        .into_iter()
        .map(|r| UnitEvent::from_report(&r))
        .filter(|ev| {
            filter.category_passes(ev.category)
                && filter.id_passes(ev)
                && filter.time_passes(ev)
                && filter.failure_passes(ev)
        })
        .collect();

    out.sort_by_key(|ev| ev.ts);

    if tail_n > 0 && out.len() > tail_n {
        let drop = out.len() - tail_n;
        out.drain(..drop);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use m87_shared::deploy_spec::{
        DeployReportKind, JobRunReport, JobRunStatus, RunState, StepReport,
    };

    fn step_report(run_id: &str, ts: u64, success: bool) -> DeployReport {
        DeployReport {
            device_id: "dev".into(),
            revision_id: "rev1".into(),
            kind: DeployReportKind::StepReport(StepReport {
                revision_id: "rev1".into(),
                run_id: run_id.into(),
                name: Some("start".into()),
                attempts: 1,
                exit_code: if success { Some(0) } else { Some(1) },
                report_time: ts,
                success,
                is_undo: false,
                error: None,
                log_tail: None,
            }),
            expires_at: None,
            created_at: ts,
        }
    }

    fn observe_report(run_id: &str, ts: u64, healthy: bool) -> DeployReport {
        DeployReport {
            device_id: "dev".into(),
            revision_id: "rev1".into(),
            kind: DeployReportKind::RunState(RunState {
                run_id: run_id.into(),
                revision_id: "rev1".into(),
                healthy: Some(healthy),
                alive: None,
                report_time: ts,
                log_tail: if healthy { None } else { Some("curl: connection refused".into()) },
            }),
            expires_at: None,
            created_at: ts,
        }
    }

    fn job_report(def: &str, run: &str, ts: u64, ok: bool) -> DeployReport {
        DeployReport {
            device_id: "dev".into(),
            revision_id: "rev1".into(),
            kind: DeployReportKind::JobRunReport(JobRunReport {
                run_id: run.into(),
                job_def_id: def.into(),
                revision_id: "rev1".into(),
                status: if ok {
                    JobRunStatus::Success
                } else {
                    JobRunStatus::Failed
                },
                report_time: ts,
                error: if ok { None } else { Some("script failed".into()) },
            }),
            expires_at: None,
            created_at: ts,
        }
    }

    #[test]
    fn from_report_step() {
        let r = step_report("web", 1000, false);
        let ev = UnitEvent::from_report(&r);
        assert_eq!(ev.ts, 1000);
        assert_eq!(ev.category, EventCategory::Service);
        assert_eq!(ev.unit_id.as_deref(), Some("web"));
        assert!(!ev.success);
        assert_eq!(ev.exit_code, Some(1));
    }

    #[test]
    fn from_report_observe_failure_carries_log_tail() {
        let r = observe_report("web", 2000, false);
        let ev = UnitEvent::from_report(&r);
        assert_eq!(ev.category, EventCategory::Service);
        assert!(!ev.success);
        assert!(matches!(
            ev.sub_kind,
            EventSubKind::Observe {
                kind: ObserveKind::Health
            }
        ));
        assert_eq!(ev.log_tail.as_deref(), Some("curl: connection refused"));
    }

    #[test]
    fn from_report_job_failure_carries_error() {
        let r = job_report("migrate", "abc-1", 3000, false);
        let ev = UnitEvent::from_report(&r);
        assert_eq!(ev.category, EventCategory::Job);
        assert_eq!(ev.unit_id.as_deref(), Some("migrate"));
        assert_eq!(ev.run_id.as_deref(), Some("abc-1"));
        assert!(!ev.success);
        assert_eq!(ev.error.as_deref(), Some("script failed"));
    }

    #[test]
    fn aggregate_sorts_by_ts_and_tails() {
        let reports = vec![
            step_report("web", 3000, true),
            observe_report("web", 1000, true),
            step_report("api", 2000, false),
        ];
        let out = aggregate_events(reports, &EventFilter::default(), 0);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].ts, 1000);
        assert_eq!(out[1].ts, 2000);
        assert_eq!(out[2].ts, 3000);
    }

    #[test]
    fn aggregate_keeps_only_tail_n() {
        let reports = (1..=10)
            .map(|i| step_report("web", i * 100, true))
            .collect::<Vec<_>>();
        let out = aggregate_events(reports, &EventFilter::default(), 3);
        assert_eq!(out.len(), 3);
        // Tail = most recent N.
        assert_eq!(out[0].ts, 800);
        assert_eq!(out[2].ts, 1000);
    }

    #[test]
    fn filter_by_id_matches_unit_or_run() {
        let reports = vec![
            step_report("web", 1000, true),
            step_report("api", 2000, true),
            job_report("migrate", "run-1", 3000, true),
            job_report("migrate", "run-2", 4000, false),
        ];
        let f = EventFilter {
            id: Some("web"),
            ..Default::default()
        };
        let out = aggregate_events(reports.clone(), &f, 0);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].unit_id.as_deref(), Some("web"));

        let f = EventFilter {
            id: Some("run-2"),
            ..Default::default()
        };
        let out = aggregate_events(reports.clone(), &f, 0);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].run_id.as_deref(), Some("run-2"));

        // job def id matches all runs of that def.
        let f = EventFilter {
            id: Some("migrate"),
            ..Default::default()
        };
        let out = aggregate_events(reports, &f, 0);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn filter_failed_only() {
        let reports = vec![
            step_report("web", 1000, true),
            step_report("web", 2000, false),
            observe_report("web", 3000, false),
            job_report("migrate", "run-1", 4000, true),
        ];
        let f = EventFilter {
            failed_only: true,
            ..Default::default()
        };
        let out = aggregate_events(reports, &f, 0);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|e| !e.success));
    }

    #[test]
    fn filter_kind_services_only_excludes_jobs() {
        let reports = vec![
            step_report("web", 1000, true),
            job_report("migrate", "run-1", 2000, true),
        ];
        let f = EventFilter {
            services: true,
            jobs: false,
            ..Default::default()
        };
        let out = aggregate_events(reports, &f, 0);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].category, EventCategory::Service);
    }

    #[test]
    fn filter_both_kinds_set_means_all() {
        let reports = vec![
            step_report("web", 1000, true),
            job_report("migrate", "run-1", 2000, true),
        ];
        let f = EventFilter {
            services: true,
            jobs: true,
            ..Default::default()
        };
        let out = aggregate_events(reports, &f, 0);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn filter_time_window() {
        let reports = (1..=10)
            .map(|i| step_report("web", i * 1000, true))
            .collect::<Vec<_>>();
        let f = EventFilter {
            since_ms: Some(3000),
            until_ms: Some(7000),
            ..Default::default()
        };
        let out = aggregate_events(reports, &f, 0);
        assert_eq!(out.len(), 5);
        assert_eq!(out.first().unwrap().ts, 3000);
        assert_eq!(out.last().unwrap().ts, 7000);
    }
}
