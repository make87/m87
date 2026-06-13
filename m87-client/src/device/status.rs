//! `m87 <dev> status` aggregator.
//!
//! Combines the server's `DeviceStatus` (current-state liveness/health/incident
//! snapshot) with optional windowed event aggregates derived from
//! `DeployReport`s. Pure / IO-free, like [`crate::device::events`], so the
//! summary logic is unit-tested in isolation from the CLI.

use std::collections::BTreeMap;

use m87_shared::device::DeviceStatus;
use serde::Serialize;

use crate::device::events::{EventCategory, EventSubKind, UnitEvent};

/// One thing currently wrong on the device.
#[derive(Debug, Clone, Serialize)]
pub struct CurrentIssue {
    pub unit: String,
    pub kind: CurrentIssueKind,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentIssueKind {
    NotAlive,
    Unhealthy,
    OpenIncident,
}

/// Per-unit roll-up over the requested time window.
#[derive(Debug, Clone, Default, Serialize)]
pub struct UnitWindow {
    pub unit: String,
    pub category: String,
    pub failures: u32,
    pub successes: u32,
    /// Number of `step/start` events seen. A proxy for "how many restarts".
    pub starts: u32,
    /// Most recent event timestamp in the window for this unit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WindowSummary {
    pub since_ms: u64,
    pub until_ms: u64,
    pub units: Vec<UnitWindow>,
    pub total_failures: u32,
    pub total_events: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSummary {
    pub device: String,
    pub current_issues: Vec<CurrentIssue>,
    pub observations: Vec<ObservationView>,
    pub open_incident_ids: Vec<String>,
    /// Populated when the caller passed `--since` (and optionally `--until`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<WindowSummary>,
}

impl StatusSummary {
    /// `true` when nothing is wrong right now AND (if a window was queried)
    /// nothing failed within the window. Drives the `--quiet` / `--short`
    /// exit code.
    pub fn is_healthy(&self) -> bool {
        if !self.current_issues.is_empty() {
            return false;
        }
        if let Some(w) = &self.window {
            if w.total_failures > 0 {
                return false;
            }
        }
        true
    }

    /// One-line summary suitable for `--short`.
    pub fn short_line(&self) -> String {
        let prefix = if self.is_healthy() { "✓" } else { "✗" };
        let mut parts: Vec<String> = Vec::new();
        if self.is_healthy() {
            parts.push("all healthy".to_string());
        } else {
            if !self.current_issues.is_empty() {
                let unhealthy = self
                    .current_issues
                    .iter()
                    .filter(|i| {
                        matches!(
                            i.kind,
                            CurrentIssueKind::NotAlive | CurrentIssueKind::Unhealthy
                        )
                    })
                    .count();
                let incidents = self
                    .current_issues
                    .iter()
                    .filter(|i| matches!(i.kind, CurrentIssueKind::OpenIncident))
                    .count();
                if unhealthy > 0 {
                    parts.push(format!(
                        "{unhealthy} unhealthy {}",
                        if unhealthy == 1 { "unit" } else { "units" }
                    ));
                }
                if incidents > 0 {
                    parts.push(format!(
                        "{incidents} open {}",
                        if incidents == 1 { "incident" } else { "incidents" }
                    ));
                }
            }
            if let Some(w) = &self.window {
                if w.total_failures > 0 {
                    let dur = w.until_ms.saturating_sub(w.since_ms) / 1000;
                    let human = humanize_secs(dur);
                    parts.push(format!("{} failures/{}", w.total_failures, human));
                }
            }
        }
        format!("{prefix} {}: {}", self.device, parts.join(", "))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ObservationView {
    pub unit: String,
    pub alive: bool,
    pub healthy: bool,
    pub crashes: u32,
    pub unhealthy_checks: u32,
}

/// Build the snapshot (no window). `device_name` is just the label used in
/// output — the function is pure.
pub fn summarize(device_name: &str, status: &DeviceStatus) -> StatusSummary {
    let mut issues: Vec<CurrentIssue> = Vec::new();
    let mut observations: Vec<ObservationView> = Vec::with_capacity(status.observations.len());

    for obs in &status.observations {
        observations.push(ObservationView {
            unit: obs.name.clone(),
            alive: obs.alive,
            healthy: obs.healthy,
            crashes: obs.crashes,
            unhealthy_checks: obs.unhealthy_checks,
        });
        if !obs.alive {
            issues.push(CurrentIssue {
                unit: obs.name.clone(),
                kind: CurrentIssueKind::NotAlive,
                detail: format!("{} crashes recorded", obs.crashes),
            });
        } else if !obs.healthy {
            issues.push(CurrentIssue {
                unit: obs.name.clone(),
                kind: CurrentIssueKind::Unhealthy,
                detail: format!("{} unhealthy checks recorded", obs.unhealthy_checks),
            });
        }
    }

    let mut incident_ids: Vec<String> = Vec::new();
    for inc in &status.incidents {
        // Open = end_time is empty / zero / not-set. The server stores
        // end_time as String, so "open" is the empty string here.
        if inc.end_time.is_empty() {
            issues.push(CurrentIssue {
                unit: "(device)".to_string(),
                kind: CurrentIssueKind::OpenIncident,
                detail: format!("incident {} opened {}", inc.id, inc.start_time),
            });
            incident_ids.push(inc.id.clone());
        }
    }

    StatusSummary {
        device: device_name.to_string(),
        current_issues: issues,
        observations,
        open_incident_ids: incident_ids,
        window: None,
    }
}

/// Layer a windowed event aggregate onto an existing snapshot. Events must
/// already have been filtered to the desired window — this function only
/// counts and groups them.
pub fn attach_window(
    summary: &mut StatusSummary,
    events: &[UnitEvent],
    since_ms: u64,
    until_ms: u64,
) {
    let mut per_unit: BTreeMap<(String, &'static str), UnitWindow> = BTreeMap::new();
    let mut total_failures: u32 = 0;
    let total_events = events.len() as u32;

    for ev in events {
        let category_label: &'static str = match ev.category {
            EventCategory::Service => "service",
            EventCategory::Job => "job",
            EventCategory::Deployment => "deployment",
        };
        let unit_label = ev
            .unit_id
            .clone()
            .unwrap_or_else(|| "(device)".to_string());
        let entry = per_unit
            .entry((unit_label.clone(), category_label))
            .or_insert_with(|| UnitWindow {
                unit: unit_label,
                category: category_label.to_string(),
                ..Default::default()
            });
        if ev.success {
            entry.successes += 1;
        } else {
            entry.failures += 1;
            total_failures += 1;
        }
        if matches!(ev.sub_kind, EventSubKind::Step { is_undo: false, .. }) {
            entry.starts += 1;
        }
        match &mut entry.last_event_ms {
            Some(t) if *t >= ev.ts => {}
            slot => *slot = Some(ev.ts),
        }
    }

    summary.window = Some(WindowSummary {
        since_ms,
        until_ms,
        units: per_unit.into_values().collect(),
        total_failures,
        total_events,
    });
}

fn humanize_secs(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86_400)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use m87_shared::device::{IncidentInfo, ObserveStatus};
    use m87_shared::deploy_spec::{
        DeployReport, DeployReportKind, JobRunReport, JobRunStatus, RunState, StepReport,
    };

    fn obs(name: &str, alive: bool, healthy: bool, crashes: u32, unhealthy: u32) -> ObserveStatus {
        ObserveStatus {
            name: name.into(),
            alive,
            healthy,
            crashes,
            unhealthy_checks: unhealthy,
        }
    }

    fn step(unit: &str, ts: u64, success: bool) -> DeployReport {
        DeployReport {
            device_id: "dev".into(),
            revision_id: "rev1".into(),
            kind: DeployReportKind::StepReport(StepReport {
                revision_id: "rev1".into(),
                run_id: unit.into(),
                name: Some("start".into()),
                attempts: 1,
                exit_code: Some(if success { 0 } else { 1 }),
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

    fn observe(unit: &str, ts: u64, healthy: bool) -> DeployReport {
        DeployReport {
            device_id: "dev".into(),
            revision_id: "rev1".into(),
            kind: DeployReportKind::RunState(RunState {
                run_id: unit.into(),
                revision_id: "rev1".into(),
                healthy: Some(healthy),
                alive: None,
                report_time: ts,
                log_tail: None,
            }),
            expires_at: None,
            created_at: ts,
        }
    }

    fn job(def: &str, run: &str, ts: u64, ok: bool) -> DeployReport {
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
                error: None,
            }),
            expires_at: None,
            created_at: ts,
        }
    }

    #[test]
    fn summarize_healthy_status_yields_no_issues() {
        let ds = DeviceStatus {
            observations: vec![obs("web", true, true, 0, 0)],
            incidents: vec![],
            device_id: None,
        };
        let summary = summarize("dev1", &ds);
        assert!(summary.current_issues.is_empty());
        assert!(summary.is_healthy());
        assert_eq!(summary.short_line(), "✓ dev1: all healthy");
    }

    #[test]
    fn summarize_unhealthy_observe_flagged() {
        let ds = DeviceStatus {
            observations: vec![obs("web", true, false, 0, 3)],
            incidents: vec![],
            device_id: None,
        };
        let summary = summarize("dev1", &ds);
        assert_eq!(summary.current_issues.len(), 1);
        assert_eq!(summary.current_issues[0].kind, CurrentIssueKind::Unhealthy);
        assert!(!summary.is_healthy());
        let s = summary.short_line();
        assert!(s.starts_with("✗ dev1"));
        assert!(s.contains("unhealthy"));
    }

    #[test]
    fn summarize_dead_observe_flagged() {
        let ds = DeviceStatus {
            observations: vec![obs("web", false, false, 2, 5)],
            incidents: vec![],
            device_id: None,
        };
        let summary = summarize("dev1", &ds);
        // NotAlive takes precedence over Unhealthy when both fail.
        assert_eq!(summary.current_issues.len(), 1);
        assert_eq!(summary.current_issues[0].kind, CurrentIssueKind::NotAlive);
    }

    #[test]
    fn summarize_open_incident_flagged_closed_ignored() {
        let ds = DeviceStatus {
            observations: vec![obs("web", true, true, 0, 0)],
            incidents: vec![
                IncidentInfo {
                    id: "open-1".into(),
                    start_time: "now".into(),
                    end_time: "".into(),
                },
                IncidentInfo {
                    id: "closed-1".into(),
                    start_time: "earlier".into(),
                    end_time: "now".into(),
                },
            ],
            device_id: None,
        };
        let summary = summarize("dev1", &ds);
        assert_eq!(summary.current_issues.len(), 1);
        assert_eq!(summary.open_incident_ids, vec!["open-1".to_string()]);
        assert!(!summary.is_healthy());
    }

    #[test]
    fn attach_window_counts_per_unit() {
        let mut summary = summarize(
            "dev1",
            &DeviceStatus {
                observations: vec![obs("web", true, true, 0, 0)],
                incidents: vec![],
                device_id: None,
            },
        );
        let reports = vec![
            step("web", 1000, true),
            step("web", 2000, false),
            observe("web", 3000, false),
            job("migrate", "run-1", 4000, true),
            job("migrate", "run-2", 5000, false),
        ];
        let events: Vec<UnitEvent> = reports.iter().map(UnitEvent::from_report).collect();
        attach_window(&mut summary, &events, 1000, 5000);

        let window = summary.window.as_ref().unwrap();
        assert_eq!(window.total_events, 5);
        assert_eq!(window.total_failures, 3);
        // Service unit "web": 2 failures (step + observe) + 1 success
        let web = window
            .units
            .iter()
            .find(|u| u.unit == "web" && u.category == "service")
            .expect("web service window");
        assert_eq!(web.failures, 2);
        assert_eq!(web.successes, 1);
        assert_eq!(web.starts, 2);
        // Job "migrate": 1 success + 1 failure
        let mig = window
            .units
            .iter()
            .find(|u| u.unit == "migrate" && u.category == "job")
            .expect("migrate job window");
        assert_eq!(mig.failures, 1);
        assert_eq!(mig.successes, 1);
        assert!(!summary.is_healthy());
    }

    #[test]
    fn window_with_no_failures_keeps_summary_healthy() {
        let mut summary = summarize(
            "dev1",
            &DeviceStatus {
                observations: vec![obs("web", true, true, 0, 0)],
                incidents: vec![],
                device_id: None,
            },
        );
        let events: Vec<UnitEvent> = vec![step("web", 100, true), observe("web", 200, true)]
            .iter()
            .map(UnitEvent::from_report)
            .collect();
        attach_window(&mut summary, &events, 0, 1000);
        assert!(summary.is_healthy());
    }

    #[test]
    fn short_line_with_window_failures() {
        let mut summary = summarize(
            "dev1",
            &DeviceStatus {
                observations: vec![obs("web", true, true, 0, 0)],
                incidents: vec![],
                device_id: None,
            },
        );
        let reports: Vec<UnitEvent> = vec![
            step("web", 1000, false),
            step("web", 2000, false),
            step("api", 3000, false),
        ]
        .iter()
        .map(UnitEvent::from_report)
        .collect();
        attach_window(&mut summary, &reports, 0, 3_600_000); // 1h window
        let s = summary.short_line();
        assert!(s.starts_with("✗ dev1"));
        assert!(s.contains("3 failures"));
        assert!(s.contains("1h"));
    }
}
