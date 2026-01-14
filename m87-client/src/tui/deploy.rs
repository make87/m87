use std::collections::HashMap;

use m87_shared::deploy_spec::{
    DeployReport, DeployReportKind, DeploymentRevision, DeploymentRevisionReport, Outcome,
    RollbackReport, RunReport, RunState, StepReport,
};

use crate::tui::helper;

pub fn print_revision_list_header() {
    println!("{:<36} {:>4} {:>8}", "REVISION", "JOBS", "ROLLBACK");
}

pub fn print_revision_short(rev: &DeploymentRevision) {
    println!(
        "{:<36} {:>4} {:>8}",
        rev.id.as_deref().unwrap_or("<none>"),
        rev.jobs.len(),
        if rev.rollback.is_some() { "yes" } else { "no" }
    );
}

pub fn print_revision_list_short(revs: &[DeploymentRevision]) {
    print_revision_list_header();
    for rev in revs {
        print_revision_short(rev);
    }
}

pub fn print_revision_verbose(rev: &DeploymentRevision) {
    match rev.to_yaml() {
        Ok(yaml) => print!("{yaml}"),
        Err(e) => eprintln!("failed to serialize revision to yaml: {e}"),
    }
}

pub fn print_revision_short_detail(rev: &DeploymentRevision) {
    // print header
    println!(
        "{:<36} {:>8} {:>8} {:>8} {:>8}",
        "JOB ID", "ENABLED", "STEPS", "OBSERVE", "FILES"
    );
    for job in &rev.jobs {
        println!(
            "  {:<36} {:>8} {:>8} {:>8} {:>8}",
            job.id,
            job.enabled,
            job.steps.len(),
            job.observe.is_some(),
            job.files.len()
        );
    }
}

#[derive(Default)]
struct RunAgg {
    run_id: String,
    run_report: Option<RunReport>,
    steps: Vec<StepReport>,
    states: Vec<RunState>,
    last_time: u64,
}

pub fn print_deployment_reports(
    reports: &[DeployReport],
    deployment: &DeploymentRevision,
    opts: &helper::RenderOpts,
) {
    let term_w = helper::terminal_width().unwrap_or(96).max(60);

    // Steps table: cap the NAME column so it can't eat everything.
    let steps_table = helper::Table::new(
        term_w,
        2,
        vec![
            helper::ColSpec {
                title: "",
                min: 2,
                max: Some(2),
                weight: 1,
                align: helper::Align::Left,
                wrap: true,
            },
            helper::ColSpec {
                title: "STEP",
                min: 8,
                max: Some(26),
                weight: 2,
                align: helper::Align::Left,
                wrap: true,
            },
            helper::ColSpec {
                title: "STATUS",
                min: 8,
                max: Some(10),
                weight: 0,
                align: helper::Align::Left,
                wrap: false,
            },
            helper::ColSpec {
                title: "TIME",
                min: 8,
                max: Some(20),
                weight: 0,
                align: helper::Align::Left,
                wrap: false,
            },
            helper::ColSpec {
                title: "INFO",
                min: 12,
                max: None,
                weight: 5,
                align: helper::Align::Left,
                wrap: true,
            },
        ],
    );

    let rb_table = helper::Table::new(
        term_w,
        2,
        vec![
            helper::ColSpec {
                title: "ROLLBACK",
                min: 10,
                max: Some(10),
                weight: 1,
                align: helper::Align::Left,
                wrap: true,
            },
            helper::ColSpec {
                title: "STATUS",
                min: 8,
                max: Some(10),
                weight: 0,
                align: helper::Align::Left,
                wrap: false,
            },
            helper::ColSpec {
                title: "TIME",
                min: 8,
                max: Some(20),
                weight: 0,
                align: helper::Align::Left,
                wrap: false,
            },
            helper::ColSpec {
                title: "INFO",
                min: 12,
                max: None,
                weight: 6,
                align: helper::Align::Left,
                wrap: true,
            },
        ],
    );

    // Aggregate
    let mut rev: Option<DeploymentRevisionReport> = None;
    let mut rollback: Option<RollbackReport> = None;
    let mut runs: HashMap<String, RunAgg> = HashMap::new();

    for r in reports {
        match &r.kind {
            DeployReportKind::DeploymentRevisionReport(x) => rev = Some(x.clone()),
            DeployReportKind::RollbackReport(x) => rollback = Some(x.clone()),
            DeployReportKind::RunReport(x) => {
                let e = runs.entry(x.run_id.clone()).or_insert_with(|| RunAgg {
                    run_id: x.run_id.clone(),
                    ..Default::default()
                });
                e.run_report = Some(x.clone());
            }
            DeployReportKind::StepReport(x) => {
                let e = runs.entry(x.run_id.clone()).or_insert_with(|| RunAgg {
                    run_id: x.run_id.clone(),
                    ..Default::default()
                });
                e.last_time = e.last_time.max(x.report_time);
                e.steps.push(x.clone());
            }
            DeployReportKind::RunState(x) => {
                let e = runs.entry(x.run_id.clone()).or_insert_with(|| RunAgg {
                    run_id: x.run_id.clone(),
                    ..Default::default()
                });
                e.last_time = e.last_time.max(x.report_time as u64);
                e.states.push(x.clone());
            }
        }
    }

    let mut run_list: Vec<RunAgg> = runs
        .into_values()
        .map(|mut ra| {
            ra.steps.sort_by_key(|s| s.report_time);
            ra.states.sort_by_key(|s| s.report_time);
            ra
        })
        .collect();

    run_list.sort_by(|a1, a2| {
        let o1 = run_outcome(a1);
        let o2 = run_outcome(a2);
        outcome_rank(o1)
            .cmp(&outcome_rank(o2))
            .then_with(|| a2.last_time.cmp(&a1.last_time))
            .then_with(|| a1.run_id.cmp(&a2.run_id))
    });

    let mut out = String::new();

    // REVISION header (label + compact info)
    let (rev_id, rev_status, rev_dirty, rev_err) = if let Some(r) = &rev {
        (
            r.revision_id.clone(),
            r.outcome.clone(),
            Some(r.dirty),
            r.error.clone(),
        )
    } else {
        let rid = reports
            .first()
            .map(|x| x.revision_id.clone())
            .unwrap_or_else(|| "unknown".to_string());
        (rid, Outcome::Unknown, None, None)
    };

    let status_txt = format!("{} {}", glyph_for_outcome(&rev_status), rev_status);
    let status_colored = helper::colorize(opts.use_color, &status_txt, status_color(&rev_status));

    let t = helper::format_time(best_updated_time(&run_list, reports), opts.time_only);

    let dirty_flag = if rev_dirty.unwrap_or(false) {
        helper::colorize(opts.use_color, "dirty", helper::AnsiColor::Red)
    } else {
        "".to_string()
    };

    out.push_str(&helper::kv_line(
        term_w,
        "deployment",
        &helper::bold(&rev_id),
        opts,
    ));
    out.push('\n');

    // status/time line (separate so id line stays clean)
    let st = format!(
        "{}   {}{}",
        status_colored,
        t,
        if dirty_flag.is_empty() { "" } else { "   " }
    );
    let st = if dirty_flag.is_empty() {
        st
    } else {
        format!("{st}{dirty_flag}")
    };
    out.push_str(&helper::kv_line(term_w, "status", &st, opts));
    out.push('\n');

    if let Some(err) = rev_err {
        out.push_str(&helper::kv_line(term_w, "error", err.trim(), opts));
        out.push('\n');
    }

    if !run_list.is_empty() {
        out.push_str(&helper::separator_line(term_w, opts));
        out.push('\n');
        out.push('\n');
    }

    // RUN blocks
    for ra in &run_list {
        let outcome = run_outcome(ra);
        let (alive, healthy) = latest_alive_healthy(&ra.states);

        let alive_s = match &alive {
            Some((_, true)) => helper::colorize(opts.use_color, "alive", helper::AnsiColor::Green),
            Some((_, false)) => helper::colorize(opts.use_color, "dead", helper::AnsiColor::Red),
            None => helper::colorize(opts.use_color, "unknown alive", helper::AnsiColor::Dim),
        };
        let healthy_s = match &healthy {
            Some((_, true)) => {
                helper::colorize(opts.use_color, "healthy", helper::AnsiColor::Green)
            }
            Some((_, false)) => {
                helper::colorize(opts.use_color, "unhealthy", helper::AnsiColor::Red)
            }
            None => "healthy=?".to_string(),
        };

        let status = match outcome {
            Outcome::Success => "✓ success",
            Outcome::Failed => "✗ failure",
            Outcome::Unknown => "? unknown",
        };
        let status_colored = helper::colorize(opts.use_color, status, status_color(&outcome));

        let enabled = match deployment.get_job_by_id(&ra.run_id) {
            Some(job) => match job.enabled {
                true => helper::colorize(opts.use_color, "✓ enabled", helper::AnsiColor::Green),
                false => helper::colorize(opts.use_color, "✗ disabled", helper::AnsiColor::Red),
            },
            None => {
                tracing::warn!("Job not found for run ID: {} in {}", ra.run_id, deployment);
                continue;
            }
        };

        let last = helper::format_time(ra.last_time, opts.time_only);
        let (steps_ok, steps_total, retry_count, undo_count) = step_stats(&ra.steps);

        let mut run_info = format!(
            "{}  {}   last update {}   steps {}/{}  max retry {}  undone {}",
            &helper::bold(&ra.run_id),
            enabled,
            last,
            steps_ok,
            steps_total,
            retry_count,
            undo_count
        );
        if let Some(e) = ra.run_report.as_ref().and_then(|r| r.error.as_ref()) {
            run_info.push_str(&format!("   err: {}", helper::single_line(e)));
        }

        // Run line (own line, not table)
        out.push_str(&run_info);
        out.push('\n');

        if healthy.is_some() || alive.is_some() {
            out.push_str(&format!("  {}", helper::gray("observe")));
            out.push('\n');
        }

        if let Some((s, _)) = healthy {
            push_check_row(
                &mut out,
                &steps_table,
                opts,
                "[health]",
                &healthy_s,
                s.report_time,
                s.log_tail,
                opts.show_logs_inline,
            );
        }
        if let Some((s, _)) = alive {
            push_check_row(
                &mut out,
                &steps_table,
                opts,
                "[alive]",
                &alive_s,
                s.report_time,
                s.log_tail,
                opts.show_logs_inline,
            );
        }
        out.push_str(&format!(
            "  {}    {}",
            helper::gray("steps"),
            status_colored
        ));
        out.push('\n');

        // Steps table
        // steps_table.header(&mut out, opts);

        let mut attempt_map: HashMap<(String, bool), u32> = HashMap::new();
        for s in &ra.steps {
            let name = s.name.clone().unwrap_or_else(|| "step".into());
            let key = (name.clone(), s.is_undo);
            let next = attempt_map.get(&key).copied().unwrap_or(0) + 1;
            attempt_map.insert(key, next);

            let step_status = if s.success { "✓ ok" } else { "✗ fail" };
            let step_status_colored = helper::colorize(
                opts.use_color,
                step_status,
                if s.success {
                    helper::AnsiColor::Green
                } else {
                    helper::AnsiColor::Red
                },
            );

            let tt = helper::format_time(s.report_time, opts.time_only);

            let mut sinfo = format!("attempt {}", next);
            if let Some(ec) = s.exit_code {
                sinfo.push_str(&format!("  exit {}", ec));
            }
            if s.is_undo {
                sinfo.push_str("  undo");
            }
            if let Some(e) = &s.error {
                if !e.trim().is_empty() {
                    sinfo.push_str(&format!("  err: {}", helper::single_line(e)));
                }
            }

            steps_table.row(
                &mut out,
                &[
                    "",
                    &format!(
                        "  {}",
                        helper::bold(&name) + if s.is_undo { " (undo)" } else { "" }
                    ),
                    &step_status_colored,
                    &tt,
                    &sinfo,
                ],
                opts,
            );

            if opts.show_logs_inline && !s.log_tail.trim().is_empty() {
                let whitespace = format!(
                    "{}{}",
                    steps_table.get_column_width_as_whitespace(0),
                    steps_table.get_column_width_as_whitespace(1)
                );

                out.push_str(&format!(
                    "{}{}",
                    whitespace,
                    helper::gray(&s.log_tail.replace("\n", &format!("\n{}", whitespace)))
                ));
                out.push('\n');
                // let hint = helper::log_hint(&s.log_tail, opts.max_log_hint);
                // steps_table.row(&mut out, &["    log tail", "", "", &hint], opts);
            }
        }

        out.push_str(&helper::separator_line(term_w, opts));
        out.push('\n');
    }

    if let Some(rb) = rollback {
        // Rollback section
        rb_table.header(&mut out, opts);
        let st = if rb.success {
            "✓ success"
        } else {
            "✗ failure"
        };
        let st = helper::colorize(
            opts.use_color,
            st,
            if rb.success {
                helper::AnsiColor::Green
            } else {
                helper::AnsiColor::Red
            },
        );

        let t = ""; // if you have a rollback time later, put it here
        let mut info = format!("undone_steps={:?}", rb.undone_steps);
        if let Some(e) = rb.error.as_ref() {
            info.push_str(&format!("  err: {}", helper::single_line(e)));
        }
        if !rb.log_tail.trim().is_empty() {
            info.push_str(&format!(
                "  log: {}",
                helper::log_hint(&rb.log_tail, opts.max_log_hint)
            ));
        }

        rb_table.row(&mut out, &["rollback", &st, t, &info], opts);
    }

    tracing::info!("{out}");
}

fn run_outcome(ra: &RunAgg) -> Outcome {
    if let Some(rr) = &ra.run_report {
        return rr.outcome.clone();
    }
    if ra.steps.iter().any(|s| !s.success) {
        return Outcome::Failed;
    }
    Outcome::Unknown
}

fn outcome_rank(o: Outcome) -> u8 {
    match o {
        Outcome::Failed => 0,
        Outcome::Unknown => 1,
        Outcome::Success => 2,
    }
}

fn latest_alive_healthy(
    states: &[RunState],
) -> (Option<(RunState, bool)>, Option<(RunState, bool)>) {
    let mut alive: Option<(u64, bool, RunState)> = None;
    let mut healthy: Option<(u64, bool, RunState)> = None;

    for s in states {
        if let Some(a) = s.alive {
            if alive
                .as_ref()
                .map(|(t, _, _)| s.report_time >= *t)
                .unwrap_or(true)
            {
                alive = Some((s.report_time, a, s.clone()));
            }
        }
        if let Some(h) = s.healthy {
            if healthy
                .as_ref()
                .map(|(t, _, _)| s.report_time >= *t)
                .unwrap_or(true)
            {
                healthy = Some((s.report_time, h, s.clone()));
            }
        }
    }

    (
        alive.map(|(_, v, s)| (s, v)),
        healthy.map(|(_, v, s)| (s, v)),
    )
}

fn step_stats(steps: &[StepReport]) -> (u32, u32, u32, u32) {
    let mut total = 0u32;
    let mut ok = 0u32;
    let mut undo = 0u32;

    let mut seen: HashMap<String, u32> = HashMap::new();
    let mut retries = 0u32;

    for s in steps {
        if s.is_undo {
            undo += 1;
            continue;
        }
        total += 1;
        if s.success {
            ok += 1;
        }
        let name = s.name.clone().unwrap_or_else(|| "step".into());
        let c = seen.get(&name).copied().unwrap_or(0) + 1;
        seen.insert(name, c);
    }

    for (_k, c) in seen {
        if c > 1 {
            retries += c - 1;
        }
    }

    (ok, total, retries, undo)
}

fn status_color(o: &Outcome) -> helper::AnsiColor {
    match o {
        Outcome::Success => helper::AnsiColor::Green,
        Outcome::Failed => helper::AnsiColor::Red,
        Outcome::Unknown => helper::AnsiColor::Dim,
    }
}

fn glyph_for_outcome(o: &Outcome) -> &'static str {
    match o {
        Outcome::Success => "✓",
        Outcome::Failed => "✗",
        Outcome::Unknown => "?",
    }
}

fn best_updated_time(run_list: &[RunAgg], reports: &[DeployReport]) -> u64 {
    let mut t = 0u64;
    for r in run_list {
        t = t.max(r.last_time);
    }
    if t == 0 {
        for r in reports {
            t = t.max(r.created_at);
        }
    }
    t
}

fn push_check_row<'a>(
    out: &mut String,
    steps_table: &helper::Table,
    opts: &helper::RenderOpts,
    label: &'a str,           // "[health]" / "[alive]"
    status_label: &'a str,    // status from state
    ts: u64,                  // report_time
    log_tail: Option<String>, // optional multi-line log
    show_log: bool,
) {
    let tt = helper::format_time(ts, opts.time_only);

    steps_table.row(out, &["", label, status_label, &tt], opts);

    if show_log && let Some(tail) = log_tail {
        // if is empty write "no logs gathered"
        let logs = match tail.trim().is_empty() {
            true => "No logs gathered".to_string(),
            false => tail.clone(),
        };
        let whitespace = format!(
            "{}{}",
            steps_table.get_column_width_as_whitespace(0),
            steps_table.get_column_width_as_whitespace(1)
        );

        out.push_str(&format!(
            "{}{}",
            whitespace,
            helper::gray(&logs.replace("\n", &format!("\n{}", whitespace)))
        ));
        out.push('\n');
    }
}
