use m87_shared::deploy_spec::{
    DeploymentRevision, DeploymentStatusSnapshot, JobRun, JobRunStatus, Outcome, RunStatus,
    StepState, UnitKind,
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
    // Delegate to the typed listing functions for a unified view
    print_services_list(rev);
    if !rev.observers.is_empty() {
        println!();
        print_observers_list(rev);
    }
    if !rev.jobs.is_empty() {
        println!();
        print_job_defs_list(rev);
    }
}

fn _print_revision_short_detail_old(rev: &DeploymentRevision) {
    if !rev.services.is_empty() {
        println!(
            "{:<36} {:>8} {:>8} {:>8} {:>8}",
            "SERVICE ID", "LIFECYCLE", "STEPS", "OBSERVE", "FILES"
        );
        for svc in &rev.services {
            println!(
                "  {:<36} {:>8} {:>8} {:>8} {:>8}",
                svc.id,
                svc.lifecycle,
                svc.steps.len(),
                svc.observe.is_some(),
                svc.files.len()
            );
        }
    }
    if !rev.observers.is_empty() {
        println!(
            "{:<36} {:>8} {:>8} {:>8}",
            "OBSERVER ID", "LIFECYCLE", "OBSERVE", "FILES"
        );
        for obs in &rev.observers {
            println!(
                "  {:<36} {:>8} {:>8} {:>8}",
                obs.id,
                obs.lifecycle,
                obs.observe.is_some(),
                obs.files.len()
            );
        }
    }
    if !rev.jobs.is_empty() {
        println!(
            "{:<36} {:>8} {:>8} {:>8}",
            "JOB ID", "LIFECYCLE", "STEPS", "FILES"
        );
        for job in &rev.jobs {
            println!(
                "  {:<36} {:>8} {:>8} {:>8}",
                job.id,
                job.lifecycle,
                job.steps.len(),
                job.files.len()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Services / observers / job defs listing
// ---------------------------------------------------------------------------

pub fn print_services_list(rev: &DeploymentRevision) {
    if rev.services.is_empty() {
        println!("No services in active deployment.");
        return;
    }
    println!(
        "{:<36} {:>10} {:>6} {:>8} {:>6}",
        "SERVICE ID", "LIFECYCLE", "STEPS", "OBSERVE", "FILES"
    );
    for svc in &rev.services {
        println!(
            "  {:<36} {:>10} {:>6} {:>8} {:>6}",
            svc.id,
            svc.lifecycle,
            svc.steps.len(),
            svc.observe.is_some(),
            svc.files.len()
        );
    }
}

pub fn print_observers_list(rev: &DeploymentRevision) {
    if rev.observers.is_empty() {
        println!("No observers in active deployment.");
        return;
    }
    println!(
        "{:<36} {:>10} {:>8} {:>6}",
        "OBSERVER ID", "LIFECYCLE", "OBSERVE", "FILES"
    );
    for obs in &rev.observers {
        println!(
            "  {:<36} {:>10} {:>8} {:>6}",
            obs.id,
            obs.lifecycle,
            obs.observe.is_some(),
            obs.files.len()
        );
    }
}

pub fn print_job_defs_list(rev: &DeploymentRevision) {
    if rev.jobs.is_empty() {
        println!("No job definitions in active deployment.");
        return;
    }
    println!(
        "{:<36} {:>10} {:>6} {:>6}",
        "JOB ID", "LIFECYCLE", "STEPS", "FILES"
    );
    for jd in &rev.jobs {
        println!(
            "  {:<36} {:>10} {:>6} {:>6}",
            jd.id,
            jd.lifecycle,
            jd.steps.len(),
            jd.files.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Job run display
// ---------------------------------------------------------------------------

fn job_run_status_str(s: &JobRunStatus) -> &'static str {
    match s {
        JobRunStatus::Queued => "queued",
        JobRunStatus::Running => "running",
        JobRunStatus::Success => "✓ success",
        JobRunStatus::Failed => "✗ failed",
    }
}

fn job_run_status_color(s: &JobRunStatus) -> helper::AnsiColor {
    match s {
        JobRunStatus::Queued => helper::AnsiColor::Dim,
        JobRunStatus::Running => helper::AnsiColor::Yellow,
        JobRunStatus::Success => helper::AnsiColor::Green,
        JobRunStatus::Failed => helper::AnsiColor::Red,
    }
}

pub fn print_job_run(run: &JobRun) {
    let opts = helper::RenderOpts::default();
    let term_w = helper::terminal_width().unwrap_or(96).max(60);
    let status = job_run_status_str(&run.status);
    let status_colored =
        helper::colorize(opts.use_color, status, job_run_status_color(&run.status));

    println!("{}", helper::kv_line(term_w, "run_id", &run.run_id, &opts));
    println!("{}", helper::kv_line(term_w, "job", &run.job_def_id, &opts));
    println!(
        "{}",
        helper::kv_line(term_w, "revision", &run.revision_id, &opts)
    );
    println!(
        "{}",
        helper::kv_line(term_w, "status", &status_colored, &opts)
    );

    let enqueued = helper::format_time(run.enqueued_at, false);
    println!("{}", helper::kv_line(term_w, "enqueued", &enqueued, &opts));

    if let Some(started) = run.started_at {
        println!(
            "{}",
            helper::kv_line(
                term_w,
                "started",
                &helper::format_time(started, false),
                &opts
            )
        );
    }
    if let Some(completed) = run.completed_at {
        println!(
            "{}",
            helper::kv_line(
                term_w,
                "completed",
                &helper::format_time(completed, false),
                &opts
            )
        );
    }
    if let Some(err) = &run.error {
        println!("{}", helper::kv_line(term_w, "error", err, &opts));
    }
    if !run.env_overrides.is_empty() {
        let pairs: Vec<String> = run
            .env_overrides
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        println!(
            "{}",
            helper::kv_line(term_w, "env", &pairs.join("  "), &opts)
        );
    }
}

pub fn print_job_run_list(runs: &[JobRun]) {
    if runs.is_empty() {
        println!("No job runs found.");
        return;
    }

    let opts = helper::RenderOpts::default();
    println!(
        "{:<36} {:<20} {:>10} {:>24}",
        "RUN ID", "JOB", "STATUS", "ENQUEUED"
    );
    for run in runs {
        let status = job_run_status_str(&run.status);
        let status_colored =
            helper::colorize(opts.use_color, status, job_run_status_color(&run.status));
        let enqueued = helper::format_time(run.enqueued_at, false);
        println!(
            "  {:<36} {:<20} {:>10} {:>24}",
            run.run_id,
            helper::truncate_visible(&run.job_def_id, 18),
            status_colored,
            enqueued
        );
    }
}

pub fn print_deployment_status_snapshot(
    snap: &DeploymentStatusSnapshot,
    opts: &helper::RenderOpts,
) {
    let term_w = helper::terminal_width().unwrap_or(96).max(60);

    let steps_table = helper::Table::new(
        term_w,
        2,
        vec![
            helper::ColSpec {
                title: "",
                min: 2,
                max: Some(2),
                weight: 0,
                align: helper::Align::Left,
                wrap: false,
            },
            helper::ColSpec {
                title: "STEP",
                min: 8,
                max: Some(28),
                weight: 2,
                align: helper::Align::Left,
                wrap: true,
            },
            helper::ColSpec {
                title: "STATUS",
                min: 8,
                max: Some(12),
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

    let mut out = String::new();

    // header
    out.push_str(&helper::kv_line(
        term_w,
        "deployment",
        &helper::bold(&snap.revision_id),
        opts,
    ));
    out.push('\n');

    let status_txt = format!("{} {}", glyph_for_outcome(&snap.outcome), snap.outcome);
    let status_colored = helper::colorize(opts.use_color, &status_txt, status_color(&snap.outcome));
    out.push_str(&helper::kv_line(term_w, "status", &status_colored, opts));
    out.push('\n');

    if snap.dirty {
        out.push_str(&helper::kv_line(
            term_w,
            "dirty",
            &helper::colorize(opts.use_color, "true", helper::AnsiColor::Red),
            opts,
        ));
        out.push('\n');
    }

    if let Some(e) = snap
        .error
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        out.push_str(&helper::kv_line(term_w, "error", e, opts));
        out.push('\n');
    }

    if !snap.runs.is_empty() {
        out.push_str(&helper::separator_line(term_w, opts));
        out.push('\n');
        out.push('\n');
    }

    for run in &snap.runs {
        let enabled = if run.enabled {
            helper::colorize(opts.use_color, "✓ enabled", helper::AnsiColor::Green)
        } else {
            helper::colorize(opts.use_color, "✗ disabled", helper::AnsiColor::Red)
        };

        let kind_txt = match run.unit_kind {
            UnitKind::Service => "service",
            UnitKind::Observer => "observer",
            UnitKind::Job => "job",
        };
        let outcome_txt = match run.outcome {
            Outcome::Success => "✓ success",
            Outcome::Failed => "✗ failure",
            Outcome::Unknown => "? unknown",
        };
        let outcome_colored =
            helper::colorize(opts.use_color, outcome_txt, status_color(&run.outcome));

        let last = if run.last_update == 0 {
            "-".to_string()
        } else {
            helper::format_time(run.last_update, opts.time_only)
        };

        let (steps_ok, steps_total, max_attempts, undone_steps) = step_stats_from_snapshot(run);

        let mut run_info = format!(
            "{}  [{}]  {}   last update {}   steps {}/{}  max attempts {}  undone {}",
            helper::bold(&run.run_id),
            kind_txt,
            enabled,
            last,
            steps_ok,
            steps_total,
            max_attempts,
            undone_steps
        );

        if let Some(e) = run
            .error
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            run_info.push_str(&format!("   err: {}", helper::single_line(e)));
        }

        out.push_str(&run_info);
        out.push('\n');

        if run.healthy.is_some() || run.alive.is_some() {
            out.push_str(&format!("  {}", helper::gray("observe")));
            out.push('\n');
        }

        if let Some(h) = &run.healthy {
            let s = if h.ok { "healthy" } else { "unhealthy" };
            let c = if h.ok {
                helper::AnsiColor::Green
            } else {
                helper::AnsiColor::Red
            };
            push_check_row_snapshot(
                &mut out,
                &steps_table,
                opts,
                "[health]",
                &helper::colorize(opts.use_color, s, c),
                h.report_time,
                h.log_tail.as_deref().unwrap_or(""),
                opts.show_logs_inline,
            );
        }

        if let Some(a) = &run.alive {
            let s = if a.ok { "alive" } else { "dead" };
            let c = if a.ok {
                helper::AnsiColor::Green
            } else {
                helper::AnsiColor::Red
            };
            push_check_row_snapshot(
                &mut out,
                &steps_table,
                opts,
                "[alive]",
                &helper::colorize(opts.use_color, s, c),
                a.report_time,
                a.log_tail.as_deref().unwrap_or(""),
                opts.show_logs_inline,
            );
        }

        out.push_str(&format!(
            "  {}    {}",
            helper::gray("steps"),
            outcome_colored
        ));
        out.push('\n');

        for st in &run.steps {
            // undo rows: show only if defined in spec AND executed (attempt exists) OR state not Pending
            if st.is_undo && (st.attempt.is_none() && st.state == StepState::Pending) {
                continue;
            }
            if st.is_undo && !st.defined_in_spec {
                // if you keep placeholder undo rows, don’t render them
                continue;
            }

            let (status_str, status_color) = match st.state {
                StepState::Pending => ("… pending", helper::AnsiColor::Dim),
                StepState::Running => ("… running", helper::AnsiColor::Yellow),
                StepState::Success => ("✓ ok", helper::AnsiColor::Green),
                StepState::Failed => ("✗ fail", helper::AnsiColor::Red),
                StepState::Skipped => ("↷ skipped", helper::AnsiColor::Dim),
            };
            let status_colored = helper::colorize(opts.use_color, status_str, status_color);

            let time_s = st
                .last_update
                .map(|t| helper::format_time(t, opts.time_only))
                .unwrap_or_else(|| "-".to_string());

            let mut info = String::new();
            if let Some(a) = &st.attempt {
                info.push_str(&format!("attempt {}", a.n));
                if let Some(ec) = a.exit_code {
                    info.push_str(&format!("  exit {}", ec));
                }
                if st.is_undo {
                    info.push_str("  undo");
                }
                if let Some(e) = a.error.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    info.push_str(&format!("  err: {}", helper::single_line(e)));
                }
            } else {
                info.push_str("not started");
                if st.is_undo {
                    info.push_str("  undo");
                }
            }

            let name = if st.is_undo {
                format!("{} (undo)", st.name)
            } else {
                st.name.clone()
            };

            steps_table.row(
                &mut out,
                &[
                    "",
                    &format!("  {}", helper::bold(&name)),
                    &status_colored,
                    &time_s,
                    &info,
                ],
                opts,
            );

            if opts.show_logs_inline {
                if let Some(a) = &st.attempt {
                    if let Some(tail) = a
                        .log_tail
                        .as_ref()
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                    {
                        let whitespace = format!(
                            "{}{}",
                            steps_table.get_column_width_as_whitespace(0),
                            steps_table.get_column_width_as_whitespace(1)
                        );
                        out.push_str(&format!(
                            "{} {}",
                            whitespace,
                            helper::gray(&tail.replace('\n', &format!("\n{}", whitespace)))
                        ));
                        out.push('\n');
                    }
                }
            }
        }

        out.push_str(&helper::separator_line(term_w, opts));
        out.push('\n');
    }

    if let Some(rb) = &snap.rollback {
        out.push('\n');
        out.push_str(&helper::kv_line(
            term_w,
            "rollback",
            &format!(
                "new revision {}",
                helper::bold(&rb.new_revision_id.clone().unwrap_or("None".to_string()))
            ),
            opts,
        ));
        out.push('\n');

        if let Some(t) = rb.report_time {
            out.push_str(&helper::kv_line(
                term_w,
                "time",
                &helper::format_time(t, opts.time_only),
                opts,
            ));
            out.push('\n');
        }
    }

    print!("{out}");
}

// counts ok/total over main steps, and includes undo steps only if executed
fn step_stats_from_snapshot(run: &RunStatus) -> (usize, usize, usize, usize) {
    let mut ok = 0usize;
    let mut total = 0usize;
    let mut max_attempts = 0usize;

    // main steps are expected
    for s in run.steps.iter().filter(|s| !s.is_undo) {
        total += 1;
        if s.state == StepState::Success {
            ok += 1;
        }
        max_attempts = max_attempts.max(s.attempts_total as usize);
    }

    // undo: count only if executed (attempt exists)
    let mut undone = 0usize;
    for s in run.steps.iter().filter(|s| s.is_undo) {
        if s.attempt.is_some() {
            undone += 1;
            total += 1;
            if s.state == StepState::Success {
                ok += 1;
            }
            max_attempts = max_attempts.max(s.attempts_total as usize);
        }
    }

    (ok, total, max_attempts, undone)
}

// Same formatting as your old `push_check_row`, but driven by snapshot.
fn push_check_row_snapshot(
    out: &mut String,
    table: &helper::Table,
    opts: &helper::RenderOpts,
    label: &str,
    status: &str,
    report_time: u64,
    log_tail: &str,
    show_logs_inline: bool,
) {
    let tt = helper::format_time(report_time, opts.time_only);

    table.row(
        out,
        &["", &format!("  {}", helper::bold(label)), status, &tt, ""],
        opts,
    );

    if show_logs_inline && !log_tail.trim().is_empty() {
        let whitespace = format!(
            "{}{}",
            table.get_column_width_as_whitespace(0),
            table.get_column_width_as_whitespace(1)
        );
        out.push_str(&format!(
            "{}{}",
            whitespace,
            helper::gray(&log_tail.replace('\n', &format!("\n{}", whitespace)))
        ));
        out.push('\n');
    }
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
