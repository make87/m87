//! Pretty/JSON rendering for the unified `m87 <dev> logs` history view.

use crate::device::events::{EventCategory, EventSubKind, ObserveKind, UnitEvent};
use crate::util::time::format_ms;

const RESET: &str = "\x1b[0m";
const GREY: &str = "\x1b[90m";
const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";

/// Render events as a fixed-width human-readable table.
///
/// `include_log_tail` controls whether to print the captured stdout/stderr
/// tail beneath each row (the `--logs` flag).
pub fn print_events_table(events: &[UnitEvent], include_log_tail: bool) {
    if events.is_empty() {
        println!("(no events match the filter)");
        return;
    }

    println!(
        "{:<20}  {:<4}  {:<22}  {:<24}  {}",
        "TIME", "KIND", "UNIT", "SOURCE", "STATUS"
    );
    for ev in events {
        let kind_glyph = match ev.category {
            EventCategory::Service => "svc",
            EventCategory::Job => "job",
            EventCategory::Deployment => "dep",
        };
        let status = status_cell(ev);
        let source = source_cell(ev);
        let unit = unit_cell(ev);

        println!(
            "{grey}{ts:<20}{reset}  {kind:<4}  {unit:<22}  {source:<24}  {status}",
            grey = GREY,
            reset = RESET,
            ts = format_ms(ev.ts),
            kind = kind_glyph,
            unit = unit,
            source = source,
            status = status,
        );

        if include_log_tail {
            if let Some(tail) = &ev.log_tail {
                if !tail.trim().is_empty() {
                    for line in tail.lines() {
                        println!("    {GREY}|{RESET} {line}");
                    }
                }
            }
            if let Some(err) = &ev.error {
                println!("    {RED}|{RESET} {err}");
            }
        }
    }
}

/// Render events as NDJSON (one JSON object per line).
pub fn print_events_ndjson(events: &[UnitEvent]) {
    for ev in events {
        match serde_json::to_string(ev) {
            Ok(line) => println!("{line}"),
            Err(e) => eprintln!("(json serialize failed: {e})"),
        }
    }
}

fn status_cell(ev: &UnitEvent) -> String {
    let (glyph, color) = if ev.success {
        ("✓", GREEN)
    } else {
        ("✗", RED)
    };
    let extras = match (&ev.sub_kind, ev.exit_code) {
        (EventSubKind::Step { .. }, Some(code)) if !ev.success => format!(" exit {code}"),
        (EventSubKind::JobTerminal { status }, _) => format!(" {status:?}").to_lowercase(),
        _ => String::new(),
    };
    let info_color = if ev.success { CYAN } else { YELLOW };
    format!("{color}{glyph}{reset}{info_color}{extras}{reset}",
        color = color,
        reset = RESET,
        info_color = info_color,
        extras = extras,
        glyph = glyph,
    )
}

fn source_cell(ev: &UnitEvent) -> String {
    match &ev.sub_kind {
        EventSubKind::Step { name, is_undo } => {
            let prefix = if *is_undo { "undo/" } else { "step/" };
            let suffix = name.as_deref().unwrap_or("-");
            format!("{prefix}{suffix}")
        }
        EventSubKind::Observe { kind } => match kind {
            ObserveKind::Liveness => "observe/liveness".to_string(),
            ObserveKind::Health => "observe/health".to_string(),
        },
        EventSubKind::RunOutcome => "run/outcome".to_string(),
        EventSubKind::JobTerminal { .. } => "job/terminal".to_string(),
        EventSubKind::RevisionOutcome => "revision/outcome".to_string(),
        EventSubKind::Rollback => "revision/rollback".to_string(),
    }
}

fn unit_cell(ev: &UnitEvent) -> String {
    match (&ev.unit_id, &ev.run_id) {
        (Some(u), Some(r)) => {
            // Show as `<unit>/<short-run>` so job runs are visually grouped
            // under their def.
            let short = if r.len() > 8 { &r[..8] } else { r.as_str() };
            format!("{u}/{short}")
        }
        (Some(u), None) => u.clone(),
        (None, Some(r)) => r.clone(),
        (None, None) => "-".to_string(),
    }
}
