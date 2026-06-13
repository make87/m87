//! Time parsing helpers used by `logs` and `status` for `--since` / `--until`.
//!
//! Two shapes are accepted:
//!
//! - **Relative durations** — `30s`, `5m`, `2h`, `24h`, `7d`, `2w`. Parsed
//!   as "this much time before `now`". Suffixes are case-insensitive.
//!   Bare numbers default to seconds.
//! - **Absolute timestamps** — anything `chrono::DateTime::parse_from_rfc3339`
//!   or `NaiveDateTime`/`NaiveDate` parsers accept, e.g.
//!   `2026-05-25T13:00:00Z`, `2026-05-25T13:00:00`, or `2026-05-25`
//!   (date-only → midnight UTC).
//!
//! Results are returned as `u64` milliseconds since the Unix epoch — the
//! same unit `DeployReport::report_time` uses, so callers can compare
//! directly without conversion.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Parse a `--since` / `--until` argument into a millis-since-epoch timestamp.
///
/// `now_ms` is passed in (rather than read each call) so callers can pin a
/// consistent "now" across multiple parses inside one command invocation.
pub fn parse_time(input: &str, now_ms_value: u64) -> Result<u64> {
    let s = input.trim();
    if s.is_empty() {
        return Err(anyhow!("empty time value"));
    }

    if let Some(secs) = parse_relative_duration_secs(s) {
        return Ok(now_ms_value.saturating_sub(secs.saturating_mul(1000)));
    }

    parse_absolute(s).with_context(|| {
        format!(
            "could not parse '{s}' as either a relative duration (e.g. 30m, 24h, 7d) \
             or an absolute timestamp (e.g. 2026-05-25 or 2026-05-25T13:00:00Z)"
        )
    })
}

/// Parse a duration suffix: `30s`, `5m`, `2h`, `24h`, `7d`, `2w`.
/// A bare integer with no suffix is treated as seconds.
/// Returns the duration in seconds, or `None` if the input is not a
/// relative duration.
fn parse_relative_duration_secs(s: &str) -> Option<u64> {
    let bytes = s.as_bytes();
    // Find where the digits end.
    let split = bytes
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(bytes.len());
    if split == 0 {
        return None;
    }
    let n: u64 = s[..split].parse().ok()?;
    let suffix = s[split..].trim().to_ascii_lowercase();
    let multiplier = match suffix.as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => 60 * 60,
        "d" | "day" | "days" => 60 * 60 * 24,
        "w" | "wk" | "wks" | "week" | "weeks" => 60 * 60 * 24 * 7,
        _ => return None,
    };
    Some(n.saturating_mul(multiplier))
}

fn parse_absolute(s: &str) -> Result<u64> {
    // 1) Full RFC3339 (with timezone): "2026-05-25T13:00:00Z" / "...+02:00"
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis().max(0) as u64);
    }
    // 2) Naive datetime (no timezone) — assume UTC: "2026-05-25T13:00:00"
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&ndt).timestamp_millis().max(0) as u64);
    }
    // 3) Naive datetime without seconds: "2026-05-25T13:00"
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M") {
        return Ok(Utc.from_utc_datetime(&ndt).timestamp_millis().max(0) as u64);
    }
    // 4) Date only — UTC midnight: "2026-05-25"
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let ndt = d.and_hms_opt(0, 0, 0).expect("midnight");
        return Ok(Utc.from_utc_datetime(&ndt).timestamp_millis().max(0) as u64);
    }
    Err(anyhow!("not a recognized timestamp"))
}

/// Format a millis-since-epoch timestamp as a short human-readable string
/// in UTC: `2026-05-25 14:23:01`. Returns `"-"` for 0.
pub fn format_ms(ms: u64) -> String {
    if ms == 0 {
        return "-".to_string();
    }
    let secs = (ms / 1000) as i64;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    match Utc.timestamp_opt(secs, nanos).single() {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => "-".to_string(),
    }
}

/// Format a timestamp as "Xh ago" / "Xm ago" / "Xs ago" relative to `now`.
/// Useful for `status` display where absolute timestamps are noisy.
pub fn format_ago(ms: u64, now_ms_value: u64) -> String {
    if ms == 0 || ms > now_ms_value {
        return "-".to_string();
    }
    let delta_secs = (now_ms_value - ms) / 1000;
    if delta_secs < 60 {
        format!("{delta_secs}s ago")
    } else if delta_secs < 3600 {
        format!("{}m ago", delta_secs / 60)
    } else if delta_secs < 86_400 {
        format!("{}h ago", delta_secs / 3600)
    } else {
        format!("{}d ago", delta_secs / 86_400)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000_000; // arbitrary fixed "now"

    #[test]
    fn relative_durations() {
        assert_eq!(parse_time("30s", NOW).unwrap(), NOW - 30 * 1000);
        assert_eq!(parse_time("5m", NOW).unwrap(), NOW - 5 * 60 * 1000);
        assert_eq!(parse_time("2h", NOW).unwrap(), NOW - 2 * 60 * 60 * 1000);
        assert_eq!(parse_time("24h", NOW).unwrap(), NOW - 24 * 60 * 60 * 1000);
        assert_eq!(parse_time("7d", NOW).unwrap(), NOW - 7 * 24 * 60 * 60 * 1000);
        assert_eq!(
            parse_time("2w", NOW).unwrap(),
            NOW - 2 * 7 * 24 * 60 * 60 * 1000
        );
    }

    #[test]
    fn relative_long_form() {
        assert_eq!(parse_time("30 seconds", NOW).unwrap(), NOW - 30 * 1000);
        assert_eq!(parse_time("5min", NOW).unwrap(), NOW - 5 * 60 * 1000);
        assert_eq!(
            parse_time("2 hours", NOW).unwrap(),
            NOW - 2 * 60 * 60 * 1000
        );
    }

    #[test]
    fn relative_bare_number_is_seconds() {
        assert_eq!(parse_time("90", NOW).unwrap(), NOW - 90 * 1000);
    }

    #[test]
    fn relative_is_case_insensitive() {
        assert_eq!(parse_time("1H", NOW).unwrap(), NOW - 60 * 60 * 1000);
        assert_eq!(parse_time("7D", NOW).unwrap(), NOW - 7 * 24 * 60 * 60 * 1000);
    }

    fn utc_ms(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> u64 {
        Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
            .single()
            .unwrap()
            .timestamp_millis() as u64
    }

    #[test]
    fn absolute_rfc3339_utc() {
        let t = parse_time("2026-05-25T13:00:00Z", NOW).unwrap();
        assert_eq!(t, utc_ms(2026, 5, 25, 13, 0, 0));
    }

    #[test]
    fn absolute_rfc3339_offset() {
        // 13:00 in +02:00 = 11:00 UTC
        let t = parse_time("2026-05-25T13:00:00+02:00", NOW).unwrap();
        assert_eq!(t, utc_ms(2026, 5, 25, 11, 0, 0));
    }

    #[test]
    fn absolute_naive_datetime() {
        let t = parse_time("2026-05-25T13:00:00", NOW).unwrap();
        assert_eq!(t, utc_ms(2026, 5, 25, 13, 0, 0));
    }

    #[test]
    fn absolute_naive_no_seconds() {
        let t = parse_time("2026-05-25T13:00", NOW).unwrap();
        assert_eq!(t, utc_ms(2026, 5, 25, 13, 0, 0));
    }

    #[test]
    fn absolute_date_only() {
        let t = parse_time("2026-05-25", NOW).unwrap();
        assert_eq!(t, utc_ms(2026, 5, 25, 0, 0, 0));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_time("not-a-time", NOW).is_err());
        assert!(parse_time("", NOW).is_err());
        assert!(parse_time("5x", NOW).is_err());
    }

    #[test]
    fn format_ms_basic() {
        assert_eq!(format_ms(0), "-");
        assert_eq!(format_ms(utc_ms(2026, 5, 25, 13, 0, 0)), "2026-05-25 13:00:00");
    }

    #[test]
    fn format_ago_buckets() {
        assert_eq!(format_ago(NOW - 30_000, NOW), "30s ago");
        assert_eq!(format_ago(NOW - 5 * 60_000, NOW), "5m ago");
        assert_eq!(format_ago(NOW - 2 * 60 * 60_000, NOW), "2h ago");
        assert_eq!(format_ago(NOW - 3 * 24 * 60 * 60_000, NOW), "3d ago");
        assert_eq!(format_ago(0, NOW), "-");
        // Future timestamp should not produce nonsense.
        assert_eq!(format_ago(NOW + 1000, NOW), "-");
    }
}
