use chrono::{DateTime, Utc};

use m87_shared::{
    auth::DeviceAuthRequest,
    device::{DeviceStatus, PublicDevice},
};

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";

const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";

fn b(s: &str) -> String {
    format!("{BOLD}{s}{RESET}")
}
fn dim(s: &str) -> String {
    format!("{DIM}{s}{RESET}")
}
fn green(s: &str) -> String {
    format!("{GREEN}{s}{RESET}")
}
fn red(s: &str) -> String {
    format!("{RED}{s}{RESET}")
}
fn yellow(s: &str) -> String {
    format!("{YELLOW}{s}{RESET}")
}
fn cyan(s: &str) -> String {
    format!("{CYAN}{s}{RESET}")
}
fn bold(s: &str) -> String {
    format!("{BOLD}{s}{RESET}")
}

fn status_badge(online: bool) -> String {
    if online {
        green("online").to_string()
    } else {
        red("offline").to_string()
    }
}

fn pending_badge(pending: bool) -> String {
    if pending {
        yellow("PENDING").to_string()
    } else {
        dim("-").to_string()
    }
}

fn visible_len(s: &str) -> usize {
    // strip ANSI CSI sequences: \x1b[ ... m
    let mut n = 0usize;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next(); // '['
                // consume until 'm' or end
                while let Some(x) = chars.next() {
                    if x == 'm' {
                        break;
                    }
                }
                continue;
            }
        }
        n += 1;
    }
    n
}

fn pad_cell(s: &str, width: usize) -> String {
    let vis = visible_len(s);
    if vis >= width {
        s.to_string()
    } else {
        let mut out = String::with_capacity(s.len() + (width - vis));
        out.push_str(s);
        out.extend(std::iter::repeat(' ').take(width - vis));
        out
    }
}

fn row8(
    c1: &str,
    w1: usize,
    c2: &str,
    w2: usize,
    c3: &str,
    w3: usize,
    c4: &str,
    w4: usize,
    c5: &str,
    w5: usize,
    c6: &str,
    w6: usize,
    c7: &str,
    w7: usize,
    c8: &str,
    w8: usize,
    c9: &str,
) -> String {
    format!(
        "{} {} {} {} {} {} {} {} {}",
        pad_cell(c1, w1),
        pad_cell(c2, w2),
        pad_cell(c3, w3),
        pad_cell(c4, w4),
        pad_cell(c5, w5),
        pad_cell(c6, w6),
        pad_cell(c7, w7),
        pad_cell(c8, w8),
        c9
    )
}

pub fn print_devices_table(devices: &[PublicDevice], auth_requests: &[DeviceAuthRequest]) {
    if devices.is_empty() && auth_requests.is_empty() {
        println!("{}", dim("No devices found"));
        return;
    }

    const WIDTH_ID: usize = 8;
    const WIDTH_NAME: usize = 18;
    const WIDTH_STATUS: usize = 8; // "ONLINE" fits; color doesn't break alignment now
    const WIDTH_ARCH: usize = 6;
    const WIDTH_OS: usize = 26;
    const WIDTH_IP: usize = 39;
    const WIDTH_LAST: usize = 12;
    const WIDTH_PENDING: usize = 8;

    println!("{}", bold("Devices"));

    // Header (dimmed, but still aligns because we pad by visible width)
    println!(
        "  {}",
        row8(
            &dim("ID"),
            WIDTH_ID,
            &dim("NAME"),
            WIDTH_NAME,
            &dim("STATUS"),
            WIDTH_STATUS,
            &dim("ARCH"),
            WIDTH_ARCH,
            &dim("OS"),
            WIDTH_OS,
            &dim("IP"),
            WIDTH_IP,
            &dim("LAST"),
            WIDTH_LAST,
            &dim("PENDING"),
            WIDTH_PENDING,
            &dim("REQUEST"),
        )
    );

    for dev in devices {
        let os = truncate_str(&dev.system_info.operating_system, WIDTH_OS - 1);
        let ip = dev.system_info.public_ip_address.as_deref().unwrap_or("-");
        let last_seen = format_relative_time(&dev.last_connection);

        println!(
            "  {}",
            row8(
                &dev.short_id,
                WIDTH_ID,
                &dev.name,
                WIDTH_NAME,
                &status_badge(dev.online),
                WIDTH_STATUS,
                &dev.system_info.architecture,
                WIDTH_ARCH,
                &os,
                WIDTH_OS,
                ip,
                WIDTH_IP,
                &last_seen,
                WIDTH_LAST,
                &dim("-"),
                WIDTH_PENDING,
                &dim("-"),
            )
        );
    }

    for req in auth_requests {
        let name = truncate_str(&req.device_info.hostname, WIDTH_NAME - 1);
        let os = truncate_str(&req.device_info.operating_system, WIDTH_OS - 1);
        let ip = req.device_info.public_ip_address.as_deref().unwrap_or("-");

        println!(
            "  {}",
            row8(
                &dim("-"),
                WIDTH_ID,
                &name,
                WIDTH_NAME,
                &cyan("AUTH"),
                WIDTH_STATUS,
                &req.device_info.architecture,
                WIDTH_ARCH,
                &os,
                WIDTH_OS,
                ip,
                WIDTH_IP,
                &dim("-"),
                WIDTH_LAST,
                &pending_badge(true),
                WIDTH_PENDING,
                &req.request_id, // copy-friendly
            )
        );
    }
}

pub fn print_device_status(name: &str, status: &DeviceStatus) {
    println!("{} {}", "Device", bold(name));

    if status.observations.is_empty() && status.incidents.is_empty() {
        println!("  {}", dim("No observations or incidents"));
        return;
    }

    if !status.observations.is_empty() {
        println!("  {}", bold("Observations"));

        // column widths (match the “devices” vibe: simple rows, aligned)
        const W_NAME: usize = 18;
        const W_LIFE: usize = 12; // ALIVE/DEAD
        const W_HEALTH: usize = 10; // HEALTHY/UNHEALTHY

        println!(
            "  {}",
            row8(
                &dim("NAME"),
                W_NAME,
                &dim("LIFELYNESS"),
                W_LIFE,
                &dim("HEALTH"),
                W_HEALTH,
                &dim("CRASHES"),
                8,
                &dim("UNHEALTHY CHECKS"),
                18,
                "",
                0,
                "",
                0,
                "",
                0,
                "" // tail
            )
        );

        for obs in &status.observations {
            let life = if obs.alive {
                green("ALIVE")
            } else {
                red("DEAD")
            };
            let health = if obs.healthy {
                green("HEALTHY")
            } else {
                yellow("UNHEALTHY")
            };

            let crashes = if obs.crashes > 0 {
                red(&obs.crashes.to_string())
            } else {
                dim("0")
            };

            let checks = if obs.unhealthy_checks > 0 {
                yellow(&obs.unhealthy_checks.to_string())
            } else {
                dim("0")
            };

            println!(
                "  {}",
                format!(
                    "{} {} {} {} {}",
                    pad_cell(&obs.name, W_NAME),
                    pad_cell(&life, W_LIFE),
                    pad_cell(&health, W_HEALTH),
                    pad_cell(&crashes, 8),
                    pad_cell(&checks, 8),
                )
            );
        }
    }

    if !status.incidents.is_empty() {
        println!("  {}", bold("Incidents"));

        const W_ID: usize = 18;
        const W_START: usize = 20;

        println!(
            "  {}",
            format!(
                "{} {} {}",
                pad_cell(&dim("ID"), W_ID),
                pad_cell(&dim("START"), W_START),
                dim("END"),
            )
        );

        for inc in &status.incidents {
            println!(
                "  {} {} {}",
                pad_cell(&red(&inc.id), W_ID),
                pad_cell(&dim(&inc.start_time), W_START),
                dim(&inc.end_time),
            );
        }
    }
}

/// Truncate a string to max length, adding "..." if truncated
fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() > max {
        format!("{}...", s.chars().take(max - 3).collect::<String>())
    } else {
        s.to_string()
    }
}

/// Format an ISO timestamp as relative time (e.g., "2 min ago", "3 days ago")
fn format_relative_time(iso_time: &str) -> String {
    let Ok(time) = iso_time.parse::<DateTime<Utc>>() else {
        return iso_time.to_string();
    };

    let now = Utc::now();
    let duration = now.signed_duration_since(time);

    let secs = duration.num_seconds();
    if secs < 0 {
        return "just now".to_string();
    }
    if secs < 60 {
        return format!("{} sec ago", secs);
    }

    let mins = duration.num_minutes();
    if mins < 60 {
        return format!("{} min ago", mins);
    }

    let hours = duration.num_hours();
    if hours < 24 {
        return format!("{} hour{} ago", hours, if hours == 1 { "" } else { "s" });
    }

    let days = duration.num_days();
    if days < 30 {
        return format!("{} day{} ago", days, if days == 1 { "" } else { "s" });
    }

    let months = days / 30;
    if months < 12 {
        return format!("{} month{} ago", months, if months == 1 { "" } else { "s" });
    }

    let years = days / 365;
    format!("{} year{} ago", years, if years == 1 { "" } else { "s" })
}
