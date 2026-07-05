use std::fmt::{self, Write};
use std::sync::{Once, OnceLock};
use tokio::sync::broadcast;

use tracing::field::Visit;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt as tracing_fmt;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, prelude::*};

static LOG_TX: OnceLock<broadcast::Sender<String>> = OnceLock::new();

struct MsgVisitor {
    msg: String,
}

impl Visit for MsgVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.msg = format!("{value:?}");
        }
    }
}

pub struct LogBroadcastLayer {
    tx: broadcast::Sender<String>,
}

impl LogBroadcastLayer {
    pub fn new(tx: broadcast::Sender<String>) -> Self {
        Self { tx }
    }
}

impl<S> Layer<S> for LogBroadcastLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Extract message field
        let mut visitor = MsgVisitor { msg: String::new() };
        event.record(&mut visitor);

        // Metadata
        let meta = event.metadata();
        let level = meta.level();

        // Simple ANSI colors
        // dont set loglvl is [observe] is in msg
        let mut line = String::new();
        if !visitor.msg.contains("[observe]") {
            let lvl_colored = match *level {
                tracing::Level::ERROR => "\x1b[31mERROR\x1b[0m",
                tracing::Level::WARN => "\x1b[33mWARN\x1b[0m",
                tracing::Level::INFO => "\x1b[32mINFO\x1b[0m",
                tracing::Level::DEBUG => "\x1b[34mDEBUG\x1b[0m",
                tracing::Level::TRACE => "\x1b[90mTRACE\x1b[0m",
            };
            let _ = write!(line, "{} {}", lvl_colored, visitor.msg);
        } else {
            let _ = write!(line, "{}", visitor.msg);
        }

        let _ = self.tx.send(line);
    }
}

fn make_filter(default_level: &str) -> EnvFilter {
    EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default_level))
        .unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Filter for the CLI log broadcast. Same base level as the console filter,
/// but the `observe` target is always admitted at INFO so live follow logs
/// reach the CLI regardless of the daemon's global level (`RUST_LOG`). Without
/// this, running the daemon below INFO silently drops all `[observe]` lines.
fn make_broadcast_filter(default_level: &str) -> EnvFilter {
    make_filter(default_level)
        .add_directive("observe=info".parse().expect("valid observe directive"))
}

/// Build the layered subscriber used by the daemon. Each layer carries its own
/// filter (rather than a single global filter), so the broadcast layer can
/// admit `observe` live-log events even when the console filter is below INFO.
/// Extracted so it can be exercised in tests with a `with_default` scope
/// instead of the global `.init()`.
fn build_subscriber(
    console_filter: EnvFilter,
    broadcast_filter: EnvFilter,
    tx: broadcast::Sender<String>,
) -> impl Subscriber + Send + Sync + 'static {
    tracing_subscriber::registry()
        .with(tracing_fmt::layer().with_filter(console_filter))
        .with(LogBroadcastLayer::new(tx).with_filter(broadcast_filter))
}

pub fn init_tracing_with_log_layer(default_level: &str) -> broadcast::Sender<String> {
    let (tx, _rx) = broadcast::channel(32_768);
    LOG_TX.set(tx.clone()).ok();

    build_subscriber(
        make_filter(default_level),
        make_broadcast_filter(default_level),
        tx.clone(),
    )
    .init();

    tx
}

static INIT: Once = Once::new();

pub fn init_logging(log_level: &str) {
    INIT.call_once(|| {
        let _ = init_tracing_with_log_layer(log_level);
    });
}

pub fn timestamp_hms() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    human_time(now.as_secs())
}

pub fn human_time(ts: u64) -> String {
    let secs = ts % 86_400;
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

pub fn get_log_rx() -> Option<broadcast::Receiver<String>> {
    LOG_TX.get().map(|tx| tx.subscribe())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_human_time_zero() {
        assert_eq!(human_time(0), "00:00:00");
    }

    #[test]
    fn test_human_time_full_day() {
        // 23:59:59 = 23*3600 + 59*60 + 59 = 86399
        assert_eq!(human_time(86399), "23:59:59");
    }

    #[test]
    fn test_human_time_wraps_at_day() {
        // 86400 seconds = 1 day, should wrap to 00:00:00
        assert_eq!(human_time(86400), "00:00:00");
    }

    #[test]
    fn test_human_time_hours_minutes_seconds() {
        // 1 hour + 1 minute + 1 second = 3661
        assert_eq!(human_time(3661), "01:01:01");
    }

    #[test]
    fn test_human_time_only_seconds() {
        assert_eq!(human_time(45), "00:00:45");
    }

    #[test]
    fn test_human_time_only_minutes() {
        assert_eq!(human_time(300), "00:05:00");
    }

    #[test]
    fn test_timestamp_hms_format() {
        let ts = timestamp_hms();
        // Should match HH:MM:SS format
        assert_eq!(ts.len(), 8);
        assert_eq!(&ts[2..3], ":");
        assert_eq!(&ts[5..6], ":");
    }

    // ── Live-log broadcast gating ────────────────────────────────────────────
    //
    // Live `observe` follow logs are emitted as
    // `tracing::info!(target: "observe", "[observe]…")` and carried to the CLI
    // over the `LogBroadcastLayer`. That layer must NOT be gated by the
    // daemon's global level: when the daemon runs below INFO (e.g.
    // `RUST_LOG=warn`, or launched non-verbose) live logs must still flow,
    // while health/liveness reports use a separate heartbeat path.

    /// Control: the broadcast itself works — a WARN line passes the WARN filter
    /// and reaches subscribers. Guards against the reproduce test below being a
    /// false positive (i.e. the broadcast being broken for some other reason).
    #[test]
    fn warn_line_reaches_broadcast_under_warn_filter() {
        let (tx, mut rx) = broadcast::channel::<String>(16);
        let subscriber =
            build_subscriber(make_filter("warn"), make_broadcast_filter("warn"), tx.clone());
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!("noisy diagnostic");
        });
        let got = rx.try_recv();
        assert!(got.is_ok(), "warn line should broadcast under warn filter");
        assert!(got.unwrap().contains("noisy diagnostic"));
    }

    /// Regression: an `observe` live-log line (emitted at INFO, as the follow
    /// loop does) must reach the CLI broadcast even when the daemon's global
    /// level is `warn`. Fails without the per-layer `observe=info` broadcast
    /// filter — that was the live-logs-missing bug.
    #[test]
    fn observe_line_reaches_broadcast_under_warn_filter() {
        let (tx, mut rx) = broadcast::channel::<String>(16);
        let subscriber =
            build_subscriber(make_filter("warn"), make_broadcast_filter("warn"), tx.clone());
        tracing::subscriber::with_default(subscriber, || {
            // Mirror the production follow emit in log_manager::spawn_follow.
            tracing::info!(target: "observe", "[observe]hello from container");
        });
        let got = rx.try_recv();
        assert!(
            got.is_ok(),
            "observe live-log line must reach the CLI broadcast even when the \
             daemon's global level is `warn`; got {got:?} — this is the \
             live-logs-missing bug"
        );
        assert!(got.unwrap().contains("hello from container"));
    }
}
