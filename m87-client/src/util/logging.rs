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
        let target = meta.target();

        // Timestamp
        let ts = timestamp_hms();

        // Simple ANSI colors
        let lvl_colored = match *level {
            tracing::Level::ERROR => "\x1b[31mERROR\x1b[0m",
            tracing::Level::WARN => "\x1b[33mWARN\x1b[0m",
            tracing::Level::INFO => "\x1b[32mINFO\x1b[0m",
            tracing::Level::DEBUG => "\x1b[34mDEBUG\x1b[0m",
            tracing::Level::TRACE => "\x1b[90mTRACE\x1b[0m",
        };

        // Build final line
        let mut line = String::new();
        let _ = write!(
            line,
            "[{}] {} [{}] {}",
            ts, lvl_colored, target, visitor.msg
        );

        let _ = self.tx.send(line);
    }
}

pub fn init_tracing_with_log_layer(default_level: &str) -> broadcast::Sender<String> {
    let (tx, _rx) = broadcast::channel(1024);
    LOG_TX.set(tx.clone()).ok();

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_fmt::layer())
        .with(LogBroadcastLayer::new(tx.clone()))
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
}
