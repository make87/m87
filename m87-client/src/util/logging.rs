use std::fmt::{self, Write};
use std::sync::{Once, OnceLock};
use tokio::sync::broadcast;
use tracing::field::Visit;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{prelude::*, EnvFilter};
use tracing_subscriber::fmt as tracing_fmt;

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
    let (tx, _rx) = broadcast::channel(1000);
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

pub fn get_log_rx() -> Option<broadcast::Receiver<String>> {
    LOG_TX.get().map(|tx| tx.subscribe())
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

pub fn human_date(ts: u64) -> String {
    // Break into days + seconds
    let days = ts / 86_400;
    let secs_of_day = ts % 86_400;

    let hour = secs_of_day / 3600;
    let min = (secs_of_day % 3600) / 60;

    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    let (_year, month, day) = unix_days_to_ymd(days as i64);

    format!(
        "{} {:>2} {:02}:{:02}",
        MONTHS[(month - 1) as usize],
        day,
        hour,
        min
    )
}

fn unix_days_to_ymd(mut days: i64) -> (i32, u32, u32) {
    // Days since 1970-01-01
    // Algorithm from Howard Hinnant's date algorithms (public domain)

    // 1. Shift epoch to March-based year so leap years are simpler
    days += 719468; // shift to civil_from_days epoch

    let era = (days >= 0)
        .then_some(days / 146097)
        .unwrap_or((days - 146096) / 146097);
    let doe = days - era * 146097; // Day of era
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // Year of era
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // Day of year
    let mp = (5 * doy + 2) / 153; // Month parameter
    let d = doy - (153 * mp + 2) / 5 + 1; // Day of month
    let m = mp + if mp < 10 { 3 } else { -9 }; // Month number
    let y = yoe + era * 400 + (m <= 2) as i64; // Year

    (y as i32, m as u32, d as u32)
}
