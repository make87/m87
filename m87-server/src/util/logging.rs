use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let fmt_layer = fmt::layer()
        .with_writer(std::io::stdout) // ensure direct stdout
        .with_ansi(false) // disable color escape codes
        .with_target(false) // cleaner logs
        .without_time(); // optional: easier container logs

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();
}
