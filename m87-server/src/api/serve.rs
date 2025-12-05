use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Extension, Router,
    http::{Method, header},
    response::IntoResponse,
    routing::{get, post},
};
use axum_server::tls_rustls::RustlsConfig;
use tokio::sync::watch;
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    sensitive_headers::SetSensitiveHeadersLayer,
    timeout::TimeoutLayer,
    trace::TraceLayer,
};
use tracing::{info, warn};

use crate::{
    api::{
        auth,
        certificate::{create_tls_config, update_cert},
        device,
        tunnel::run_quic_endpoint,
    },
    config::AppConfig,
    db::Mongo,
    relay::relay_state::RelayState,
    response::ServerResult,
    util::app_state::AppState,
};

async fn get_status() -> impl IntoResponse {
    "ok"
}

pub async fn serve(
    db: Arc<Mongo>,
    relay: Arc<RelayState>,
    cfg: Arc<AppConfig>,
) -> ServerResult<()> {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .expect("failed to install ring");

    std::fs::create_dir_all(&cfg.certificate_path)?;

    let (reload_tx, reload_rx) = watch::channel(());

    let state = AppState {
        db: db.clone(),
        config: cfg.clone(),
        relay: relay.clone(),
    };

    // CORS for REST
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::any())
        .allow_methods([Method::GET, Method::POST, Method::DELETE])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("sec-websocket-protocol"),
        ]);

    // Admin route: writes certs to disk + signals reload
    let admin = Router::new()
        .route("/update-cert", post(update_cert))
        .layer(Extension(reload_tx.clone()));

    let app = Router::new()
        .nest("/auth", auth::create_route())
        .nest("/device", device::create_route())
        .nest("/admin", admin)
        .route("/status", get(get_status))
        .layer(cors)
        .layer(SetSensitiveHeadersLayer::new(std::iter::once(
            header::AUTHORIZATION,
        )))
        .layer(TimeoutLayer::new(Duration::from_secs(30)))
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .with_state(state.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.unified_port));
    info!("HTTPS/QUIC listening on {}", addr);

    // ===== HTTPS SERVER LOOP WITH HOT RELOAD =====
    let https_task = tokio::spawn({
        let cfg = cfg.clone();
        let app = app.clone();
        let mut reload_rx = reload_rx.clone();

        async move {
            loop {
                // Load TLS fresh from disk
                let tls = create_tls_config(&cfg).await.expect("TLS load failed");
                let tls_cfg = RustlsConfig::from_config(Arc::new(tls));

                let handle = axum_server::Handle::new();
                let server = axum_server::bind_rustls(addr, tls_cfg)
                    .handle(handle.clone())
                    .serve(app.clone().into_make_service());

                tokio::select! {
                    _ = server => {
                        warn!("HTTPS terminated unexpectedly");
                        continue;
                    }
                    _ = reload_rx.changed() => {
                        warn!("TLS updated → restarting HTTPS");
                        handle.shutdown();
                    }
                }
            }
        }
    });

    // ===== QUIC SERVER =====
    let quic_task = tokio::spawn(run_quic_endpoint(
        cfg.clone(),
        relay.clone(),
        reload_rx.clone(),
    ));

    let _ = tokio::join!(https_task, quic_task);

    Ok(())
}
