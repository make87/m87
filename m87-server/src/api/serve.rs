use arc_swap::ArcSwap;
use axum::{
    http::{header, Method},
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use futures::StreamExt;
use rustls::ServerConfig;
use std::{sync::Arc, time::Duration};

use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::TcpListenerStream;
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
        tunnel::handle_sni,
    },
    config::AppConfig,
    db::Mongo,
    relay::relay_state::RelayState,
    response::ServerResult,
    util::app_state::AppState,
};

async fn get_status() -> impl IntoResponse {
    "ok".to_string()
}

pub async fn serve(
    db: Arc<Mongo>,
    relay: Arc<RelayState>,
    cfg: Arc<AppConfig>,
) -> ServerResult<()> {
    // Ensure rustls has a crypto provider before anything touches TLS
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .expect("failed to install ring crypto provider");

    let state = AppState {
        db: db.clone(),
        config: cfg.clone(),
        relay: relay.clone(),
    };
    // create cfg.certificate_path if it does not exist
    std::fs::create_dir_all(&cfg.certificate_path).expect("failed to create certificate directory");
    // will load existing or create self signed certificate on firststartup. Valid certs have to be posted via update-cert route
    let current = Arc::new(ArcSwap::from(Arc::new(create_tls_config(&cfg).await?)));

    // ===== REST on loopback =====
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::any())
        .allow_methods([Method::GET, Method::POST, Method::DELETE])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("sec-websocket-protocol"),
        ]);

    let admin_route = Router::new()
        .route("/update-cert", post(update_cert))
        .layer(Extension(current.clone()));

    let app = Router::new()
        .nest("/auth", auth::create_route())
        .nest("/device", device::create_route())
        .nest("/admin", admin_route)
        .route("/status", get(get_status))
        .with_state(state.clone())
        .layer(cors)
        .layer(SetSensitiveHeadersLayer::new(std::iter::once(
            header::AUTHORIZATION,
        )))
        .layer(TimeoutLayer::new(Duration::from_secs(30)))
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new());

    let rest_listener = TcpListener::bind(("127.0.0.1", cfg.rest_port))
        .await
        .expect("bind REST");
    info!("REST listening on 127.0.0.1:{}", cfg.rest_port);

    let rest_task = tokio::spawn(async move {
        if let Err(e) = axum::serve(rest_listener, app).await {
            warn!("Axum server failed: {e:?}");
        }
    });

    // === TLS (ACME or self-signed) ===
    // Don't spawn here — serve_tls_or_selfsigned already does internal spawns.
    serve_tls_or_selfsigned(cfg.clone(), state.clone(), current.clone()).await?;

    // === Wait for REST task forever ===
    let _ = rest_task.await;
    Ok(())
}

pub async fn serve_tls_or_selfsigned(
    cfg: Arc<AppConfig>,
    state: AppState,
    current: Arc<ArcSwap<ServerConfig>>,
) -> ServerResult<()> {
    let tcp = TcpListener::bind(("0.0.0.0", cfg.unified_port))
        .await
        .expect("bind TLS");
    let mut incoming = TcpListenerStream::new(tcp);

    // --- accept incoming connections ---
    tokio::spawn(async move {
        while let Some(Ok(stream)) = incoming.next().await {
            let acceptor = TlsAcceptor::from(current.load_full());
            let state = state.clone();
            tokio::spawn(async move {
                match acceptor.accept(stream).await {
                    Ok(tls) => {
                        let sni = tls.get_ref().1.server_name().unwrap_or("").to_string();
                        let _ = handle_sni(&sni, tls, &state).await;
                    }
                    Err(e) => warn!("TLS handshake failed: {e:?}"),
                }
            });
        }
    });

    Ok(())
}
