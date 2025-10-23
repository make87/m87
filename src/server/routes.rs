use anyhow::Result;
use warp::Filter;

use crate::server::container_logs::handle_container_logs_ws;
use crate::server::container_terminal::handle_container_terminal_ws;
use crate::server::logs::handle_logs_ws;
use crate::server::metrics::handle_system_metrics_ws;
use crate::server::terminal::handle_terminal_ws;

pub async fn run_server(server_port: u16) -> Result<()> {
    let logs_route = warp::path("logs").and(warp::ws()).and_then(handle_logs_ws);

    let terminal_route = warp::path("terminal")
        .and(warp::ws())
        .and_then(handle_terminal_ws);

    let metrics_route = warp::path("metrics")
        .and(warp::ws())
        .and_then(handle_system_metrics_ws);

    let container_terminal_route = warp::path("container")
        .and(warp::path::param::<String>())
        .and(warp::ws())
        .and_then(handle_container_terminal_ws);

    let container_logs_route = warp::path("container-logs")
        .and(warp::path::param::<String>())
        .and(warp::ws())
        .and_then(handle_container_logs_ws);

    let routes = logs_route
        .or(terminal_route)
        .or(metrics_route)
        .or(container_terminal_route)
        .or(container_logs_route)
        .recover(handle_rejection);

    let _ = warp::serve(routes.with(warp::log("warp_ws_server")))
        .run(([0, 0, 0, 0], server_port))
        .await;

    Ok(())
}

async fn handle_rejection(
    err: warp::Rejection,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    if err.is_not_found() {
        eprintln!("Route not found: {:?}", err);
    } else {
        eprintln!("Request failed: {:?}", err);
    }
    Ok(warp::reply::with_status(
        "Something went wrong",
        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
    ))
}
