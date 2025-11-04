use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use m87_client::{agent, auth, config::Config};

async fn start_mongo() -> (String, testcontainers::ContainerAsync<GenericImage>) {
    let image = GenericImage::new("mongo", "7.0.5")
        .with_exposed_port(27017.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"));
    let container = image.start().await.expect("Failed to start MongoDB");
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let uri = format!("mongodb://127.0.0.1:{port}/testdb_{}", Uuid::new_v4());
    (uri, container)
}

async fn start_server(mongo_uri: &str, rest_port: u16) -> std::process::Child {
    let mut cmd = std::process::Command::new("cargo");
    cmd.args(["run", "--package", "m87-server"])
        .env("MONGO_URI", mongo_uri)
        .env("MONGO_DB", "e2e-tests")
        .env("UNIFIED_PORT", rest_port.to_string())
        .env("PUBLIC_ADDRESS", "localhost")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut server = cmd.spawn().expect("Failed to start server");
    sleep(Duration::from_secs(3)).await;
    server
}

#[tokio::test]
async fn e2e_clear_and_run_agent() {
    let (mongo_uri, _mongo) = start_mongo().await;
    let rest_port = 8085;
    let mut server = start_server(&mongo_uri, rest_port).await;

    Config::clear().unwrap();
    let mut cfg = Config::load().unwrap();
    cfg.api_url = format!("http://localhost:{rest_port}");
    cfg.trust_invalid_server_cert = true;
    cfg.save().unwrap();

    let owner_ref = Some("test@example.com".to_string());
    let result = agent::run(owner_ref).await;
    assert!(result.is_ok(), "Agent run failed: {:?}", result);

    let _ = server.kill();
}

#[tokio::test]
async fn e2e_auth_login_status() {
    let (mongo_uri, _mongo) = start_mongo().await;
    let rest_port = 8082;
    let mut server = start_server(&mongo_uri, rest_port).await;

    Config::clear().unwrap();
    let mut cfg = Config::load().unwrap();
    cfg.api_url = format!("http://localhost:{rest_port}");
    cfg.trust_invalid_server_cert = true;
    cfg.save().unwrap();

    let result = auth::login_agent(Some("tester@example.com".into())).await;
    assert!(result.is_ok(), "Login failed: {:?}", result);

    let status = auth::status().await;
    assert!(status.is_ok(), "Status check failed: {:?}", status);

    let _ = server.kill();
}
