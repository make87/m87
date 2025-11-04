use m87_client::run_cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Delegate to the CLI implementation in lib.rs
    run_cli().await
}
