use m87_client::run_cli;
use m87_client::util::shutdown::SHUTDOWN;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() {
            SHUTDOWN.cancel();
        }
    });

    run_cli().await
}
