use crate::rest::upgrade::BoxedIo;
use axum::extract::{Path, Query};
use tokio::{io, io::AsyncWriteExt};
use tokio_serial::SerialPortBuilderExt;
use tracing::{error, info};

#[derive(Debug, serde::Deserialize)]
pub struct SerialQuery {
    pub baud: Option<u32>,
}

pub async fn handle_serial_io(
    (Path(port), Query(SerialQuery { baud })): (Path<String>, Query<SerialQuery>),
    mut io: BoxedIo,
) {
    let baud = baud.unwrap_or(115200);

    let serial_path = format!("/dev/{}", port); // e.g. ttyUSB0

    let builder = tokio_serial::new(serial_path.clone(), baud)
        .data_bits(tokio_serial::DataBits::Eight)
        .parity(tokio_serial::Parity::None)
        .stop_bits(tokio_serial::StopBits::One)
        .flow_control(tokio_serial::FlowControl::None);

    let mut serial = match builder.open_native_async() {
        Ok(s) => s,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to open {serial_path}: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    match io::copy_bidirectional(&mut io, &mut serial).await {
        Ok((a, b)) => info!("serial closed cleanly (client→dev={a}, dev→client={b})"),
        Err(e) => error!("serial forwarding error: {e}"),
    }
}
