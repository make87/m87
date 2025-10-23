use crate::server::auth::validate_token_via_ws;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::process::Command;
use tokio::select;
use tokio::time::{timeout, Duration};
use warp::ws::{Message, WebSocket, Ws};

/// Per-connection interactive SSH shell (not shared on purpose).
pub async fn terminal_handler(ws: WebSocket) {
    let (mut ws_tx, mut ws_rx) = ws.split();

    if let Err(e) = validate_token_via_ws(&mut ws_tx, &mut ws_rx, true).await {
        eprintln!("auth failed: {}", e);
        return;
    }

    let mut child = match Command::new("ssh")
        .arg("localhost")
        .arg("-tt")
        .arg("-o")
        .arg("LogLevel=QUIET")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("RequestTTY=force")
        .env("TERM", "xterm-256color")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            let _ = ws_tx
                .send(Message::text(format!("Failed to start SSH: {}", e)))
                .await;
            return;
        }
    };

    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let mut stdin_writer = BufWriter::new(stdin);
    let mut stdout_reader = tokio::io::BufReader::new(stdout);
    let mut stderr_reader = tokio::io::BufReader::new(stderr);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(1);

    // stdout
    let tx1 = tx.clone();
    let ready1 = ready_tx.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 128];
        let mut sent = false;
        loop {
            match stdout_reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if !sent {
                        let _ = ready1.send(()).await;
                        sent = true;
                    }
                    if tx1
                        .send(String::from_utf8_lossy(&buf[..n]).to_string())
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        if !sent {
            let _ = ready1.send(()).await;
        }
    });

    // stderr
    let tx2 = tx.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 128];
        loop {
            match stderr_reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if tx2
                        .send(String::from_utf8_lossy(&buf[..n]).to_string())
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    if timeout(Duration::from_secs(2), ready_rx.recv())
        .await
        .is_err()
    {
        let _ = ws_tx
            .send(Message::text("SSH shell failed to start within timeout\n"))
            .await;
        let _ = child.kill().await;
        return;
    }
    let _ = ws_tx
        .send(Message::text("Shell connected successfully\n\r"))
        .await;

    loop {
        select! {
            Some(cmd) = ws_rx.next() => {
                match cmd {
                    Ok(msg) => {
                        if msg.is_close() { break; }
                        let data = msg.into_bytes();
                        if stdin_writer.write_all(&data).await.is_err() { break; }
                        if stdin_writer.flush().await.is_err() { break; }
                    }
                    Err(_) => break,
                }
            }
            Some(out) = rx.recv() => {
                if ws_tx.send(Message::text(out)).await.is_err() { break; }
            }
        }
    }

    let _ = child.kill().await;
}

pub async fn handle_terminal_ws(ws: Ws) -> Result<impl warp::Reply, std::convert::Infallible> {
    Ok(ws.on_upgrade(move |socket| terminal_handler(socket)))
}
