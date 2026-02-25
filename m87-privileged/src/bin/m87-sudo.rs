use std::process::ExitCode;

use m87_shared::privileged::{DenyReason, ExecContext, OutputStream, PrivilegedMessage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const DEFAULT_SOCKET_PATH: &str = "/run/m87/privileged.sock";

#[tokio::main]
async fn main() -> ExitCode {
    let (socket_path, argv) = match parse_args() {
        Ok(v) => v,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(2);
        }
    };

    let stream = match UnixStream::connect(&socket_path).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("m87-sudo: cannot connect to {socket_path}: {e}");
            return ExitCode::from(125);
        }
    };

    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let id = format!("sudo-{}", std::process::id());
    let msg = PrivilegedMessage::Exec {
        id: id.clone(),
        argv,
        context: ExecContext::Unattended,
    };

    let mut line = match serde_json::to_string(&msg) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("m87-sudo: failed to serialize request: {e}");
            return ExitCode::from(125);
        }
    };
    line.push('\n');

    if let Err(e) = writer.write_all(line.as_bytes()).await {
        eprintln!("m87-sudo: failed to send request: {e}");
        return ExitCode::from(125);
    }

    loop {
        let resp = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => {
                eprintln!("m87-sudo: connection closed unexpectedly");
                return ExitCode::from(125);
            }
            Err(e) => {
                eprintln!("m87-sudo: read error: {e}");
                return ExitCode::from(125);
            }
        };

        let msg: PrivilegedMessage = match serde_json::from_str(&resp) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("m87-sudo: invalid response: {e}");
                return ExitCode::from(125);
            }
        };

        match msg {
            PrivilegedMessage::Output { stream, data, .. } => match stream {
                OutputStream::Stdout => {
                    println!("{data}");
                }
                OutputStream::Stderr => {
                    eprintln!("{data}");
                }
            },
            PrivilegedMessage::Result { exit_code, .. } => {
                return ExitCode::from(exit_code as u8);
            }
            PrivilegedMessage::Denied { reason, .. } => {
                let reason_str = match reason {
                    DenyReason::NoPolicy => "no matching policy grant",
                    DenyReason::UserDenied => "request denied by user",
                    DenyReason::Timeout => "approval timed out",
                };
                eprintln!("m87-sudo: denied — {reason_str}");
                return ExitCode::from(126);
            }
            _ => {
                eprintln!("m87-sudo: unexpected response from daemon");
                return ExitCode::from(125);
            }
        }
    }
}

fn parse_args() -> Result<(String, Vec<String>), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        return Err("usage: m87-sudo [--socket-path PATH] <command> [args...]".to_string());
    }

    let mut socket_path = DEFAULT_SOCKET_PATH.to_string();
    let mut i = 0;

    if args[i] == "--socket-path" {
        if i + 1 >= args.len() {
            return Err("--socket-path requires a value".to_string());
        }
        socket_path = args[i + 1].clone();
        i += 2;
    }

    if i >= args.len() {
        return Err("usage: m87-sudo [--socket-path PATH] <command> [args...]".to_string());
    }

    let argv = args[i..].to_vec();
    Ok((socket_path, argv))
}
