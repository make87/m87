use anyhow::{bail, Context, Result};
use m87_shared::privileged::{OutputStream, PrivilegedMessage};
use m87_shared::shell::ensure_minimal_path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::process::Command;
use tracing::debug;

/// Execute `argv` as a child process, streaming stdout/stderr back over the
/// socket as `Output` messages. Also forwards stdin from the socket.
/// Returns the child exit code.
pub async fn execute_streaming(
    argv: Vec<String>,
    cwd: String,
    request_id: &str,
    reader: &mut Lines<BufReader<OwnedReadHalf>>,
    writer: &mut OwnedWriteHalf,
) -> Result<i32> {
    if argv.is_empty() {
        bail!("argv is empty");
    }

    let program = &argv[0];
    let args = &argv[1..];

    debug!(request_id, %program, cwd, "spawning child process");

    let mut cmd = Command::new(program);
    cmd.args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env_clear()
        .env("PATH", ensure_minimal_path())
        .env("LANG", "C.UTF-8")
        .kill_on_drop(true)
        .current_dir(&cwd);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn: {program}"))?;

    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");
    let mut child_stdin = Some(child.stdin.take().expect("stdin was piped"));

    let mut stdout_reader = BufReader::new(stdout).lines();
    let mut stderr_reader = BufReader::new(stderr).lines();

    let mut stdout_done = false;
    let mut stderr_done = false;
    let mut stdin_done = false;

    loop {
        if stdout_done && stderr_done {
            break;
        }

        tokio::select! {
            line = stdout_reader.next_line(), if !stdout_done => {
                match line {
                    Ok(Some(data)) => {
                        send_output(writer, request_id, OutputStream::Stdout, data).await?;
                    }
                    Ok(None) | Err(_) => {
                        stdout_done = true;
                    }
                }
            }
            line = stderr_reader.next_line(), if !stderr_done => {
                match line {
                    Ok(Some(data)) => {
                        send_output(writer, request_id, OutputStream::Stderr, data).await?;
                    }
                    Ok(None) | Err(_) => {
                        stderr_done = true;
                    }
                }
            }
            msg = reader.next_line(), if !stdin_done => {
                match msg {
                    Ok(Some(line)) => {
                        match serde_json::from_str::<PrivilegedMessage>(&line) {
                            Ok(PrivilegedMessage::StdinData { data, .. }) => {
                                if let Some(ref mut stdin) = child_stdin {
                                    let _ = stdin.write_all(data.as_bytes()).await;
                                    let _ = stdin.write_all(b"\n").await;
                                }
                            }
                            Ok(PrivilegedMessage::StdinClose { .. }) => {
                                child_stdin = None;
                                stdin_done = true;
                            }
                            Ok(_) => {
                                // ignore other message types during execution
                            }
                            Err(_) => {
                                // ignore malformed JSON
                            }
                        }
                    }
                    Ok(None) | Err(_) => {
                        // socket closed or read error
                        child_stdin = None;
                        stdin_done = true;
                    }
                }
            }
        }
    }

    let status = child.wait().await.context("failed to wait on child")?;
    let exit_code = status.code().unwrap_or(-1);

    // Send final Result message (stdout/stderr already streamed).
    let msg = PrivilegedMessage::Result {
        id: request_id.to_string(),
        exit_code,
        stdout: String::new(),
        stderr: String::new(),
    };
    send_message(writer, &msg).await?;

    Ok(exit_code)
}

async fn send_output(
    writer: &mut OwnedWriteHalf,
    request_id: &str,
    stream: OutputStream,
    data: String,
) -> Result<()> {
    let msg = PrivilegedMessage::Output {
        id: request_id.to_string(),
        stream,
        data,
    };
    send_message(writer, &msg).await
}

pub async fn send_message(
    writer: &mut OwnedWriteHalf,
    msg: &PrivilegedMessage,
) -> Result<()> {
    let mut line = serde_json::to_string(msg).context("failed to serialize message")?;
    line.push('\n');
    writer
        .write_all(line.as_bytes())
        .await
        .context("failed to write to socket")?;
    Ok(())
}
