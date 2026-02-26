use anyhow::{bail, Context, Result};
use m87_shared::privileged::{OutputStream, PrivilegedMessage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedWriteHalf;
use tokio::process::Command;
use tracing::{debug, error};

/// Execute `argv` as a child process, streaming stdout/stderr back over the
/// socket as `Output` messages.  Returns the child exit code.
pub async fn execute_streaming(
    argv: Vec<String>,
    request_id: &str,
    writer: &mut OwnedWriteHalf,
) -> Result<i32> {
    if argv.is_empty() {
        bail!("argv is empty");
    }

    let program = &argv[0];
    let args = &argv[1..];

    debug!(request_id, %program, "spawning child process");

    let mut child = Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env_clear()
        .env("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
        .env("LANG", "C.UTF-8")
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to spawn: {program}"))?;

    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");

    let mut stdout_reader = BufReader::new(stdout).lines();
    let mut stderr_reader = BufReader::new(stderr).lines();

    loop {
        tokio::select! {
            line = stdout_reader.next_line() => {
                match line {
                    Ok(Some(data)) => {
                        send_output(writer, request_id, OutputStream::Stdout, data).await?;
                    }
                    Ok(None) => {
                        // stdout closed — wait for stderr to finish, then break.
                        while let Ok(Some(data)) = stderr_reader.next_line().await {
                            send_output(writer, request_id, OutputStream::Stderr, data).await?;
                        }
                        break;
                    }
                    Err(e) => {
                        error!(request_id, "error reading stdout: {e}");
                        break;
                    }
                }
            }
            line = stderr_reader.next_line() => {
                match line {
                    Ok(Some(data)) => {
                        send_output(writer, request_id, OutputStream::Stderr, data).await?;
                    }
                    Ok(None) => {
                        // stderr closed — drain remaining stdout, then break.
                        while let Ok(Some(data)) = stdout_reader.next_line().await {
                            send_output(writer, request_id, OutputStream::Stdout, data).await?;
                        }
                        break;
                    }
                    Err(e) => {
                        error!(request_id, "error reading stderr: {e}");
                        break;
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
