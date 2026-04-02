use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use tokio::sync::{Mutex, mpsc};
use tokio::{
    io::AsyncReadExt,
    io::AsyncWriteExt,
    select,
    time::{Duration, Instant, interval, sleep},
};

use std::{io::Read, io::Write, sync::Arc};

use crate::streams::quic::QuicIo;
use crate::util::shell::{self, ShellMode};

#[cfg(unix)]
fn kill_process_group(pid: u32, sig: i32) {
    unsafe {
        let _ = libc::kill(-(pid as i32), sig);
    }
}

#[cfg(unix)]
async fn terminate_session(child_pid: u32) {
    if child_pid == 0 {
        return;
    }

    kill_process_group(child_pid, libc::SIGHUP);
    sleep(Duration::from_millis(200)).await;
    kill_process_group(child_pid, libc::SIGTERM);
    sleep(Duration::from_millis(500)).await;
    kill_process_group(child_pid, libc::SIGKILL);
}

#[cfg(not(unix))]
async fn terminate_session(_child_pid: u32) {}

pub async fn handle_terminal_io(term: Option<String>, io: &mut QuicIo) {
    let _ = io.write_all(b"\n\rInitializing shell..").await;

    let pty_system = native_pty_system();

    let mut buf = [0u8; 5];
    io.read_exact(&mut buf).await.ok();

    let (rows, cols) = if buf[0] == 0xFF {
        (
            u16::from_be_bytes([buf[1], buf[2]]),
            u16::from_be_bytes([buf[3], buf[4]]),
        )
    } else {
        (24, 80)
    };

    let pair = match pty_system.openpty(PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    }) {
        Ok(p) => p,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to create PTY: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    let detected_shell = shell::detect_shell();
    let args = shell::build_shell_args(&detected_shell, ShellMode::InteractiveLogin);
    let mut cmd = CommandBuilder::new(&detected_shell);
    let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    cmd.args(&args_refs);

    let term = term.as_deref().unwrap_or("xterm-256color");
    cmd.env("TERM", term);
    cmd.env("COLORTERM", "truecolor");

    if !shell::supports_login_flag(&detected_shell) {
        cmd.env("PATH", shell::ensure_minimal_path());
    }

    let child = match pair.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to spawn shell: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    let child_pid = child.process_id().unwrap_or(0);

    let reader = match pair.master.try_clone_reader() {
        Ok(r) => r,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to get PTY reader: {e}\n").as_bytes())
                .await;
            terminate_session(child_pid).await;
            return;
        }
    };

    let writer = match pair.master.take_writer() {
        Ok(w) => w,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to get PTY writer: {e}\n").as_bytes())
                .await;
            terminate_session(child_pid).await;
            return;
        }
    };

    let writer = Arc::new(Mutex::new(writer));

    let (pty_tx, mut pty_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    tokio::task::spawn_blocking(move || {
        let mut reader = reader;
        let mut buf = [0u8; 1024];

        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let _ = pty_tx.send(buf[..n].to_vec());
                }
                Err(_) => break,
            }
        }
    });

    let (exit_tx, mut exit_rx) = mpsc::channel::<()>(1);
    let child_arc = Arc::new(std::sync::Mutex::new(child));
    let child_wait = child_arc.clone();
    tokio::task::spawn_blocking(move || {
        let mut guard = child_wait.lock().unwrap();
        let _ = guard.wait();
        let _ = exit_tx.blocking_send(());
    });

    let _ = io.write_all(b"Shell connected successfully\r\n").await;

    let mut io_read_buf = [0u8; 1024];
    let mut input_buf: Vec<u8> = Vec::new();
    let idle_timeout = Duration::from_secs(300);
    let mut last_activity = Instant::now();
    let mut ticker = interval(Duration::from_secs(30));

    'outer: loop {
        select! {
            _ = ticker.tick() => {
                if last_activity.elapsed() > idle_timeout {
                    break 'outer;
                }
            }

            r = io.read(&mut io_read_buf) => {
                match r {
                    Ok(0) => break 'outer,
                    Ok(n) => {
                        last_activity = Instant::now();
                        input_buf.extend_from_slice(&io_read_buf[..n]);

                        while !input_buf.is_empty() {
                            if input_buf.len() >= 5 && input_buf[0] == 0xFF {
                                let new_rows = u16::from_be_bytes([input_buf[1], input_buf[2]]);
                                let new_cols = u16::from_be_bytes([input_buf[3], input_buf[4]]);

                                let _ = pair.master.resize(PtySize {
                                    rows: new_rows,
                                    cols: new_cols,
                                    pixel_width: 0,
                                    pixel_height: 0,
                                });

                                input_buf.drain(..5);
                                continue;
                            }

                            let next_resize = input_buf
                                .iter()
                                .position(|&b| b == 0xFF)
                                .unwrap_or(input_buf.len());

                            let payload: Vec<u8> = input_buf.drain(..next_resize).collect();

                            if !payload.is_empty() {
                                let writer = writer.clone();

                                match tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                                    let mut w = writer.blocking_lock();
                                    w.write_all(&payload)?;
                                    w.flush()
                                })
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    _ => break 'outer,
                                }
                            }
                        }
                    }
                    Err(_) => break 'outer,
                }
            }

            Some(out) = pty_rx.recv() => {
                last_activity = Instant::now();
                if io.write_all(&out).await.is_err() {
                    break 'outer;
                }
            }

            Some(()) = exit_rx.recv() => {
                break 'outer;
            }
        }
    }

    drop(pair);
    terminate_session(child_pid).await;

    {
        let mut guard = child_arc.lock().unwrap();
        let _ = guard.kill();
    }

    let _ = io.shutdown().await;
}

#[cfg(test)]
mod tests {
    use crate::util::shell;
    use std::path::Path;

    #[test]
    fn test_detect_shell_returns_valid_path() {
        let s = shell::detect_shell();
        assert!(!s.is_empty());

        #[cfg(unix)]
        {
            assert!(s.starts_with('/'));
            assert!(Path::new(&s).exists());
        }

        #[cfg(windows)]
        {
            assert_eq!(s, "powershell.exe");
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_detect_shell_fallback_exists() {
        let s = shell::detect_shell();
        assert!(Path::new(&s).exists());
    }
}
