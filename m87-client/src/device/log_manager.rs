use anyhow::{Context, Result, anyhow};
use m87_shared::deploy_spec::{CommandSpec, LogSpec};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::mpsc,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::util::command::build_command;
use crate::util::format::format_log;

/// On-demand logs only:
/// - Snapshot: run once, capture bounded output (for `m87 <device> logs` and incident evidence)
/// - Follow: stream while a client is connected (`m87 <device> logs -f`)
///
/// No always-on processes.
#[derive(Clone)]
pub struct LogManager {
    tx: mpsc::Sender<LogCmd>,
}

enum LogCmd {
    Snapshot {
        run_id: String,
        spec: LogSpec,
        env: BTreeMap<String, String>,
        workdir: PathBuf,
        max_bytes: usize,
        max_lines: usize,
        timeout: Duration,
        resp: mpsc::Sender<Result<Vec<String>>>,
    },
    FollowStart {
        run_id: String,
        spec: LogSpec,
        env: BTreeMap<String, String>,
        workdir: PathBuf,
    },
    FollowStop {
        run_id: String,
    },
    StopAll,
}

struct FollowStream {
    cancel: CancellationToken,
    followers: u64,
}

impl LogManager {
    /// `event_tx` receives `TelemetryEvent::LogLine` only for active follow sessions.
    pub fn start() -> Self {
        let (tx, mut rx) = mpsc::channel::<LogCmd>(64);

        tokio::spawn(async move {
            let mut follows: HashMap<String, FollowStream> = HashMap::new();

            loop {
                match rx.recv().await {
                    Some(LogCmd::Snapshot {
                        run_id,
                        spec,
                        env,
                        workdir,
                        max_bytes,
                        max_lines,
                        timeout: t,
                        resp,
                    }) => {
                        let r = snapshot_logs(
                            &run_id, &spec.tail, &env, &workdir, max_bytes, max_lines, t,
                        )
                        .await;
                        let _ = resp.send(r).await;
                    }

                    Some(LogCmd::FollowStart {
                        run_id,
                        spec,
                        env,
                        workdir,
                    }) => {
                        // Check if we already have a follow stream for this unit
                        if let Some(stream) = follows.get_mut(&run_id) {
                            // Increment follower count
                            stream.followers += 1;
                            tracing::debug!(
                                "Added follower to {} (total: {})",
                                run_id,
                                stream.followers
                            );
                            continue;
                        }

                        let Some(follow) = spec.follow.as_ref() else {
                            tracing::info!(
                                "Skipping follow for {} since there is no follow spec",
                                run_id
                            );
                            continue;
                        };

                        let cancel = CancellationToken::new();
                        match spawn_follow(&run_id, &follow, &env, &workdir, cancel.clone()).await {
                            Ok(()) => {
                                follows.insert(
                                    run_id.clone(),
                                    FollowStream {
                                        cancel,
                                        followers: 1,
                                    },
                                );
                                tracing::debug!("Started follow stream for {}", run_id);
                            }
                            Err(e) => {
                                tracing::error!("log follow spawn failed: {e}");
                            }
                        }
                    }

                    Some(LogCmd::FollowStop { run_id }) => {
                        if let Some(stream) = follows.get_mut(&run_id) {
                            stream.followers = stream.followers.saturating_sub(1);
                            tracing::debug!(
                                "Removed follower from {} (remaining: {})",
                                run_id,
                                stream.followers
                            );

                            if stream.followers == 0 {
                                tracing::debug!(
                                    "No more followers for {}, cancelling stream",
                                    run_id
                                );
                                let stream = follows.remove(&run_id).unwrap();
                                stream.cancel.cancel();
                            }
                        }
                    }

                    Some(LogCmd::StopAll) => {
                        for (_, s) in follows.drain() {
                            s.cancel.cancel();
                        }
                    }

                    None => break,
                }
            }
        });

        Self { tx }
    }

    /// Run a bounded snapshot command once.
    /// Use for:
    /// - `m87 <device> logs` (non-follow)
    /// - incident evidence collection (last logs)
    pub async fn snapshot(
        &self,
        run_id: String,
        spec: LogSpec,
        env: BTreeMap<String, String>,
        workdir: PathBuf,
        max_bytes: usize,
        max_lines: usize,
        timeout_dur: Duration,
    ) -> Result<Vec<String>> {
        let (resp_tx, mut resp_rx) = mpsc::channel::<Result<Vec<String>>>(1);
        let _ = self
            .tx
            .send(LogCmd::Snapshot {
                run_id,
                spec,
                env,
                workdir,
                max_bytes,
                max_lines,
                timeout: timeout_dur,
                resp: resp_tx,
            })
            .await;

        resp_rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("snapshot response channel closed"))?
    }

    /// Start streaming logs for a unit. Increments follower count.
    /// Multiple followers can watch the same unit; the stream is shared.
    pub async fn follow_start(
        &self,
        run_id: String,
        spec: &LogSpec,
        env: BTreeMap<String, String>,
        workdir: PathBuf,
    ) {
        let _ = self
            .tx
            .send(LogCmd::FollowStart {
                run_id,
                spec: spec.clone(),
                env,
                workdir,
            })
            .await;
    }

    /// Stop streaming logs for a unit. Decrements follower count.
    /// Stream is only cancelled when follower count reaches 0.
    pub async fn follow_stop(&self, run_id: String) {
        let _ = self.tx.send(LogCmd::FollowStop { run_id }).await;
    }

    pub async fn stop_all(&self) {
        let _ = self.tx.send(LogCmd::StopAll).await;
    }
}

async fn spawn_follow(
    run_id: &str,
    spec: &CommandSpec,
    env: &BTreeMap<String, String>,
    workdir: &Path,
    cancel: CancellationToken,
) -> Result<()> {
    let mut cmd = build_command(spec)?;
    cmd.current_dir(workdir);
    for (k, v) in env {
        cmd.env(k, v);
    }

    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn().context("spawn command")?;

    let stdout = child.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;
    let stderr = child.stderr.take().ok_or_else(|| anyhow!("no stderr"))?;

    async fn follow_lines<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
        reader: R,
        unit: String,
        cancel: CancellationToken,
    ) {
        let mut lines = BufReader::new(reader).lines();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Stop following logs promptly on shutdown.
                    break;
                }
                res = lines.next_line() => {
                    match res {
                        Ok(Some(line)) => tracing::info!("[observe]{}", format_log(&unit, &line, true)),
                        Ok(None) => break, // EOF
                        Err(_) => break,   // I/O error; best-effort
                    }
                }
            }
        }
    }

    let unit = run_id.to_string();
    tokio::spawn(follow_lines(stdout, unit, cancel.clone()));

    let unit = run_id.to_string();
    tokio::spawn(follow_lines(stderr, unit, cancel.clone()));

    // Ensure the process is terminated when cancellation is requested.
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = child.kill().await;   // best-effort
                    let _ = child.wait().await;   // reap
                }
                _ = child.wait() => {
                    // Process exited normally.
                }
            }
        }
    });

    Ok(())
}

/// Runs the log command once and captures bounded output.
/// Assumes the provided command exits on its own (no -f).
async fn snapshot_logs(
    _run_id: &str,
    spec: &CommandSpec,
    env: &BTreeMap<String, String>,
    workdir: &Path,
    max_bytes: usize,
    max_lines: usize,
    timeout_dur: Duration,
) -> Result<Vec<String>> {
    let mut cmd = build_command(spec)?;
    cmd.current_dir(workdir);
    for (k, v) in env {
        cmd.env(k, v);
    }
    // Capture stdout for snapshot
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::null());

    let mut child = cmd.spawn().context("spawn log snapshot command")?;
    let stdout = child.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;
    let mut lines = BufReader::new(stdout).lines();

    let fut = async {
        let mut out = Vec::new();
        let mut bytes: usize = 0;

        while let Ok(Some(line)) = lines.next_line().await {
            bytes += line.len() + 1;
            out.push(line);

            if out.len() >= max_lines || bytes >= max_bytes {
                break;
            }
        }

        // Ensure process doesn't linger
        let _ = child.start_kill();
        let _ = child.wait().await;

        Ok::<_, anyhow::Error>(out)
    };

    timeout(timeout_dur, fut)
        .await
        .context("log snapshot timeout")?
}
