use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use m87_shared::privileged::{
    ApprovalDecision, DenyReason, ExecContext, Grant, GrantType, PolicyStore, PrivilegedMessage,
};
use tokio::io::AsyncBufReadExt;
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::audit::AuditLogger;
use crate::executor::{execute_streaming, send_message};
use crate::policy;

pub async fn handle_connection(
    stream: UnixStream,
    policy: Arc<Mutex<PolicyStore>>,
    audit: Arc<AuditLogger>,
    policy_path: PathBuf,
) {
    if let Err(e) = handle_connection_inner(stream, policy, audit, policy_path).await {
        error!("connection handler error: {e:#}");
    }
}

async fn handle_connection_inner(
    stream: UnixStream,
    policy_store: Arc<Mutex<PolicyStore>>,
    audit: Arc<AuditLogger>,
    policy_path: PathBuf,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = tokio::io::BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        let msg: PrivilegedMessage = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                warn!("invalid message from client: {e}");
                continue;
            }
        };

        match msg {
            PrivilegedMessage::Exec { id, argv, context } => {
                handle_exec(
                    &id,
                    argv,
                    context,
                    &policy_store,
                    &audit,
                    &policy_path,
                    &mut writer,
                    &mut lines,
                )
                .await?;
            }
            other => {
                warn!("unexpected message type from client: {other:?}");
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_exec(
    id: &str,
    argv: Vec<String>,
    context: ExecContext,
    policy_lock: &Arc<Mutex<PolicyStore>>,
    audit: &Arc<AuditLogger>,
    policy_path: &Path,
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    lines: &mut tokio::io::Lines<tokio::io::BufReader<tokio::net::unix::OwnedReadHalf>>,
) -> Result<()> {
    let argv_joined = argv.join(" ");
    audit.log_request(id, &argv).await;

    // Check existing policy.
    let mut store = policy_lock.lock().await;
    policy::prune_expired(&mut store);

    if let Some(idx) = policy::check_policy(&store, &argv_joined) {
        let pattern = store.grants[idx].pattern.clone();
        let is_once = store.grants[idx].grant_type == GrantType::Once;

        audit.log_policy_hit(id, &pattern).await;
        info!(id, %argv_joined, %pattern, "policy match — executing");

        if is_once {
            policy::consume_once(&mut store, idx);
            policy::save_policy(&store, policy_path).await?;
        }
        drop(store);

        let exit_code = execute_streaming(argv, id, writer).await?;
        audit.log_exec(id, exit_code).await;
        return Ok(());
    }

    // No policy match.
    audit.log_policy_miss(id).await;

    if context == ExecContext::Unattended {
        info!(id, %argv_joined, "no policy match in unattended mode — denying");
        audit.log_denied(id, "no_policy").await;
        drop(store);
        let msg = PrivilegedMessage::Denied {
            id: id.to_string(),
            reason: DenyReason::NoPolicy,
        };
        send_message(writer, &msg).await?;
        return Ok(());
    }

    // Interactive: ask for approval.
    drop(store);
    info!(id, %argv_joined, "no policy match — requesting approval");
    let approval_needed = PrivilegedMessage::ApprovalNeeded {
        id: id.to_string(),
        command: argv_joined.clone(),
    };
    send_message(writer, &approval_needed).await?;

    // Wait for Approval response with 60s timeout.
    type ApprovalResult = (ApprovalDecision, Option<u64>, Option<String>, Option<String>);
    let approval: std::result::Result<anyhow::Result<ApprovalResult>, _> =
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            while let Some(line) = lines.next_line().await.transpose() {
                let line = line?;
                let msg: PrivilegedMessage = serde_json::from_str(&line)?;
                if let PrivilegedMessage::Approval {
                    id: ref aid,
                    decision,
                    duration_secs,
                    pattern,
                    user,
                    ..
                } = msg
                    && aid == id
                {
                    return Ok((decision, duration_secs, pattern, user));
                }
            }
            anyhow::bail!("connection closed while waiting for approval");
        })
        .await;

    match approval {
        Ok(Ok((decision, duration_secs, pattern, user))) => {
            audit
                .log_approval(id, &format!("{decision:?}"))
                .await;

            match decision {
                ApprovalDecision::Deny => {
                    audit.log_denied(id, "user_denied").await;
                    let msg = PrivilegedMessage::Denied {
                        id: id.to_string(),
                        reason: DenyReason::UserDenied,
                    };
                    send_message(writer, &msg).await?;
                }
                ApprovalDecision::AllowOnce
                | ApprovalDecision::AllowTimed
                | ApprovalDecision::AllowAlways => {
                    let grant_pattern =
                        pattern.unwrap_or_else(|| argv_joined.clone());
                    let created_by = user.unwrap_or_else(|| "unknown".to_string());

                    let grant_type = match decision {
                        ApprovalDecision::AllowOnce => GrantType::Once,
                        ApprovalDecision::AllowTimed => GrantType::Timed,
                        ApprovalDecision::AllowAlways => GrantType::Always,
                        ApprovalDecision::Deny => unreachable!(),
                    };

                    let expires = if grant_type == GrantType::Timed {
                        let secs = duration_secs.unwrap_or(300);
                        Some(
                            Utc::now()
                                + chrono::TimeDelta::try_seconds(secs as i64)
                                    .unwrap_or(chrono::TimeDelta::seconds(300)),
                        )
                    } else {
                        None
                    };

                    let grant = Grant {
                        pattern: grant_pattern,
                        grant_type,
                        expires,
                        created: Utc::now(),
                        created_by,
                    };

                    let mut store = policy_lock.lock().await;
                    store.grants.push(grant);
                    policy::save_policy(&store, policy_path).await?;
                    drop(store);

                    let exit_code = execute_streaming(argv, id, writer).await?;
                    audit.log_exec(id, exit_code).await;
                }
            }
        }
        Ok(Err(e)) => {
            error!(id, "error waiting for approval: {e:#}");
            audit.log_denied(id, "error").await;
            let msg = PrivilegedMessage::Denied {
                id: id.to_string(),
                reason: DenyReason::Timeout,
            };
            send_message(writer, &msg).await?;
        }
        Err(_) => {
            warn!(id, "approval timed out");
            audit.log_denied(id, "timeout").await;
            let msg = PrivilegedMessage::Denied {
                id: id.to_string(),
                reason: DenyReason::Timeout,
            };
            send_message(writer, &msg).await?;
        }
    }

    Ok(())
}
