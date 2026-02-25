use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use m87_shared::privileged::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use m87_privileged::{audit, handler, policy};

/// Helper: spin up the daemon listener in a temp dir and return the socket path.
struct TestDaemon {
    socket_path: PathBuf,
    policy_path: PathBuf,
    _tmp: tempfile::TempDir,
}

impl TestDaemon {
    async fn start() -> Self {
        Self::start_with_grants(vec![]).await
    }

    async fn start_with_grants(grants: Vec<Grant>) -> Self {
        let tmp = tempfile::tempdir().expect("failed to create tempdir");
        let socket_path = tmp.path().join("privileged.sock");
        let policy_path = tmp.path().join("policy.json");
        let audit_path = tmp.path().join("audit.log");

        // Write initial policy.
        let store = PolicyStore {
            version: 1,
            grants,
        };
        policy::save_policy(&store, &policy_path)
            .await
            .expect("failed to write policy");

        let policy = Arc::new(Mutex::new(store));
        let audit_logger = Arc::new(
            audit::AuditLogger::new(&audit_path)
                .await
                .expect("failed to create audit logger"),
        );

        let listener =
            UnixListener::bind(&socket_path).expect("failed to bind socket");

        let pp = policy_path.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let p = Arc::clone(&policy);
                        let a = Arc::clone(&audit_logger);
                        let pp = pp.clone();
                        tokio::spawn(async move {
                            handler::handle_connection(stream, p, a, pp).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            socket_path,
            policy_path,
            _tmp: tmp,
        }
    }

    async fn connect(&self) -> ClientConn {
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .expect("failed to connect to daemon");
        let (reader, writer) = stream.into_split();
        ClientConn {
            writer,
            lines: BufReader::new(reader).lines(),
        }
    }
}

struct ClientConn {
    writer: tokio::net::unix::OwnedWriteHalf,
    lines: tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
}

impl ClientConn {
    async fn send(&mut self, msg: &PrivilegedMessage) {
        let mut line = serde_json::to_string(msg).unwrap();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
    }

    async fn recv(&mut self) -> PrivilegedMessage {
        let line = self
            .lines
            .next_line()
            .await
            .expect("read error")
            .expect("connection closed unexpectedly");
        serde_json::from_str(&line).expect("invalid JSON from daemon")
    }

    /// Read messages until we get a Result (skipping Output messages).
    /// Returns all Output messages collected along the way, plus the final Result.
    async fn recv_until_result(&mut self) -> (Vec<PrivilegedMessage>, PrivilegedMessage) {
        let mut outputs = vec![];
        loop {
            let msg = self.recv().await;
            match &msg {
                PrivilegedMessage::Result { .. } => return (outputs, msg),
                PrivilegedMessage::Output { .. } => outputs.push(msg),
                other => panic!("unexpected message while waiting for result: {other:?}"),
            }
        }
    }
}

fn make_grant(pattern: &str, grant_type: GrantType) -> Grant {
    Grant {
        pattern: pattern.to_string(),
        grant_type,
        expires: None,
        created: Utc::now(),
        created_by: "test".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test: command matches an existing "always" policy grant -> immediate execution.
#[tokio::test]
async fn policy_match_executes_immediately() {
    let daemon = TestDaemon::start_with_grants(vec![make_grant("echo *", GrantType::Always)]).await;
    let mut client = daemon.connect().await;

    client
        .send(&PrivilegedMessage::Exec {
            id: "req_1".into(),
            argv: vec!["echo".into(), "hello".into(), "world".into()],
            context: ExecContext::Agent,
        })
        .await;

    let (outputs, result) = client.recv_until_result().await;

    // Should have streamed "hello world" on stdout.
    assert!(
        outputs.iter().any(|m| matches!(m, PrivilegedMessage::Output { data, stream, .. }
            if *stream == OutputStream::Stdout && data.contains("hello world"))),
        "expected stdout output containing 'hello world', got: {outputs:?}"
    );

    match result {
        PrivilegedMessage::Result { id, exit_code, .. } => {
            assert_eq!(id, "req_1");
            assert_eq!(exit_code, 0);
        }
        other => panic!("expected Result, got: {other:?}"),
    }
}

/// Test: unattended context with no matching policy -> immediate deny.
#[tokio::test]
async fn unattended_no_policy_denies() {
    let daemon = TestDaemon::start().await;
    let mut client = daemon.connect().await;

    client
        .send(&PrivilegedMessage::Exec {
            id: "req_2".into(),
            argv: vec!["echo".into(), "test".into()],
            context: ExecContext::Unattended,
        })
        .await;

    let msg = client.recv().await;
    match msg {
        PrivilegedMessage::Denied { id, reason } => {
            assert_eq!(id, "req_2");
            assert_eq!(reason, DenyReason::NoPolicy);
        }
        other => panic!("expected Denied, got: {other:?}"),
    }
}

/// Test: agent context, no policy -> approval_needed, user approves once -> executes.
#[tokio::test]
async fn approval_allow_once_flow() {
    let daemon = TestDaemon::start().await;
    let mut client = daemon.connect().await;

    client
        .send(&PrivilegedMessage::Exec {
            id: "req_3".into(),
            argv: vec!["echo".into(), "approved".into()],
            context: ExecContext::Agent,
        })
        .await;

    // Should get ApprovalNeeded.
    let msg = client.recv().await;
    match &msg {
        PrivilegedMessage::ApprovalNeeded { id, command } => {
            assert_eq!(id, "req_3");
            assert_eq!(command, "echo approved");
        }
        other => panic!("expected ApprovalNeeded, got: {other:?}"),
    }

    // Send approval.
    client
        .send(&PrivilegedMessage::Approval {
            id: "req_3".into(),
            decision: ApprovalDecision::AllowOnce,
            duration_secs: None,
            pattern: None,
            user: Some("tester@test.com".into()),
        })
        .await;

    // Should execute and return result.
    let (outputs, result) = client.recv_until_result().await;

    assert!(
        outputs.iter().any(|m| matches!(m, PrivilegedMessage::Output { data, stream, .. }
            if *stream == OutputStream::Stdout && data.contains("approved"))),
        "expected stdout 'approved', got: {outputs:?}"
    );

    match result {
        PrivilegedMessage::Result { exit_code, .. } => assert_eq!(exit_code, 0),
        other => panic!("expected Result, got: {other:?}"),
    }

    // The "once" grant was created and consumed on the first use above.
    // A second request should match the once grant (consuming it), execute, then
    // a *third* request should need approval again since the grant is gone.
    client
        .send(&PrivilegedMessage::Exec {
            id: "req_3b".into(),
            argv: vec!["echo".into(), "approved".into()],
            context: ExecContext::Agent,
        })
        .await;

    // The once grant is consumed on this second exec — it matches, runs, and is deleted.
    let (_, result2) = client.recv_until_result().await;
    assert!(
        matches!(result2, PrivilegedMessage::Result { exit_code: 0, .. }),
        "second exec should match the once grant"
    );

    // Third request — once grant has been consumed, should need approval again.
    client
        .send(&PrivilegedMessage::Exec {
            id: "req_3c".into(),
            argv: vec!["echo".into(), "approved".into()],
            context: ExecContext::Agent,
        })
        .await;

    let msg = client.recv().await;
    assert!(
        matches!(msg, PrivilegedMessage::ApprovalNeeded { .. }),
        "once grant should be consumed, expected ApprovalNeeded on third request"
    );
}

/// Test: agent context, user denies -> denied message.
#[tokio::test]
async fn approval_deny_flow() {
    let daemon = TestDaemon::start().await;
    let mut client = daemon.connect().await;

    client
        .send(&PrivilegedMessage::Exec {
            id: "req_4".into(),
            argv: vec!["echo".into(), "nope".into()],
            context: ExecContext::Agent,
        })
        .await;

    let msg = client.recv().await;
    assert!(matches!(msg, PrivilegedMessage::ApprovalNeeded { .. }));

    // Deny.
    client
        .send(&PrivilegedMessage::Approval {
            id: "req_4".into(),
            decision: ApprovalDecision::Deny,
            duration_secs: None,
            pattern: None,
            user: None,
        })
        .await;

    let msg = client.recv().await;
    match msg {
        PrivilegedMessage::Denied { id, reason } => {
            assert_eq!(id, "req_4");
            assert_eq!(reason, DenyReason::UserDenied);
        }
        other => panic!("expected Denied, got: {other:?}"),
    }
}

/// Test: allow_always creates a persistent grant that works on subsequent requests.
#[tokio::test]
async fn approval_allow_always_persists() {
    let daemon = TestDaemon::start().await;
    let mut client = daemon.connect().await;

    // First request — needs approval.
    client
        .send(&PrivilegedMessage::Exec {
            id: "req_5".into(),
            argv: vec!["echo".into(), "persist".into()],
            context: ExecContext::Agent,
        })
        .await;

    let msg = client.recv().await;
    assert!(matches!(msg, PrivilegedMessage::ApprovalNeeded { .. }));

    // Approve always with a wildcard pattern.
    client
        .send(&PrivilegedMessage::Approval {
            id: "req_5".into(),
            decision: ApprovalDecision::AllowAlways,
            duration_secs: None,
            pattern: Some("echo *".into()),
            user: Some("tester@test.com".into()),
        })
        .await;

    let (_outputs, result) = client.recv_until_result().await;
    assert!(matches!(result, PrivilegedMessage::Result { exit_code: 0, .. }));

    // Second request — should match the new always grant, no approval needed.
    client
        .send(&PrivilegedMessage::Exec {
            id: "req_5b".into(),
            argv: vec!["echo".into(), "second".into()],
            context: ExecContext::Agent,
        })
        .await;

    let (_, result) = client.recv_until_result().await;
    assert!(matches!(result, PrivilegedMessage::Result { exit_code: 0, .. }));

    // Verify the policy was persisted to disk.
    let store = policy::load_policy(&daemon.policy_path)
        .await
        .expect("failed to load policy");
    assert!(
        store.grants.iter().any(|g| g.pattern == "echo *" && g.grant_type == GrantType::Always),
        "expected 'echo *' always grant in policy file"
    );
}

/// Test: timed grant with short duration, then verify expiry.
#[tokio::test]
async fn timed_grant_expires() {
    let daemon = TestDaemon::start().await;
    let mut client = daemon.connect().await;

    // Request -> approve with 1-second timed grant.
    client
        .send(&PrivilegedMessage::Exec {
            id: "req_6".into(),
            argv: vec!["echo".into(), "timed".into()],
            context: ExecContext::Agent,
        })
        .await;

    let msg = client.recv().await;
    assert!(matches!(msg, PrivilegedMessage::ApprovalNeeded { .. }));

    client
        .send(&PrivilegedMessage::Approval {
            id: "req_6".into(),
            decision: ApprovalDecision::AllowTimed,
            duration_secs: Some(1),
            pattern: Some("echo timed".into()),
            user: Some("tester@test.com".into()),
        })
        .await;

    let (_, result) = client.recv_until_result().await;
    assert!(matches!(result, PrivilegedMessage::Result { exit_code: 0, .. }));

    // Wait for the grant to expire.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Same command should now require approval again (grant expired).
    client
        .send(&PrivilegedMessage::Exec {
            id: "req_6b".into(),
            argv: vec!["echo".into(), "timed".into()],
            context: ExecContext::Agent,
        })
        .await;

    let msg = client.recv().await;
    assert!(
        matches!(msg, PrivilegedMessage::ApprovalNeeded { .. }),
        "expected ApprovalNeeded after timed grant expired, got: {msg:?}"
    );
}

/// Test: command that fails returns non-zero exit code.
#[tokio::test]
async fn failed_command_returns_exit_code() {
    let daemon =
        TestDaemon::start_with_grants(vec![make_grant("false", GrantType::Always)]).await;
    let mut client = daemon.connect().await;

    client
        .send(&PrivilegedMessage::Exec {
            id: "req_7".into(),
            argv: vec!["false".into()],
            context: ExecContext::Agent,
        })
        .await;

    let (_, result) = client.recv_until_result().await;
    match result {
        PrivilegedMessage::Result { exit_code, .. } => {
            assert_ne!(exit_code, 0, "false should return non-zero exit code");
        }
        other => panic!("expected Result, got: {other:?}"),
    }
}

/// Test: TTY context also triggers approval flow (same as agent).
#[tokio::test]
async fn tty_context_triggers_approval() {
    let daemon = TestDaemon::start().await;
    let mut client = daemon.connect().await;

    client
        .send(&PrivilegedMessage::Exec {
            id: "req_8".into(),
            argv: vec!["echo".into(), "tty".into()],
            context: ExecContext::Tty,
        })
        .await;

    let msg = client.recv().await;
    match msg {
        PrivilegedMessage::ApprovalNeeded { id, command } => {
            assert_eq!(id, "req_8");
            assert_eq!(command, "echo tty");
        }
        other => panic!("expected ApprovalNeeded for TTY context, got: {other:?}"),
    }
}

/// Test: multiple concurrent connections work independently.
#[tokio::test]
async fn multiple_connections() {
    let daemon =
        TestDaemon::start_with_grants(vec![make_grant("echo *", GrantType::Always)]).await;

    let mut client1 = daemon.connect().await;
    let mut client2 = daemon.connect().await;

    client1
        .send(&PrivilegedMessage::Exec {
            id: "req_a".into(),
            argv: vec!["echo".into(), "from_client_1".into()],
            context: ExecContext::Agent,
        })
        .await;

    client2
        .send(&PrivilegedMessage::Exec {
            id: "req_b".into(),
            argv: vec!["echo".into(), "from_client_2".into()],
            context: ExecContext::Agent,
        })
        .await;

    let (_, result1) = client1.recv_until_result().await;
    let (_, result2) = client2.recv_until_result().await;

    assert!(matches!(result1, PrivilegedMessage::Result { exit_code: 0, .. }));
    assert!(matches!(result2, PrivilegedMessage::Result { exit_code: 0, .. }));
}

// ---------------------------------------------------------------------------
// m87-sudo perspective tests (unattended context)
// ---------------------------------------------------------------------------

/// Test: m87-sudo with a matching policy grant → command executes and output streams.
#[tokio::test]
async fn m87_sudo_policy_match() {
    let daemon =
        TestDaemon::start_with_grants(vec![make_grant("echo *", GrantType::Always)]).await;
    let mut client = daemon.connect().await;

    // m87-sudo always sends Unattended context.
    client
        .send(&PrivilegedMessage::Exec {
            id: "sudo_1".into(),
            argv: vec!["echo".into(), "hello".into(), "from".into(), "sudo".into()],
            context: ExecContext::Unattended,
        })
        .await;

    let (outputs, result) = client.recv_until_result().await;

    assert!(
        outputs.iter().any(|m| matches!(m, PrivilegedMessage::Output { data, stream, .. }
            if *stream == OutputStream::Stdout && data.contains("hello from sudo"))),
        "expected stdout containing 'hello from sudo', got: {outputs:?}"
    );

    match result {
        PrivilegedMessage::Result { id, exit_code, .. } => {
            assert_eq!(id, "sudo_1");
            assert_eq!(exit_code, 0);
        }
        other => panic!("expected Result, got: {other:?}"),
    }
}

/// Test: m87-sudo with no matching policy → immediate deny (exit 126 semantics).
#[tokio::test]
async fn m87_sudo_no_policy_denied() {
    let daemon = TestDaemon::start().await;
    let mut client = daemon.connect().await;

    client
        .send(&PrivilegedMessage::Exec {
            id: "sudo_2".into(),
            argv: vec!["rm".into(), "-rf".into(), "/".into()],
            context: ExecContext::Unattended,
        })
        .await;

    let msg = client.recv().await;
    match msg {
        PrivilegedMessage::Denied { id, reason } => {
            assert_eq!(id, "sudo_2");
            assert_eq!(reason, DenyReason::NoPolicy);
        }
        other => panic!("expected Denied, got: {other:?}"),
    }
}

/// Test: m87-sudo with a matching grant but the command fails → non-zero exit code.
#[tokio::test]
async fn m87_sudo_failed_command() {
    let daemon =
        TestDaemon::start_with_grants(vec![make_grant("false", GrantType::Always)]).await;
    let mut client = daemon.connect().await;

    client
        .send(&PrivilegedMessage::Exec {
            id: "sudo_3".into(),
            argv: vec!["false".into()],
            context: ExecContext::Unattended,
        })
        .await;

    let (_, result) = client.recv_until_result().await;
    match result {
        PrivilegedMessage::Result { id, exit_code, .. } => {
            assert_eq!(id, "sudo_3");
            assert_ne!(exit_code, 0, "false should return non-zero exit code");
        }
        other => panic!("expected Result, got: {other:?}"),
    }
}

/// Test: stderr output is streamed correctly.
#[tokio::test]
async fn stderr_is_streamed() {
    // Use a command that writes to stderr.
    // Grant the exact command since glob `*` doesn't match `/` in paths.
    let daemon = TestDaemon::start_with_grants(vec![make_grant(
        "ls /nonexistent_path_xyz_12345",
        GrantType::Always,
    )])
    .await;
    let mut client = daemon.connect().await;

    client
        .send(&PrivilegedMessage::Exec {
            id: "req_9".into(),
            argv: vec!["ls".into(), "/nonexistent_path_xyz_12345".into()],
            context: ExecContext::Agent,
        })
        .await;

    let (outputs, result) = client.recv_until_result().await;

    // Should have stderr output.
    assert!(
        outputs.iter().any(|m| matches!(m, PrivilegedMessage::Output { stream, .. }
            if *stream == OutputStream::Stderr)),
        "expected stderr output from ls on nonexistent path, got: {outputs:?}"
    );

    // ls on nonexistent path should return non-zero.
    match result {
        PrivilegedMessage::Result { exit_code, .. } => {
            assert_ne!(exit_code, 0);
        }
        other => panic!("expected Result, got: {other:?}"),
    }
}
