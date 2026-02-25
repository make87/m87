use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Protocol messages — newline-delimited JSON over Unix socket
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PrivilegedMessage {
    Exec {
        id: String,
        argv: Vec<String>,
        context: ExecContext,
    },
    ApprovalNeeded {
        id: String,
        command: String,
    },
    Approval {
        id: String,
        decision: ApprovalDecision,
        duration_secs: Option<u64>,
        pattern: Option<String>,
        user: Option<String>,
    },
    Output {
        id: String,
        stream: OutputStream,
        data: String,
    },
    Result {
        id: String,
        exit_code: i32,
        stdout: String,
        stderr: String,
    },
    Denied {
        id: String,
        reason: DenyReason,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecContext {
    Agent,
    Tty,
    Unattended,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    AllowOnce,
    AllowTimed,
    AllowAlways,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DenyReason {
    UserDenied,
    Timeout,
    NoPolicy,
}

// ---------------------------------------------------------------------------
// Policy types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyStore {
    pub version: u32,
    pub grants: Vec<Grant>,
}

impl Default for PolicyStore {
    fn default() -> Self {
        Self {
            version: 1,
            grants: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    pub pattern: String,
    pub grant_type: GrantType,
    pub expires: Option<DateTime<Utc>>,
    pub created: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GrantType {
    Once,
    Timed,
    Always,
}
