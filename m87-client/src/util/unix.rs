//! Unix-specific utilities for runtime operations.
//! Only compiled when `feature = "runtime"` is enabled.

use anyhow::{bail, Context, Result};
use nix::unistd::{Uid, User};
use std::path::{Path, PathBuf};
use std::process::Command;

/// User identity information resolved from passwd database
#[derive(Debug, Clone)]
pub struct UserInfo {
    pub username: String,
    pub uid: u32,
    pub home_dir: PathBuf,
}

/// Resolve the "real" user who invoked this process.
///
/// Strategy:
/// - If running as root (euid == 0): require SUDO_USER env var
/// - If non-root: lookup current euid in passwd database
///
/// This avoids trusting USER env var which can be manipulated.
pub fn resolve_invoking_user() -> Result<UserInfo> {
    let euid = Uid::effective();

    if euid.is_root() {
        // Running as root - must have SUDO_USER to know who invoked
        let sudo_user = std::env::var("SUDO_USER").context(
            "Running as root but SUDO_USER not set. Cannot determine original user.\n\
             Please run without sudo first, or set SUDO_USER manually.",
        )?;

        let user = User::from_name(&sudo_user)
            .context("Failed to lookup user in passwd database")?
            .ok_or_else(|| {
                anyhow::anyhow!("User '{}' not found in passwd database", sudo_user)
            })?;

        Ok(UserInfo {
            username: user.name,
            uid: user.uid.as_raw(),
            home_dir: user.dir,
        })
    } else {
        // Non-root - lookup effective uid
        let user = User::from_uid(euid)
            .context("Failed to lookup current user in passwd database")?
            .ok_or_else(|| {
                anyhow::anyhow!("Current user (uid {}) not found in passwd database", euid)
            })?;

        Ok(UserInfo {
            username: user.name,
            uid: user.uid.as_raw(),
            home_dir: user.dir,
        })
    }
}

/// Check if the current process has root privileges
pub fn is_root() -> bool {
    Uid::effective().is_root()
}

/// Find the absolute path to systemctl
pub fn find_systemctl() -> Result<PathBuf> {
    // Check common locations
    for path in &["/usr/bin/systemctl", "/bin/systemctl"] {
        let p = PathBuf::from(path);
        if p.exists() {
            return Ok(p);
        }
    }

    // Fallback: try which
    if let Ok(output) = Command::new("which").arg("systemctl").output() {
        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path.is_empty() {
                return Ok(PathBuf::from(path));
            }
        }
    }

    bail!("systemctl not found. Is systemd installed?")
}

/// Validate that a path is safe for use in systemd ExecStart
/// Rejects paths with characters that would require escaping or break unit files
pub fn validate_exec_path(path: &Path) -> Result<()> {
    let path_str = path.to_string_lossy();

    // Check for whitespace (space, tab)
    if path_str.contains(' ') || path_str.contains('\t') {
        bail!(
            "Executable path contains whitespace: '{}'\n\
             Systemd service files require special escaping for paths with whitespace.\n\
             Please install m87 to a path without spaces (e.g., /usr/local/bin/m87)",
            path_str
        );
    }

    // Check for newlines (would break unit file format)
    if path_str.contains('\n') || path_str.contains('\r') {
        bail!(
            "Executable path contains newline characters: '{}'",
            path_str.escape_debug()
        );
    }

    // Check for quotes (would complicate escaping)
    if path_str.contains('"') {
        bail!(
            "Executable path contains quote characters: '{}'",
            path_str
        );
    }

    Ok(())
}

/// Run systemctl with the given arguments, using absolute path
pub fn run_systemctl(args: &[&str]) -> Result<std::process::ExitStatus> {
    let systemctl = find_systemctl()?;

    Command::new(&systemctl)
        .args(args)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .with_context(|| format!("Failed to run systemctl {:?}", args))
}

/// Run systemctl and check for success
pub fn run_systemctl_checked(args: &[&str]) -> Result<()> {
    let status = run_systemctl(args)?;

    if !status.success() {
        bail!(
            "systemctl {:?} failed with exit code {:?}",
            args,
            status.code()
        );
    }

    Ok(())
}

/// Re-execute the current binary with sudo for privileged operations
pub fn reexec_with_sudo(args: &[&str]) -> Result<()> {
    let exe_path =
        std::env::current_exe().context("Failed to get current executable path")?;

    let status = Command::new("sudo")
        .arg(exe_path.as_os_str())
        .args(args)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .context("Failed to execute sudo")?;

    if !status.success() {
        bail!(
            "Privileged operation failed with exit code {:?}",
            status.code()
        );
    }

    Ok(())
}
