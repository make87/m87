//! Unified shell detection, argument building, and PATH hardening.
//!
//! Used by `exec.rs`, `terminal.rs`, and `ssh.rs` to consistently
//! detect the user's shell across full Linux desktops, minimal Alpine/BusyBox
//! containers, and Yocto-built embedded images.

use std::path::Path;

/// How the shell will be invoked — determines which flags are safe.
pub enum ShellMode {
    /// Interactive login shell (terminal.rs)
    InteractiveLogin,
    /// Non-interactive command execution without PTY (exec.rs piped mode)
    ExecPiped { command: String },
    /// Command execution with PTY (exec.rs PTY mode)
    ExecPty { command: String },
}

/// Detect the user's login shell with robust fallbacks.
///
/// Detection order:
/// 1. `$SHELL` env var — validated that the binary exists on disk
/// 2. `/etc/passwd` via `libc::getpwuid(geteuid())` — validated
/// 3. Probe fallback chain (includes `/bin/ash` for Alpine/BusyBox)
/// 4. Final unconditional fallback: `/bin/sh`
pub fn detect_shell() -> String {
    if cfg!(windows) {
        return "powershell.exe".to_string();
    }

    // 1. $SHELL env var — validate binary exists
    if let Ok(shell) = std::env::var("SHELL") {
        if !shell.is_empty() && Path::new(&shell).exists() {
            return shell;
        }
    }

    // 2. /etc/passwd via libc
    #[cfg(unix)]
    {
        let shell = shell_from_passwd();
        if let Some(shell) = shell {
            return shell;
        }
    }

    // 3. Probe fallback chain with existence checks
    let candidates = [
        "/bin/bash",
        "/usr/bin/bash",
        "/bin/zsh",
        "/usr/bin/zsh",
        "/usr/bin/fish",
        "/bin/ash",
        "/bin/sh",
    ];
    for candidate in &candidates {
        if Path::new(candidate).exists() {
            return candidate.to_string();
        }
    }

    // 4. Final unconditional fallback
    "/bin/sh".to_string()
}

/// Try to read the user's shell from /etc/passwd via libc.
#[cfg(unix)]
fn shell_from_passwd() -> Option<String> {
    use std::ffi::CStr;
    unsafe {
        let uid = libc::geteuid();
        let pwd = libc::getpwuid(uid);
        if !pwd.is_null() {
            let shell = CStr::from_ptr((*pwd).pw_shell);
            if let Ok(s) = shell.to_str() {
                if !s.is_empty() && Path::new(s).exists() {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
}

/// Returns true if the shell basename supports `-l` (login) flag.
///
/// BusyBox `ash`, `dash`, and generic `sh` do NOT reliably support `-l`,
/// so we only enable it for shells known to handle it.
fn supports_login_flag(shell: &str) -> bool {
    let basename = Path::new(shell)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    matches!(basename, "bash" | "zsh" | "fish")
}

/// Build the correct argument vector for the given shell and mode.
///
/// Uses basename matching to decide which flags are safe — BusyBox `ash`/`dash`
/// and generic `sh` don't reliably support `-l` or `-i` in non-interactive contexts.
pub fn build_shell_args(shell: &str, mode: ShellMode) -> Vec<String> {
    match mode {
        ShellMode::InteractiveLogin => {
            if supports_login_flag(shell) {
                vec!["-l".to_string(), "-i".to_string()]
            } else {
                // Bare invocation — ash/dash/sh will be interactive by default
                // when connected to a PTY
                vec![]
            }
        }
        ShellMode::ExecPiped { command } => {
            if supports_login_flag(shell) {
                vec!["-l".to_string(), "-c".to_string(), command]
            } else {
                vec!["-c".to_string(), command]
            }
        }
        ShellMode::ExecPty { command } => {
            // No -i (causes prompt/MOTD noise), no -l (breaks ash/dash)
            vec!["-c".to_string(), command]
        }
    }
}

/// Merge a minimal set of essential PATH directories with the current `$PATH`.
///
/// Only adds missing directories — never removes existing ones. Safe to call
/// unconditionally, even on systems with a fully populated PATH.
pub fn ensure_minimal_path() -> String {
    let essential = [
        "/usr/local/sbin",
        "/usr/local/bin",
        "/usr/sbin",
        "/usr/bin",
        "/sbin",
        "/bin",
    ];

    let current = std::env::var("PATH").unwrap_or_default();
    let current_dirs: Vec<&str> = if current.is_empty() {
        Vec::new()
    } else {
        current.split(':').collect()
    };

    let mut result: Vec<&str> = current_dirs.clone();
    for dir in &essential {
        if !current_dirs.contains(dir) {
            result.push(dir);
        }
    }

    result.join(":")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_shell_returns_valid_path() {
        let shell = detect_shell();
        assert!(!shell.is_empty());

        #[cfg(unix)]
        {
            assert!(shell.starts_with('/'));
            assert!(Path::new(&shell).exists());
        }

        #[cfg(windows)]
        {
            assert_eq!(shell, "powershell.exe");
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_detect_shell_fallback_exists() {
        let shell = detect_shell();
        assert!(Path::new(&shell).exists());
    }

    #[test]
    fn test_supports_login_flag() {
        assert!(supports_login_flag("/bin/bash"));
        assert!(supports_login_flag("/usr/bin/bash"));
        assert!(supports_login_flag("/bin/zsh"));
        assert!(supports_login_flag("/usr/bin/fish"));
        assert!(!supports_login_flag("/bin/sh"));
        assert!(!supports_login_flag("/bin/ash"));
        assert!(!supports_login_flag("/bin/dash"));
    }

    #[test]
    fn test_build_shell_args_interactive_login_bash() {
        let args = build_shell_args("/bin/bash", ShellMode::InteractiveLogin);
        assert_eq!(args, vec!["-l", "-i"]);
    }

    #[test]
    fn test_build_shell_args_interactive_login_ash() {
        let args = build_shell_args("/bin/ash", ShellMode::InteractiveLogin);
        assert!(args.is_empty());
    }

    #[test]
    fn test_build_shell_args_interactive_login_sh() {
        let args = build_shell_args("/bin/sh", ShellMode::InteractiveLogin);
        assert!(args.is_empty());
    }

    #[test]
    fn test_build_shell_args_exec_piped_bash() {
        let args = build_shell_args(
            "/bin/bash",
            ShellMode::ExecPiped {
                command: "echo hello".to_string(),
            },
        );
        assert_eq!(args, vec!["-l", "-c", "echo hello"]);
    }

    #[test]
    fn test_build_shell_args_exec_piped_ash() {
        let args = build_shell_args(
            "/bin/ash",
            ShellMode::ExecPiped {
                command: "echo hello".to_string(),
            },
        );
        assert_eq!(args, vec!["-c", "echo hello"]);
    }

    #[test]
    fn test_build_shell_args_exec_pty() {
        // PTY mode should never pass -l or -i regardless of shell
        let args = build_shell_args(
            "/bin/bash",
            ShellMode::ExecPty {
                command: "htop".to_string(),
            },
        );
        assert_eq!(args, vec!["-c", "htop"]);

        let args = build_shell_args(
            "/bin/ash",
            ShellMode::ExecPty {
                command: "htop".to_string(),
            },
        );
        assert_eq!(args, vec!["-c", "htop"]);
    }

    #[test]
    fn test_ensure_minimal_path_includes_essentials() {
        // ensure_minimal_path should always include the essential directories
        let path = ensure_minimal_path();
        assert!(path.contains("/usr/bin"));
        assert!(path.contains("/bin"));
        assert!(path.contains("/sbin"));
        assert!(path.contains("/usr/sbin"));
        assert!(path.contains("/usr/local/bin"));
        assert!(path.contains("/usr/local/sbin"));
    }

    #[test]
    fn test_ensure_minimal_path_no_duplicates() {
        let path = ensure_minimal_path();
        // Each essential dir should appear at most once
        for dir in [
            "/usr/local/sbin",
            "/usr/local/bin",
            "/usr/sbin",
            "/usr/bin",
            "/sbin",
            "/bin",
        ] {
            assert_eq!(
                path.split(':').filter(|&d| d == dir).count(),
                1,
                "duplicate for {dir}"
            );
        }
    }
}
