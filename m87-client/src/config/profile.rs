//! Profile management.
//!
//! A *profile* is a self-contained pair of `config.json` + `credentials.json`,
//! letting a single machine hold several logged-in accounts and switch between
//! them without deleting and re-creating the local config.
//!
//! Storage layout (under the platform config dir, e.g. `~/.config/m87`):
//!
//! ```text
//! m87/
//! ├── config.json          ← the "default" profile (legacy location)
//! ├── credentials.json     ← the "default" profile credentials
//! ├── active_profile       ← plain-text name of the active profile
//! └── profiles/
//!     ├── work/
//!     │   ├── config.json
//!     │   └── credentials.json
//!     └── personal/
//!         ├── config.json
//!         └── credentials.json
//! ```
//!
//! The `default` profile maps to the legacy top-level files so existing
//! installs keep working untouched. Named profiles live under `profiles/<name>`.
//! The active profile can also be overridden for a single invocation with the
//! `M87_PROFILE` environment variable.

use anyhow::{Context, Result, bail};
use std::fs;
use std::path::PathBuf;

use super::Config;

/// File (under the m87 dir) recording the active profile name.
const ACTIVE_PROFILE_FILE: &str = "active_profile";
/// Sub-directory holding named (non-default) profiles.
const PROFILES_DIR: &str = "profiles";
/// Name of the implicit profile backed by the legacy top-level files.
pub const DEFAULT_PROFILE: &str = "default";
/// Per-invocation override of the active profile.
pub const PROFILE_ENV_VAR: &str = "M87_PROFILE";

/// Root m87 directory (e.g. `~/.config/m87`). Does not create anything.
fn m87_dir() -> Result<PathBuf> {
    Ok(Config::get_config_dir()?.join("m87"))
}

/// Reject names that would escape the profiles directory or collide with the
/// storage layout. Profile names become directory names, so keep them simple.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("profile name cannot be empty");
    }
    if name == DEFAULT_PROFILE {
        // `default` is valid to *use*, but it is implicit and cannot be created
        // or removed; callers that need it special-case it before getting here.
        return Ok(());
    }
    if name == PROFILES_DIR || name == ACTIVE_PROFILE_FILE {
        bail!("'{name}' is a reserved name");
    }
    let valid = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
    if !valid || name.starts_with('.') {
        bail!(
            "invalid profile name '{name}': use only letters, digits, '-', '_' and '.' \
             (not starting with '.')"
        );
    }
    Ok(())
}

/// Name of the currently active profile.
///
/// Resolution order: `M87_PROFILE` env var → `active_profile` file → `default`.
pub fn active_profile() -> Result<String> {
    if let Ok(name) = std::env::var(PROFILE_ENV_VAR) {
        let name = name.trim();
        if !name.is_empty() {
            return Ok(name.to_string());
        }
    }

    let path = m87_dir()?.join(ACTIVE_PROFILE_FILE);
    if path.exists() {
        let name = fs::read_to_string(&path)
            .context("Failed to read active profile file")?
            .trim()
            .to_string();
        if name.is_empty() {
            return Ok(DEFAULT_PROFILE.to_string());
        }
        Ok(name)
    } else {
        Ok(DEFAULT_PROFILE.to_string())
    }
}

/// Directory holding a given profile's `config.json` + `credentials.json`.
///
/// The default profile maps to the top-level m87 dir for backwards
/// compatibility; named profiles live under `profiles/<name>`.
pub fn profile_dir(name: &str) -> Result<PathBuf> {
    let base = m87_dir()?;
    if name == DEFAULT_PROFILE {
        Ok(base)
    } else {
        Ok(base.join(PROFILES_DIR).join(name))
    }
}

/// Directory for the active profile — where `config.json` and
/// `credentials.json` are read from / written to for the current invocation.
pub fn active_profile_dir() -> Result<PathBuf> {
    profile_dir(&active_profile()?)
}

/// Whether a profile already exists on disk.
fn profile_exists(name: &str) -> Result<bool> {
    if name == DEFAULT_PROFILE {
        return Ok(true);
    }
    Ok(profile_dir(name)?.is_dir())
}

/// Persist the active profile name. Mirrors `Config::save`'s sudo-ownership fix
/// so a profile switched under `sudo` stays owned by the original user.
pub fn set_active_profile(name: &str) -> Result<()> {
    let dir = m87_dir()?;
    fs::create_dir_all(&dir).context("Failed to create config directory")?;
    let path = dir.join(ACTIVE_PROFILE_FILE);
    fs::write(&path, name).context("Failed to write active profile file")?;

    #[cfg(unix)]
    if let Ok(sudo_user) = std::env::var("SUDO_USER") {
        use std::process::Command;
        let _ = Command::new("chown")
            .args(["-R", &sudo_user, dir.to_str().unwrap_or("")])
            .status();
    }

    Ok(())
}

/// Create a new (empty) profile directory.
pub fn create_profile(name: &str) -> Result<()> {
    validate_name(name)?;
    if name == DEFAULT_PROFILE {
        bail!("the 'default' profile always exists and cannot be created");
    }
    if profile_exists(name)? {
        bail!("profile '{name}' already exists");
    }
    let dir = profile_dir(name)?;
    fs::create_dir_all(&dir).context("Failed to create profile directory")?;

    #[cfg(unix)]
    if let Ok(sudo_user) = std::env::var("SUDO_USER") {
        use std::process::Command;
        if let Some(base) = m87_dir()?.to_str() {
            let _ = Command::new("chown").args(["-R", &sudo_user, base]).status();
        }
    }

    Ok(())
}

/// Switch to an existing profile. Errors if the profile does not exist.
pub fn switch_profile(name: &str) -> Result<()> {
    validate_name(name)?;
    if !profile_exists(name)? {
        bail!("profile '{name}' does not exist — create it with `m87 profile add {name}`");
    }
    set_active_profile(name)
}

/// Delete a profile and all of its credentials. The default profile cannot be
/// removed. If the removed profile was active, the active profile resets to
/// `default`.
pub fn remove_profile(name: &str) -> Result<()> {
    validate_name(name)?;
    if name == DEFAULT_PROFILE {
        bail!("the 'default' profile cannot be removed");
    }
    if !profile_exists(name)? {
        bail!("profile '{name}' does not exist");
    }
    let dir = profile_dir(name)?;
    fs::remove_dir_all(&dir).context("Failed to remove profile directory")?;

    if active_profile()? == name {
        set_active_profile(DEFAULT_PROFILE)?;
    }
    Ok(())
}

/// Rename a profile. The default profile cannot be renamed (it is the legacy
/// top-level storage). If the renamed profile was active, the active pointer
/// follows it.
pub fn rename_profile(old: &str, new: &str) -> Result<()> {
    validate_name(old)?;
    validate_name(new)?;
    if old == DEFAULT_PROFILE || new == DEFAULT_PROFILE {
        bail!("the 'default' profile cannot be renamed");
    }
    if !profile_exists(old)? {
        bail!("profile '{old}' does not exist");
    }
    if profile_exists(new)? {
        bail!("profile '{new}' already exists");
    }
    let from = profile_dir(old)?;
    let to = profile_dir(new)?;
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent).context("Failed to create profiles directory")?;
    }
    fs::rename(&from, &to).context("Failed to rename profile directory")?;

    if active_profile()? == old {
        set_active_profile(new)?;
    }
    Ok(())
}

/// A profile and a summary of its login state, for display.
pub struct ProfileInfo {
    pub name: String,
    pub active: bool,
    pub logged_in: bool,
    pub owner_reference: Option<String>,
    pub runtime_server_url: Option<String>,
}

/// Read a profile's stored owner reference + server url without activating it.
fn read_profile_config(name: &str) -> Option<(Option<String>, Option<String>)> {
    let path = profile_dir(name).ok()?.join("config.json");
    let contents = fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&contents).ok()?;
    let owner = value
        .get("owner_reference")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let url = value
        .get("runtime_server_url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    Some((owner, url))
}

/// Whether a profile has stored CLI credentials (i.e. is logged in).
fn read_profile_logged_in(name: &str) -> bool {
    let path = match profile_dir(name) {
        Ok(dir) => dir.join("credentials.json"),
        Err(_) => return false,
    };
    let contents = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return false,
    };
    serde_json::from_str::<serde_json::Value>(&contents)
        .ok()
        .and_then(|v| v.get("credentials").cloned())
        .map(|c| !c.is_null())
        .unwrap_or(false)
}

/// List every profile with a summary of its login state. The default profile
/// is always included; named profiles are sorted alphabetically after it.
pub fn list_profiles() -> Result<Vec<ProfileInfo>> {
    let active = active_profile()?;

    let mut names: Vec<String> = vec![DEFAULT_PROFILE.to_string()];
    let profiles_root = m87_dir()?.join(PROFILES_DIR);
    if profiles_root.is_dir() {
        let mut named: Vec<String> = fs::read_dir(&profiles_root)
            .context("Failed to read profiles directory")?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        named.sort();
        names.extend(named);
    }
    // The active profile may live only in the env var / pointer without a dir
    // yet (e.g. just created); make sure it still shows up.
    if !names.iter().any(|n| n == &active) {
        names.push(active.clone());
    }

    Ok(names
        .into_iter()
        .map(|name| {
            let (owner_reference, runtime_server_url) =
                read_profile_config(&name).unwrap_or((None, None));
            ProfileInfo {
                active: name == active,
                logged_in: read_profile_logged_in(&name),
                owner_reference,
                runtime_server_url,
                name,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_simple_names() {
        for name in ["work", "personal", "acme-prod", "team_42", "v1.2"] {
            assert!(validate_name(name).is_ok(), "{name} should be valid");
        }
    }

    #[test]
    fn rejects_empty_name() {
        assert!(validate_name("").is_err());
    }

    #[test]
    fn rejects_path_traversal() {
        for name in ["..", "../escape", "a/b", "a\\b", ".hidden", "/abs"] {
            assert!(validate_name(name).is_err(), "{name} should be rejected");
        }
    }

    #[test]
    fn rejects_reserved_names() {
        assert!(validate_name(PROFILES_DIR).is_err());
        assert!(validate_name(ACTIVE_PROFILE_FILE).is_err());
    }

    #[test]
    fn default_profile_maps_to_legacy_dir() {
        let base = m87_dir().unwrap();
        assert_eq!(profile_dir(DEFAULT_PROFILE).unwrap(), base);
    }

    #[test]
    fn named_profile_lives_under_profiles_subdir() {
        let expected = m87_dir().unwrap().join(PROFILES_DIR).join("work");
        assert_eq!(profile_dir("work").unwrap(), expected);
    }
}
