use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use m87_shared::privileged::{GrantType, PolicyStore};
use tracing::info;

pub async fn load_policy(path: &Path) -> Result<PolicyStore> {
    if !path.exists() {
        info!("no policy file found at {}, creating default", path.display());
        let store = PolicyStore::default();
        save_policy(&store, path).await?;
        return Ok(store);
    }

    let data = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read policy file: {}", path.display()))?;

    let store: PolicyStore =
        serde_json::from_str(&data).context("failed to parse policy file")?;

    Ok(store)
}

pub async fn save_policy(store: &PolicyStore, path: &Path) -> Result<()> {
    let data = serde_json::to_string_pretty(store).context("failed to serialize policy")?;

    // Atomic write: write to a temp file in the same directory, then rename.
    let dir = path
        .parent()
        .context("policy path has no parent directory")?;
    tokio::fs::create_dir_all(dir).await?;

    let tmp_path = dir.join(".privileged-policy.tmp");
    tokio::fs::write(&tmp_path, data.as_bytes()).await?;
    tokio::fs::rename(&tmp_path, path).await?;

    Ok(())
}

/// Check whether the given command string matches any active grant.
/// Returns the index of the first matching grant, if any.
pub fn check_policy(store: &PolicyStore, argv_joined: &str) -> Option<usize> {
    let now = Utc::now();
    for (i, grant) in store.grants.iter().enumerate() {
        // Skip expired timed grants.
        if grant.grant_type == GrantType::Timed
            && let Some(expires) = grant.expires
            && expires < now
        {
            continue;
        }

        if glob_match::glob_match(&grant.pattern, argv_joined) {
            return Some(i);
        }
    }
    None
}

/// Remove a `Once` grant after it has been consumed.
pub fn consume_once(store: &mut PolicyStore, index: usize) {
    if index < store.grants.len() && store.grants[index].grant_type == GrantType::Once {
        store.grants.remove(index);
    }
}

/// Remove all timed grants that have expired.
pub fn prune_expired(store: &mut PolicyStore) {
    let now = Utc::now();
    store.grants.retain(|g| {
        if g.grant_type == GrantType::Timed {
            match g.expires {
                Some(expires) => expires > now,
                None => true,
            }
        } else {
            true
        }
    });
}
