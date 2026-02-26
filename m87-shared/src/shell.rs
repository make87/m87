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
    fn test_ensure_minimal_path_includes_essentials() {
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
