use std::process::Stdio;
use std::sync::OnceLock;
use tokio::process::Command;
use tokio::sync::OnceCell;

// Use OnceLock for synchronous network creation (doesn't need async)
static NETWORK_CREATED: OnceLock<Result<(), String>> = OnceLock::new();

// Use tokio's OnceCell for async image building
static IMAGES_BUILT: OnceCell<Result<(), String>> = OnceCell::const_new();

pub const NETWORK_NAME: &str = "m87-e2e-network";
pub const SERVER_IMAGE_NAME: &str = "m87-server";
pub const SERVER_IMAGE_TAG: &str = "e2e";
pub const CLIENT_IMAGE_NAME: &str = "m87-client";
pub const CLIENT_IMAGE_TAG: &str = "e2e";
// Full image references for building
pub const SERVER_IMAGE: &str = "m87-server:e2e";
pub const CLIENT_IMAGE: &str = "m87-client:e2e";

/// Build Docker images for E2E tests (runs once per test run)
/// Always rebuilds to pick up code changes - Docker layer caching makes this fast when unchanged
pub async fn ensure_images_built() -> Result<(), String> {
    let result = IMAGES_BUILT
        .get_or_init(|| async { build_images().await })
        .await;

    result.clone()
}

async fn build_images() -> Result<(), String> {
    // Get workspace root (parent of tests/)
    let workspace_root = std::env::current_dir()
        .map(|p| p.parent().map(|p| p.to_path_buf()).unwrap_or(p))
        .unwrap_or_else(|_| std::path::PathBuf::from(".."));

    // 1) Compile the binaries ONCE on the host, reusing the cargo `target/`
    //    cache. This replaces two cold in-Docker release compiles (one per
    //    image, recompiling the whole workspace) with a single incremental host
    //    build. In CI these are already built by a prior workflow step, so this
    //    is a fast up-to-date no-op; locally it just builds what's stale.
    cargo_build(&workspace_root, &["build", "-p", "m87-server"], "m87-server").await?;
    cargo_build(
        &workspace_root,
        &["build", "-p", "m87-client", "--features", "runtime,cli"],
        "m87-client (m87)",
    )
    .await?;

    let debug_dir = workspace_root.join("target").join("debug");
    let server_bin = debug_dir.join("m87-server");
    let client_bin = debug_dir.join("m87");

    // 2) Stage each binary next to its slim COPY-only Dockerfile.e2e in a temp
    //    build context and build the image. A temp context is required because
    //    the repo `.dockerignore` excludes `target/`, so we cannot COPY the
    //    binary from the workspace root.
    build_e2e_image(
        &workspace_root,
        "m87-server/Dockerfile.e2e",
        &server_bin,
        "m87-server",
        SERVER_IMAGE,
    )
    .await?;
    build_e2e_image(
        &workspace_root,
        "m87-client/Dockerfile.e2e",
        &client_bin,
        "m87",
        CLIENT_IMAGE,
    )
    .await?;

    Ok(())
}

/// Run `cargo <args>` from the workspace root (the `tests` crate is excluded
/// from the workspace, so we build the members from the parent dir).
async fn cargo_build(
    workspace_root: &std::path::Path,
    args: &[&str],
    label: &str,
) -> Result<(), String> {
    tracing::info!("Host-building {} (reuses cargo target cache)...", label);
    let status = Command::new("cargo")
        .args(args)
        .current_dir(workspace_root)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await;
    match status {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => Err(format!(
            "Host build of {} failed (exit code: {:?})",
            label,
            s.code()
        )),
        Err(e) => Err(format!("Failed to run cargo build for {}: {}", label, e)),
    }
}

/// Stage `bin` (renamed to `bin_name`) alongside `dockerfile_rel` in a fresh
/// temp context and `docker build` a slim COPY-only image tagged `image`.
async fn build_e2e_image(
    workspace_root: &std::path::Path,
    dockerfile_rel: &str,
    bin: &std::path::Path,
    bin_name: &str,
    image: &str,
) -> Result<(), String> {
    tracing::info!("Building {} (copy-only, host-built binary)...", image);

    let ctx = tempfile::tempdir()
        .map_err(|e| format!("Failed to create temp build context for {}: {}", image, e))?;
    let ctx_path = ctx.path();

    std::fs::copy(bin, ctx_path.join(bin_name)).map_err(|e| {
        format!(
            "Failed to stage binary {} into build context: {}",
            bin.display(),
            e
        )
    })?;
    std::fs::copy(workspace_root.join(dockerfile_rel), ctx_path.join("Dockerfile"))
        .map_err(|e| format!("Failed to stage {} into build context: {}", dockerfile_rel, e))?;

    // `docker buildx build --load` so we can attach a layer cache. The base
    // image + apt layer are stable across runs; only the final `COPY <binary>`
    // layer changes, so caching skips the ~40s apt install every run. GitHub's
    // Actions cache backend (`type=gha`) is only available inside a GHA runner
    // with a `docker-container` buildx builder (set up by setup-buildx-action),
    // so we add the cache flags only there; locally it's a plain buildx build.
    let mut args: Vec<String> = vec![
        "buildx".into(),
        "build".into(),
        "--load".into(),
        "-t".into(),
        image.into(),
    ];
    if std::env::var("GITHUB_ACTIONS").as_deref() == Ok("true") {
        let scope = image.replace(':', "-");
        args.push(format!("--cache-from=type=gha,scope={scope}"));
        // ignore-error keeps a cache-export hiccup (e.g. missing token) from
        // failing the whole image build.
        args.push(format!(
            "--cache-to=type=gha,scope={scope},mode=max,ignore-error=true"
        ));
    }
    args.push(".".into());

    let status = Command::new("docker")
        .args(&args)
        .current_dir(ctx_path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await;

    match status {
        Ok(s) if s.success() => {
            tracing::info!("{} built successfully", image);
            Ok(())
        }
        Ok(s) => Err(format!(
            "Failed to build {} (exit code: {:?})",
            image,
            s.code()
        )),
        Err(e) => Err(format!("Failed to run docker build for {}: {}", image, e)),
    }
}

/// Create Docker network for container communication (runs once per test run)
pub fn ensure_network_created() -> Result<(), String> {
    let result = NETWORK_CREATED.get_or_init(|| create_network());
    result.clone()
}

fn create_network() -> Result<(), String> {
    tracing::info!("Creating E2E Docker network: {}", NETWORK_NAME);

    // First try to remove existing network (ignore errors)
    let _ = std::process::Command::new("docker")
        .args(["network", "rm", NETWORK_NAME])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    // Create network
    let result = std::process::Command::new("docker")
        .args(["network", "create", NETWORK_NAME])
        .output();

    match result {
        Ok(output) if !output.status.success() => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Ignore "already exists" error
            if !stderr.contains("already exists") {
                return Err(format!("Failed to create network: {}", stderr));
            }
        }
        Err(e) => {
            return Err(format!("Failed to run docker network create: {}", e));
        }
        _ => {
            tracing::info!("Docker network created successfully");
        }
    }

    Ok(())
}

/// Clean up network after tests (call manually if needed)
#[allow(dead_code)]
pub fn cleanup_network() {
    let _ = std::process::Command::new("docker")
        .args(["network", "rm", NETWORK_NAME])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}
