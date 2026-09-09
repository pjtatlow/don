#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Command, PlatformDownload};
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::time::Duration;

/// Start a minimal HTTP server that serves files from a directory.
/// Returns the server address (e.g., "127.0.0.1:12345") and a shutdown sender.
async fn start_file_server(
    serve_dir: std::path::PathBuf,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::routing::get;

    let app = axum::Router::new()
        .route(
            "/{*path}",
            get(
                |Path(path): Path<String>, State(dir): State<std::path::PathBuf>| async move {
                    let file_path = dir.join(&path);
                    match std::fs::read(&file_path) {
                        Ok(data) => Ok(data),
                        Err(_) => Err(StatusCode::NOT_FOUND),
                    }
                },
            ),
        )
        .with_state(serve_dir);

    // Bind port 0 and read back what the kernel assigned, rather than asking
    // for a port that was probed and released a moment ago: the listener never
    // stops holding the port, so nothing else on the machine can take it in
    // between and this can't fail with "address already in use".
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    (addr, shutdown_tx)
}

/// Create a tar.gz archive with the given files.
fn create_tar_gz(path: &std::path::Path, files: &[(&str, &[u8])]) {
    let file = std::fs::File::create(path).unwrap();
    let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut builder = tar::Builder::new(encoder);

    for (name, content) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        builder.append_data(&mut header, name, *content).unwrap();
    }
    builder.finish().unwrap();
}

/// Compute the SHA-256 hash of a file.
fn sha256_of_file(path: &std::path::Path) -> String {
    let data = std::fs::read(path).unwrap();
    don::download::compute_sha256(&data)
}

// --- Integration tests ---

#[test]
fn integration_download_and_cache_bare_binary() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("download-bare");
        let serve_dir = dir.child("serve");
        std::fs::create_dir_all(&serve_dir).unwrap();

        // Create a test binary to serve.
        let binary_content = b"#!/bin/sh\necho hello";
        let binary_path = serve_dir.join("mytool-linux-amd64");
        std::fs::write(&binary_path, binary_content).unwrap();
        let sha = sha256_of_file(&binary_path);

        let (addr, shutdown_tx) = start_file_server(serve_dir).await;

        let cache_base = dir.child("cache");
        let artifact = PlatformDownload {
            url: format!("http://{addr}/mytool-linux-amd64"),
            sha256: sha.clone(),
            path: None,
            setup: None,
            headers: std::collections::HashMap::new(),
        };

        // Download and verify.
        don::download::ensure_artifact(&artifact, &cache_base, "test", None)
            .await
            .unwrap();

        // Verify cached at the correct path.
        let cached_binary = artifact.binary_path(&cache_base, "test").unwrap();
        assert!(cached_binary.exists(), "binary should be cached");
        assert_eq!(
            std::fs::read(&cached_binary).unwrap(),
            binary_content.as_slice()
        );

        // Verify executable permissions.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&cached_binary)
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o111, 0o111, "binary should be executable");
        }

        let _ = shutdown_tx.send(());
    });
}

#[test]
fn integration_download_wrong_sha256_fails() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("download-bad-sha");
        let serve_dir = dir.child("serve");
        std::fs::create_dir_all(&serve_dir).unwrap();

        std::fs::write(serve_dir.join("tool"), b"some content").unwrap();

        let (addr, shutdown_tx) = start_file_server(serve_dir).await;

        let cache_base = dir.child("cache");
        let artifact = PlatformDownload {
            url: format!("http://{addr}/tool"),
            sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            path: None,
            setup: None,
            headers: std::collections::HashMap::new(),
        };

        let result = don::download::ensure_artifact(&artifact, &cache_base, "test", None).await;
        assert!(result.is_err(), "should fail with wrong sha256");

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("SHA-256 mismatch"),
            "error should mention SHA-256 mismatch: {err_msg}"
        );

        // Verify no partial cache left behind.
        let cache_dir = artifact.cache_dir(&cache_base, "test");
        assert!(
            !cache_dir.exists(),
            "no cache directory should exist after failure"
        );

        let _ = shutdown_tx.send(());
    });
}

#[test]
fn integration_download_tar_gz_extraction() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("download-tar");
        let serve_dir = dir.child("serve");
        std::fs::create_dir_all(&serve_dir).unwrap();

        // Create a tar.gz with a nested binary.
        let archive_path = serve_dir.join("tool-v1.0.tar.gz");
        create_tar_gz(
            &archive_path,
            &[
                ("tool-v1.0/bin/mytool", b"#!/bin/sh\necho tool"),
                ("tool-v1.0/README.md", b"# Tool"),
            ],
        );
        let sha = sha256_of_file(&archive_path);

        let (addr, shutdown_tx) = start_file_server(serve_dir).await;

        let cache_base = dir.child("cache");
        let artifact = PlatformDownload {
            url: format!("http://{addr}/tool-v1.0.tar.gz"),
            sha256: sha.clone(),
            path: Some("tool-v1.0/bin/mytool".to_string()),
            setup: None,
            headers: std::collections::HashMap::new(),
        };

        don::download::ensure_artifact(&artifact, &cache_base, "test", None)
            .await
            .unwrap();

        // Verify files extracted correctly.
        let binary = artifact
            .cache_dir(&cache_base, "test")
            .join("tool-v1.0/bin/mytool");
        assert!(binary.exists(), "binary should be extracted");
        assert_eq!(
            std::fs::read_to_string(&binary).unwrap(),
            "#!/bin/sh\necho tool"
        );

        let readme = artifact
            .cache_dir(&cache_base, "test")
            .join("tool-v1.0/README.md");
        assert!(readme.exists(), "README should be extracted");

        let _ = shutdown_tx.send(());
    });
}

#[test]
fn integration_download_setup_runs_once() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("download-setup");
        let serve_dir = dir.child("serve");
        std::fs::create_dir_all(&serve_dir).unwrap();

        let binary_content = b"binary-content";
        std::fs::write(serve_dir.join("tool"), binary_content).unwrap();
        let sha = sha256_of_file(&serve_dir.join("tool"));

        let (addr, shutdown_tx) = start_file_server(serve_dir).await;

        let cache_base = dir.child("cache");
        let counter_file = dir.child("counter");
        std::fs::write(&counter_file, "0").unwrap();

        let artifact = PlatformDownload {
            url: format!("http://{addr}/tool"),
            sha256: sha.clone(),
            path: None,
            setup: Some(Command {
                cmd: "sh".to_string(),
                args: vec![
                    "-c".to_string(),
                    format!(
                        "count=$(cat {0}); echo $((count + 1)) > {0}",
                        counter_file.display()
                    ),
                ],
            }),
            headers: std::collections::HashMap::new(),
        };

        // First call: downloads + runs setup.
        don::download::ensure_artifact(&artifact, &cache_base, "test", None)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&counter_file).unwrap().trim(),
            "1",
            "setup should have run once"
        );

        // Second call: cache hit, setup skipped (marker file exists).
        don::download::ensure_artifact(&artifact, &cache_base, "test", None)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&counter_file).unwrap().trim(),
            "1",
            "setup should not run again"
        );

        // Verify marker file exists.
        assert!(
            artifact
                .cache_dir(&cache_base, "test")
                .join(".setup-marker")
                .exists(),
            "setup marker should exist"
        );

        let _ = shutdown_tx.send(());
    });
}

#[test]
fn integration_download_cache_hit_skips_network() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("download-cache-hit");
        let serve_dir = dir.child("serve");
        std::fs::create_dir_all(&serve_dir).unwrap();

        let binary_content = b"cached-binary";
        std::fs::write(serve_dir.join("tool"), binary_content).unwrap();
        let sha = sha256_of_file(&serve_dir.join("tool"));

        let (addr, shutdown_tx) = start_file_server(serve_dir).await;

        let cache_base = dir.child("cache");
        let artifact = PlatformDownload {
            url: format!("http://{addr}/tool"),
            sha256: sha.clone(),
            path: None,
            setup: None,
            headers: std::collections::HashMap::new(),
        };

        // First download.
        don::download::ensure_artifact(&artifact, &cache_base, "test", None)
            .await
            .unwrap();

        // Stop the server — if second call hits the network, it will fail.
        let _ = shutdown_tx.send(());
        // Give the server a moment to shut down.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second call should succeed from cache (no network).
        don::download::ensure_artifact(&artifact, &cache_base, "test", None)
            .await
            .unwrap();

        // Verify the cached file is still there.
        let cached = artifact.cache_dir(&cache_base, "test").join("tool");
        assert!(cached.exists());
        assert_eq!(std::fs::read(&cached).unwrap(), binary_content.as_slice());
    });
}

#[test]
fn integration_task_download_resolves_cmd() {
    // A task with download config runs the cached binary instead of searching PATH.
    run_with_timeout(Duration::from_secs(10), async {
        use don::config::Platform;
        use don::config::Task;

        let dir = TempDir::new("download-task-cmd");
        let serve_dir = dir.child("serve");
        std::fs::create_dir_all(&serve_dir).unwrap();

        // Create a fake binary that echoes a known phrase.
        let binary_content = b"#!/bin/sh\necho hello-from-download\n";
        std::fs::write(serve_dir.join("tool"), binary_content).unwrap();
        let sha = sha256_of_file(&serve_dir.join("tool"));

        let (addr, shutdown_tx) = start_file_server(serve_dir).await;

        let cache_base = dir.child("cache");
        let mut platform_map = std::collections::HashMap::new();
        platform_map.insert(
            Platform::current().unwrap(),
            PlatformDownload {
                url: format!("http://{addr}/tool"),
                sha256: sha.clone(),
                path: None,
                setup: None,
                headers: std::collections::HashMap::new(),
            },
        );
        let download = don::config::DownloadConfig {
            platform: platform_map,
            bin_name: None,
        };

        // Ensure the artifact is staged.
        let artifact = download.for_platform(Platform::current().unwrap()).unwrap();
        don::download::ensure_artifact(artifact, &cache_base, "test", None)
            .await
            .unwrap();

        // Build a Task with this download and verify resolved_cmd points at the cache.
        let task = Task {
            cmd: Some("tool".to_string()),
            args: vec![],
            dir: None,
            env: std::collections::HashMap::new(),
            depends_on: vec![],
            watch: vec![],
            ignore: vec![],
            debounce: None,
            timeout: None,
            log: don::config::LogConfig::Stdout,
            interactive: false,
            headless: None,
            auto_run: don::config::TaskAutoRun::Always,
            download: Some(download),
            bazel: None,
            params: vec![],
            hidden: false,
            auto_filter_on_failure: None,
            secrets: None,
        };
        let resolved = task
            .resolved_cmd(Platform::current().unwrap(), "test", Some(&cache_base))
            .unwrap();
        assert!(
            resolved.as_ref().unwrap().starts_with(&cache_base),
            "resolved path should be under cache: {resolved:?}"
        );
        assert_eq!(resolved.as_ref().unwrap().file_name().unwrap(), "tool");
        assert!(
            resolved.as_ref().unwrap().exists(),
            "resolved binary should exist"
        );

        let _ = shutdown_tx.send(());
    });
}

#[test]
fn integration_download_concurrent_same_sha() {
    // Two concurrent ensure_artifact calls for the same sha should serialize
    // via the download lock — one downloads, the other sees the cache hit.
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("download-concurrent");
        let serve_dir = dir.child("serve");
        std::fs::create_dir_all(&serve_dir).unwrap();

        // Make the file slightly larger so the download isn't instantaneous.
        let binary_content = vec![42u8; 100_000];
        std::fs::write(serve_dir.join("tool"), &binary_content).unwrap();
        let sha = sha256_of_file(&serve_dir.join("tool"));

        let (addr, shutdown_tx) = start_file_server(serve_dir).await;
        let cache_base = dir.child("cache");

        let artifact = PlatformDownload {
            url: format!("http://{addr}/tool"),
            sha256: sha.clone(),
            path: None,
            setup: None,
            headers: std::collections::HashMap::new(),
        };

        // Launch 3 concurrent downloads of the same artifact.
        let cache_base_clone = cache_base.clone();
        let artifact_clone = artifact.clone();
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let cb = cache_base_clone.clone();
                let a = artifact_clone.clone();
                tokio::spawn(
                    async move { don::download::ensure_artifact(&a, &cb, "test", None).await },
                )
            })
            .collect();

        for h in handles {
            h.await.unwrap().unwrap();
        }

        let cached = artifact.cache_dir(&cache_base, "test").join("tool");
        assert!(
            cached.exists(),
            "binary should be cached after concurrent downloads"
        );
        assert_eq!(std::fs::read(&cached).unwrap(), binary_content);

        let _ = shutdown_tx.send(());
    });
}
