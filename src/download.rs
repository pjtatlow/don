//! Artifact downloading, verification, and caching.
//!
//! Downloads platform-specific artifacts to `.don/cache/<sha256>/`, verifies
//! SHA-256 hashes, extracts archives (tar.gz, zip), and runs one-time setup
//! commands. Cache hits skip the network entirely.

use futures_util::StreamExt;
use hex::encode;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncWriteExt as _;

use crate::config::PlatformDownload;

/// Marker file written after a successful setup command to prevent re-running.
const SETUP_MARKER: &str = ".setup-marker";

/// HTTP request timeout for downloads. Must be long enough for large archives
/// on slow connections (e.g. CockroachDB at ~300MB).
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);

/// Maximum time allowed for an artifact setup command.
#[cfg(not(test))]
const SETUP_TIMEOUT: Duration = Duration::from_secs(300);
#[cfg(test)]
const SETUP_TIMEOUT: Duration = Duration::from_millis(200);

/// Log a progress line after this many bytes received.
const PROGRESS_INTERVAL_BYTES: u64 = 10 * 1024 * 1024;

/// Prune cache entries under `cache_base` (`<cache_base>/<owner>/<hash>/`)
/// that aren't present in the `keep` set of `(owner_name, composite_hash)` pairs.
/// Returns the list of removed directory paths.
///
/// Also removes any top-level owner dir that's not referenced at all.
/// Skips dotfiles (locks, download temps, staging dirs).
pub fn prune_cache(
    cache_base: &Path,
    keep: &std::collections::HashSet<(String, String)>,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut removed = Vec::new();
    let owner_entries = match std::fs::read_dir(cache_base) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(removed),
        Err(e) => return Err(e),
    };
    let owners_in_keep: std::collections::HashSet<&str> =
        keep.iter().map(|(o, _)| o.as_str()).collect();

    for owner_entry in owner_entries {
        let owner_entry = owner_entry?;
        let owner_path = owner_entry.path();
        let owner_name = match owner_entry.file_name().to_str() {
            Some(n) => n.to_string(),
            None => continue,
        };
        if owner_name.starts_with('.') || !owner_path.is_dir() {
            continue;
        }
        // If the owner isn't in config at all, remove its whole dir.
        if !owners_in_keep.contains(owner_name.as_str()) {
            std::fs::remove_dir_all(&owner_path)?;
            removed.push(owner_path);
            continue;
        }
        // Walk hash subdirs for this owner.
        for hash_entry in std::fs::read_dir(&owner_path)? {
            let hash_entry = hash_entry?;
            let hash_path = hash_entry.path();
            let hash_name = match hash_entry.file_name().to_str() {
                Some(n) => n.to_string(),
                None => continue,
            };
            // Skip dotfiles (locks, staging dirs, download temps).
            if hash_name.starts_with('.') {
                continue;
            }
            if hash_name.len() != 64 || !hash_name.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }
            if !hash_path.is_dir() {
                continue;
            }
            if !keep.contains(&(owner_name.clone(), hash_name)) {
                std::fs::remove_dir_all(&hash_path)?;
                removed.push(hash_path);
            }
        }
    }
    Ok(removed)
}

/// Create a symlink in `bin_dir` pointing at the artifact's binary. The symlink
/// name is the binary's filename (last path component). Replaces any existing
/// symlink at that path.
///
/// This makes downloaded binaries reachable by name from other services/tasks
/// when `bin_dir` is on `PATH`.
pub fn link_binary(
    artifact: &PlatformDownload,
    cache_base: &Path,
    owner_name: &str,
    bin_name: &str,
    bin_dir: &Path,
) -> Result<(), DownloadError> {
    let binary_path = match artifact.binary_path(cache_base, owner_name) {
        Some(p) => p,
        None => return Ok(()),
    };
    std::fs::create_dir_all(bin_dir)?;
    let link_path = bin_dir.join(bin_name);

    // Canonicalize the target so the symlink doesn't break when resolved
    // from the bin_dir's perspective. (A relative target is resolved relative
    // to the directory containing the symlink, not the process cwd.)
    let absolute_target = binary_path.canonicalize()?;

    // Remove existing link/file if present (may point at a stale cache entry).
    let _ = std::fs::remove_file(&link_path);

    #[cfg(unix)]
    std::os::unix::fs::symlink(&absolute_target, &link_path)?;
    #[cfg(not(unix))]
    std::fs::copy(&absolute_target, &link_path)?;

    Ok(())
}

/// Errors that can occur during artifact download and verification.
#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    /// HTTP request failed.
    #[error("failed to download '{url}': {source}")]
    Http {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    /// Downloaded file hash does not match the expected SHA-256.
    #[error("SHA-256 mismatch for '{url}': expected {expected}, got {actual}")]
    Sha256Mismatch {
        url: String,
        expected: String,
        actual: String,
    },
    /// Archive extraction failed.
    #[error("failed to extract archive from '{url}': {source}")]
    Extract {
        url: String,
        #[source]
        source: std::io::Error,
    },
    /// Setup command failed.
    #[error("setup command '{cmd}' failed: {message}")]
    Setup { cmd: String, message: String },
    /// Header configuration error (e.g., missing env var).
    #[error("invalid download header: {message}")]
    Headers { message: String },
    /// Generic I/O error.
    #[error("download I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Archive type detected from the URL file extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveType {
    TarGz,
    TarXz,
    TarBz2,
    TarZst,
    Zip,
    /// Not an archive — treat as a bare binary.
    None,
}

/// Detect archive type from a URL's file extension.
pub fn detect_archive_type(url: &str) -> ArchiveType {
    // Strip query string and fragment before checking extension.
    let path = url.split(['?', '#']).next().unwrap_or(url);
    if path.ends_with(".tar.gz") || path.ends_with(".tgz") {
        ArchiveType::TarGz
    } else if path.ends_with(".tar.xz") || path.ends_with(".txz") {
        ArchiveType::TarXz
    } else if path.ends_with(".tar.bz2") || path.ends_with(".tbz2") || path.ends_with(".tbz") {
        ArchiveType::TarBz2
    } else if path.ends_with(".tar.zst") || path.ends_with(".tzst") {
        ArchiveType::TarZst
    } else if path.ends_with(".zip") {
        ArchiveType::Zip
    } else {
        ArchiveType::None
    }
}

/// Ensure an artifact is downloaded, verified, and ready in the cache.
///
/// If the cache directory already exists with the expected content, this is a
/// no-op. Otherwise, downloads the file, verifies its SHA-256 hash, extracts
/// archives, and runs the setup command (if any).
///
/// `service_writer` is used for progress output if available.
pub async fn ensure_artifact(
    artifact: &PlatformDownload,
    cache_base: &Path,
    owner_name: &str,
    service_writer: Option<&crate::output::ServiceWriter>,
) -> Result<(), DownloadError> {
    let cache_dir = artifact.cache_dir(cache_base, owner_name);

    // Cache hit: directory exists, skip download.
    if cache_dir.is_dir() {
        if let Some(writer) = service_writer {
            writer
                .write_line("artifact cached, skipping download")
                .await;
        }
        // Still run setup if marker is missing (e.g., previous setup failed).
        run_setup_if_needed(artifact, &cache_dir, service_writer).await?;
        return Ok(());
    }

    // Owner namespace dir: `<cache_base>/<owner>/`. Lock, download, and
    // staging paths all live under it so they sort with their real cache dir.
    let owner_dir = cache_base.join(owner_name);
    std::fs::create_dir_all(&owner_dir)?;

    let composite = artifact.composite_hash();

    // Acquire an exclusive lock on this composite hash to serialize concurrent
    // downloads of the same artifact (multiple services sharing one binary).
    let lock_path = owner_dir.join(format!(".lock-{composite}"));
    let lock = acquire_download_lock(&lock_path, service_writer).await?;

    // Re-check the cache after acquiring the lock — another process may have
    // finished the download while we were waiting.
    if cache_dir.is_dir() {
        drop(lock);
        let _ = std::fs::remove_file(&lock_path);
        if let Some(writer) = service_writer {
            writer
                .write_line("artifact cached, skipping download")
                .await;
        }
        run_setup_if_needed(artifact, &cache_dir, service_writer).await?;
        return Ok(());
    }

    // Paths for atomic extraction: download + stage in temp locations, then
    // rename atomically to the final cache dir.
    let download_path = owner_dir.join(format!(".download-{composite}"));
    let staging_dir = owner_dir.join(format!(".staging-{composite}"));

    // Clean up any stale staging artifacts from a previous interrupted run.
    let _ = std::fs::remove_file(&download_path);
    let _ = std::fs::remove_dir_all(&staging_dir);

    if let Some(writer) = service_writer {
        writer
            .write_line(&format!("downloading {}", artifact.url))
            .await;
    }

    // Expand header env vars up front so we fail early on missing vars.
    let headers = match expand_headers(&artifact.headers) {
        Ok(h) => h,
        Err(msg) => {
            let _ = std::fs::remove_file(&download_path);
            drop(lock);
            let _ = std::fs::remove_file(&lock_path);
            return Err(DownloadError::Headers { message: msg });
        }
    };

    // Download with streaming + inline SHA-256 verification.
    let result = async {
        download_and_verify(
            &artifact.url,
            &download_path,
            &artifact.sha256,
            &headers,
            service_writer,
        )
        .await?;

        if let Some(writer) = service_writer {
            writer.write_line("SHA-256 verified").await;
        }

        extract_download(&download_path, &staging_dir, &artifact.url).await?;
        // Atomic rename of the extracted tree into its final home.
        std::fs::rename(&staging_dir, &cache_dir).map_err(DownloadError::Io)?;
        Ok::<(), DownloadError>(())
    }
    .await;

    // Always clean up the download file and any leftover staging dir.
    let _ = std::fs::remove_file(&download_path);
    let _ = std::fs::remove_dir_all(&staging_dir);

    // Release the lock (drop guard) and remove the lock file.
    drop(lock);
    let _ = std::fs::remove_file(&lock_path);

    result?;

    // Run setup command if configured.
    run_setup_if_needed(artifact, &cache_dir, service_writer).await?;

    Ok(())
}

/// A held flock — released when dropped.
struct DownloadLock {
    _file: std::fs::File,
}

/// Acquire an exclusive file lock for this artifact's download.
///
/// Uses non-blocking flock attempts with async sleeps so dropping the future
/// during shutdown does not strand a `spawn_blocking` worker behind another
/// process's long download.
async fn acquire_download_lock(
    lock_path: &Path,
    service_writer: Option<&crate::output::ServiceWriter>,
) -> Result<DownloadLock, DownloadError> {
    use std::os::fd::AsRawFd;

    let lock_path = lock_path.to_path_buf();
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;

    let mut logged_wait = false;
    loop {
        let fd = file.as_raw_fd();
        let try_result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if try_result == 0 {
            return Ok(DownloadLock { _file: file });
        }
        let errno = std::io::Error::last_os_error();
        if errno.raw_os_error() == Some(libc::EWOULDBLOCK) {
            if !logged_wait && let Some(writer) = service_writer {
                writer
                    .write_line("another process is downloading this artifact, waiting...")
                    .await;
            }
            logged_wait = true;
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return Err(DownloadError::Io(errno));
    }
}

/// Stream a download to `dest` while computing the SHA-256 hash. Verifies the
/// hash against `expected_sha256` when the stream completes. Enforces
/// `DOWNLOAD_TIMEOUT` as a total request budget.
async fn download_and_verify(
    url: &str,
    dest: &Path,
    expected_sha256: &str,
    headers: &std::collections::HashMap<String, String>,
    service_writer: Option<&crate::output::ServiceWriter>,
) -> Result<(), DownloadError> {
    let client = reqwest::Client::builder()
        .timeout(DOWNLOAD_TIMEOUT)
        .build()
        .map_err(|e| DownloadError::Http {
            url: url.to_string(),
            source: e,
        })?;

    let mut request = client.get(url);
    for (k, v) in headers {
        request = request.header(k, v);
    }
    let response = request.send().await.map_err(|e| DownloadError::Http {
        url: url.to_string(),
        source: e,
    })?;

    let response = response
        .error_for_status()
        .map_err(|e| DownloadError::Http {
            url: url.to_string(),
            source: e,
        })?;

    let total_size = response.content_length();
    let mut file = tokio::fs::File::create(dest).await?;
    let mut hasher = Sha256::new();
    let mut stream = response.bytes_stream();
    let mut bytes_received: u64 = 0;
    let mut next_progress_log: u64 = PROGRESS_INTERVAL_BYTES;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| DownloadError::Http {
            url: url.to_string(),
            source: e,
        })?;
        file.write_all(&chunk).await?;
        hasher.update(&chunk);
        bytes_received += chunk.len() as u64;

        if bytes_received >= next_progress_log
            && let Some(writer) = service_writer
        {
            writer
                .write_line(&format_progress(bytes_received, total_size))
                .await;
            next_progress_log += PROGRESS_INTERVAL_BYTES;
        }
    }
    file.flush().await?;
    drop(file);

    let actual = encode(hasher.finalize());
    if actual != expected_sha256.to_lowercase() {
        return Err(DownloadError::Sha256Mismatch {
            url: url.to_string(),
            expected: expected_sha256.to_string(),
            actual,
        });
    }
    Ok(())
}

async fn extract_download(
    download_path: &Path,
    staging_dir: &Path,
    url: &str,
) -> Result<(), DownloadError> {
    let download_path = download_path.to_path_buf();
    let staging_dir = staging_dir.to_path_buf();
    let url = url.to_string();
    tokio::task::spawn_blocking(move || match detect_archive_type(&url) {
        ArchiveType::TarGz => extract_tar_gz(&download_path, &staging_dir, &url),
        ArchiveType::TarXz => extract_tar_xz(&download_path, &staging_dir, &url),
        ArchiveType::TarBz2 => extract_tar_bz2(&download_path, &staging_dir, &url),
        ArchiveType::TarZst => extract_tar_zst(&download_path, &staging_dir, &url),
        ArchiveType::Zip => extract_zip(&download_path, &staging_dir, &url),
        ArchiveType::None => place_bare_binary(&download_path, &staging_dir, &url),
    })
    .await
    .map_err(|e| DownloadError::Io(std::io::Error::other(e)))?
}

/// Expand `${VAR}` references in header values against the process
/// environment. Returns an error if any referenced var is missing.
fn expand_headers(
    headers: &std::collections::HashMap<String, String>,
) -> Result<std::collections::HashMap<String, String>, String> {
    let mut out = std::collections::HashMap::with_capacity(headers.len());
    for (k, v) in headers {
        out.insert(k.clone(), expand_env_vars(v, k)?);
    }
    Ok(out)
}

/// Expand `${VAR}` references in a string against the process environment.
/// `context` is the header name, used for error messages.
fn expand_env_vars(input: &str, context: &str) -> Result<String, String> {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == '$' && chars.peek().map(|(_, c)| *c) == Some('{') {
            // Find the closing '}'.
            let rest = &input[i + 2..];
            if let Some(end) = rest.find('}') {
                let var_name = &rest[..end];
                let value = std::env::var(var_name).map_err(|_| {
                    format!("header '{context}' references env var '{var_name}' which is not set")
                })?;
                out.push_str(&value);
                // Advance the iterator past the `${VAR}`.
                for _ in 0..(end + 2) {
                    chars.next();
                }
                continue;
            }
        }
        out.push(c);
    }
    Ok(out)
}

/// Format a progress line: "downloaded 10.0/300.5 MB" or "downloaded 10.0 MB"
/// when the server didn't provide Content-Length.
fn format_progress(bytes: u64, total: Option<u64>) -> String {
    let mb = bytes as f64 / 1_048_576.0;
    match total {
        Some(t) => {
            let total_mb = t as f64 / 1_048_576.0;
            format!("downloaded {mb:.1}/{total_mb:.1} MB")
        }
        None => format!("downloaded {mb:.1} MB"),
    }
}

/// Verify that a file's SHA-256 hash matches the expected value.
pub async fn verify_sha256(path: &Path, expected: &str, url: &str) -> Result<(), DownloadError> {
    let path = path.to_path_buf();
    let expected = expected.to_string();
    let url = url.to_string();

    tokio::task::spawn_blocking(move || verify_sha256_sync(&path, &expected, &url))
        .await
        .map_err(|e| DownloadError::Io(std::io::Error::other(e)))?
}

/// Synchronous SHA-256 verification.
fn verify_sha256_sync(path: &Path, expected: &str, url: &str) -> Result<(), DownloadError> {
    let data = std::fs::read(path)?;
    let actual = compute_sha256(&data);

    if actual != expected.to_lowercase() {
        return Err(DownloadError::Sha256Mismatch {
            url: url.to_string(),
            expected: expected.to_string(),
            actual,
        });
    }
    Ok(())
}

/// Compute the SHA-256 hex digest of a byte slice.
pub fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    encode(hasher.finalize())
}

/// Extract a .tar.gz archive into the cache directory.
///
/// Guards against path traversal: entries with absolute paths or `..`
/// components are rejected with a clear error.
fn extract_tar_gz(archive_path: &Path, cache_dir: &Path, url: &str) -> Result<(), DownloadError> {
    let file = std::fs::File::open(archive_path).map_err(|e| DownloadError::Extract {
        url: url.to_string(),
        source: e,
    })?;
    let decoder = flate2::read::GzDecoder::new(file);
    let archive = tar::Archive::new(decoder);
    std::fs::create_dir_all(cache_dir)?;
    unpack_tar(archive, cache_dir, url)
}

/// Extract a .tar.xz archive into the cache directory.
fn extract_tar_xz(archive_path: &Path, cache_dir: &Path, url: &str) -> Result<(), DownloadError> {
    let file = std::fs::File::open(archive_path).map_err(|e| DownloadError::Extract {
        url: url.to_string(),
        source: e,
    })?;
    let decoder = xz2::read::XzDecoder::new(file);
    let archive = tar::Archive::new(decoder);
    std::fs::create_dir_all(cache_dir)?;
    unpack_tar(archive, cache_dir, url)
}

/// Extract a .tar.bz2 archive into the cache directory.
fn extract_tar_bz2(archive_path: &Path, cache_dir: &Path, url: &str) -> Result<(), DownloadError> {
    let file = std::fs::File::open(archive_path).map_err(|e| DownloadError::Extract {
        url: url.to_string(),
        source: e,
    })?;
    let decoder = bzip2::read::BzDecoder::new(file);
    let archive = tar::Archive::new(decoder);
    std::fs::create_dir_all(cache_dir)?;
    unpack_tar(archive, cache_dir, url)
}

/// Extract a .tar.zst archive into the cache directory.
fn extract_tar_zst(archive_path: &Path, cache_dir: &Path, url: &str) -> Result<(), DownloadError> {
    let file = std::fs::File::open(archive_path).map_err(|e| DownloadError::Extract {
        url: url.to_string(),
        source: e,
    })?;
    let decoder = zstd::stream::read::Decoder::new(file).map_err(|e| DownloadError::Extract {
        url: url.to_string(),
        source: e,
    })?;
    let archive = tar::Archive::new(decoder);
    std::fs::create_dir_all(cache_dir)?;
    unpack_tar(archive, cache_dir, url)
}

/// Safely unpack a tar archive: validate each entry path before extracting.
fn unpack_tar<R: std::io::Read>(
    mut archive: tar::Archive<R>,
    cache_dir: &Path,
    url: &str,
) -> Result<(), DownloadError> {
    for entry in archive.entries().map_err(|e| DownloadError::Extract {
        url: url.to_string(),
        source: e,
    })? {
        let mut entry = entry.map_err(|e| DownloadError::Extract {
            url: url.to_string(),
            source: e,
        })?;
        let entry_path = entry.path().map_err(|e| DownloadError::Extract {
            url: url.to_string(),
            source: e,
        })?;
        if !is_safe_entry_path(&entry_path) {
            return Err(DownloadError::Extract {
                url: url.to_string(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsafe tar entry path: {}", entry_path.display()),
                ),
            });
        }
        entry
            .unpack_in(cache_dir)
            .map_err(|e| DownloadError::Extract {
                url: url.to_string(),
                source: e,
            })?;
    }
    Ok(())
}

/// True if the archive entry path is safe to extract under a base dir:
/// no absolute paths and no `..` components.
fn is_safe_entry_path(path: &Path) -> bool {
    use std::path::Component;
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return false,
        }
    }
    true
}

/// Extract a .zip archive into the cache directory.
fn extract_zip(archive_path: &Path, cache_dir: &Path, url: &str) -> Result<(), DownloadError> {
    let file = std::fs::File::open(archive_path).map_err(|e| DownloadError::Extract {
        url: url.to_string(),
        source: e,
    })?;
    let mut archive = zip::ZipArchive::new(file).map_err(|e| DownloadError::Extract {
        url: url.to_string(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
    })?;

    std::fs::create_dir_all(cache_dir)?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).map_err(|e| DownloadError::Extract {
            url: url.to_string(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
        })?;

        let Some(entry_path) = entry.enclosed_name() else {
            // Skip entries with unsafe paths (path traversal).
            continue;
        };
        let out_path = cache_dir.join(entry_path);

        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut outfile = std::fs::File::create(&out_path)?;
            std::io::copy(&mut entry, &mut outfile)?;

            // Preserve Unix permissions if available.
            #[cfg(unix)]
            if let Some(mode) = entry.unix_mode() {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(mode))?;
            }
        }
    }

    Ok(())
}

/// Place a bare binary (non-archive) into the cache directory.
fn place_bare_binary(temp_path: &Path, cache_dir: &Path, url: &str) -> Result<(), DownloadError> {
    std::fs::create_dir_all(cache_dir)?;

    let filename = url
        .split(['?', '#'])
        .next()
        .unwrap_or(url)
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| DownloadError::Extract {
            url: url.to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "URL has no filename component",
            ),
        })?;

    let dest = cache_dir.join(filename);
    std::fs::copy(temp_path, &dest)?;

    // Make the binary executable on Unix.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755))?;
    }

    Ok(())
}

/// Run the setup command if configured and the marker file doesn't exist.
async fn run_setup_if_needed(
    artifact: &PlatformDownload,
    cache_dir: &Path,
    service_writer: Option<&crate::output::ServiceWriter>,
) -> Result<(), DownloadError> {
    let setup = match &artifact.setup {
        Some(cmd) => cmd,
        None => return Ok(()),
    };

    let marker_path = cache_dir.join(SETUP_MARKER);
    if marker_path.exists() {
        return Ok(());
    }

    if let Some(writer) = service_writer {
        writer
            .write_line(&format!(
                "running setup: {} {}",
                setup.cmd,
                setup.args.join(" ")
            ))
            .await;
    }

    let mut cmd = tokio::process::Command::new(&setup.cmd);
    cmd.args(&setup.args)
        .current_dir(cache_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let child = cmd.spawn().map_err(|e| DownloadError::Setup {
        cmd: setup.cmd.clone(),
        message: e.to_string(),
    })?;
    let output = match tokio::time::timeout(SETUP_TIMEOUT, child.wait_with_output()).await {
        Ok(result) => result.map_err(|e| DownloadError::Setup {
            cmd: setup.cmd.clone(),
            message: e.to_string(),
        })?,
        Err(_) => {
            return Err(DownloadError::Setup {
                cmd: setup.cmd.clone(),
                message: format!("timed out after {SETUP_TIMEOUT:?}"),
            });
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(DownloadError::Setup {
            cmd: setup.cmd.clone(),
            message: format!("exited with {}: {}", output.status, stderr.trim()),
        });
    }

    // Write marker file to prevent re-running setup.
    std::fs::write(&marker_path, "")?;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_compute_sha256() {
        struct Case {
            name: &'static str,
            input: &'static [u8],
            expected: &'static str,
        }

        let cases = vec![
            Case {
                name: "empty",
                input: b"",
                expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            },
            Case {
                name: "hello world",
                input: b"hello world",
                expected: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
            },
            Case {
                name: "binary data",
                input: &[0x00, 0x01, 0x02, 0xff],
                expected: "3d1f57c984978ef98a18378c8166c1cb8ede02c03eeb6aee7e2f121dfeee3e56",
            },
        ];

        for case in cases {
            let actual = compute_sha256(case.input);
            assert_eq!(actual, case.expected, "case: {}", case.name);
        }
    }

    #[tokio::test]
    async fn test_verify_sha256_correct() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"hello world").unwrap();

        let result = verify_sha256(
            &path,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
            "http://example.com/test.bin",
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_verify_sha256_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"hello world").unwrap();

        let result = verify_sha256(&path, "0000000000000000", "http://example.com/test.bin").await;

        match result {
            Err(DownloadError::Sha256Mismatch {
                expected, actual, ..
            }) => {
                assert_eq!(expected, "0000000000000000");
                assert_eq!(
                    actual,
                    "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                );
            }
            other => panic!("expected Sha256Mismatch, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_verify_sha256_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        std::fs::write(&path, b"").unwrap();

        let result = verify_sha256(
            &path,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "http://example.com/empty.bin",
        )
        .await;

        assert!(result.is_ok());
    }

    #[test]
    fn test_detect_archive_type() {
        struct Case {
            name: &'static str,
            url: &'static str,
            expected: ArchiveType,
        }

        let cases = vec![
            Case {
                name: "tar.gz",
                url: "https://example.com/tool-v1.0.tar.gz",
                expected: ArchiveType::TarGz,
            },
            Case {
                name: "tgz",
                url: "https://example.com/tool-v1.0.tgz",
                expected: ArchiveType::TarGz,
            },
            Case {
                name: "tar.xz",
                url: "https://example.com/tool-v1.0.tar.xz",
                expected: ArchiveType::TarXz,
            },
            Case {
                name: "txz",
                url: "https://example.com/tool-v1.0.txz",
                expected: ArchiveType::TarXz,
            },
            Case {
                name: "tar.bz2",
                url: "https://example.com/tool-v1.0.tar.bz2",
                expected: ArchiveType::TarBz2,
            },
            Case {
                name: "tar.zst",
                url: "https://example.com/tool-v1.0.tar.zst",
                expected: ArchiveType::TarZst,
            },
            Case {
                name: "tzst",
                url: "https://example.com/tool-v1.0.tzst",
                expected: ArchiveType::TarZst,
            },
            Case {
                name: "zip",
                url: "https://example.com/tool-v1.0.zip",
                expected: ArchiveType::Zip,
            },
            Case {
                name: "bare binary",
                url: "https://example.com/mytool-linux-amd64",
                expected: ArchiveType::None,
            },
            Case {
                name: "tar.gz with query string",
                url: "https://example.com/tool.tar.gz?token=abc",
                expected: ArchiveType::TarGz,
            },
            Case {
                name: "zip with fragment",
                url: "https://example.com/tool.zip#v2",
                expected: ArchiveType::Zip,
            },
            Case {
                name: "misleading extension after query",
                url: "https://example.com/binary?file=tool.tar.gz",
                expected: ArchiveType::None,
            },
        ];

        for case in cases {
            let actual = detect_archive_type(case.url);
            assert_eq!(actual, case.expected, "case: {}", case.name);
        }
    }

    #[test]
    fn test_cache_path_construction() {
        let cache_base = PathBuf::from("/tmp/don-cache");
        let artifact = PlatformDownload {
            url: "https://example.com/tool.tar.gz".to_string(),
            sha256: "abc123def456".to_string(),
            path: None,
            setup: None,
            headers: std::collections::HashMap::new(),
        };

        let cache_dir = artifact.cache_dir(&cache_base, "svc");
        let expected_composite = artifact.composite_hash();
        assert_eq!(
            cache_dir,
            PathBuf::from(format!("/tmp/don-cache/svc/{expected_composite}"))
        );
    }

    #[test]
    fn test_is_safe_entry_path() {
        struct Case {
            path: &'static str,
            safe: bool,
        }
        let cases = vec![
            Case {
                path: "foo/bar.txt",
                safe: true,
            },
            Case {
                path: "foo/bar/baz",
                safe: true,
            },
            Case {
                path: "bin/tool",
                safe: true,
            },
            Case {
                path: "./foo",
                safe: true,
            },
            Case {
                path: "../etc/passwd",
                safe: false,
            },
            Case {
                path: "foo/../../etc/passwd",
                safe: false,
            },
            Case {
                path: "/etc/passwd",
                safe: false,
            },
            Case {
                path: "/tmp/evil",
                safe: false,
            },
        ];
        for case in cases {
            let result = is_safe_entry_path(Path::new(case.path));
            assert_eq!(result, case.safe, "path: {}", case.path);
        }
    }

    fn write_tar_with_file<W: std::io::Write>(mut writer: W, filename: &str, content: &[u8]) -> W {
        {
            let mut builder = tar::Builder::new(&mut writer);
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, filename, content).unwrap();
            builder.finish().unwrap();
        }
        writer
    }

    #[test]
    fn test_extract_tar_xz() {
        let dir = tempfile::tempdir().unwrap();
        let archive_path = dir.path().join("test.tar.xz");
        let cache_dir = dir.path().join("extracted");

        let out = std::fs::File::create(&archive_path).unwrap();
        let encoder = xz2::write::XzEncoder::new(out, 6);
        write_tar_with_file(encoder, "test.txt", b"hello from xz")
            .finish()
            .unwrap();

        extract_tar_xz(&archive_path, &cache_dir, "http://example.com/test.tar.xz").unwrap();
        assert_eq!(
            std::fs::read_to_string(cache_dir.join("test.txt")).unwrap(),
            "hello from xz"
        );
    }

    #[test]
    fn test_extract_tar_bz2() {
        let dir = tempfile::tempdir().unwrap();
        let archive_path = dir.path().join("test.tar.bz2");
        let cache_dir = dir.path().join("extracted");

        let out = std::fs::File::create(&archive_path).unwrap();
        let encoder = bzip2::write::BzEncoder::new(out, bzip2::Compression::default());
        write_tar_with_file(encoder, "test.txt", b"hello from bz2")
            .finish()
            .unwrap();

        extract_tar_bz2(&archive_path, &cache_dir, "http://example.com/test.tar.bz2").unwrap();
        assert_eq!(
            std::fs::read_to_string(cache_dir.join("test.txt")).unwrap(),
            "hello from bz2"
        );
    }

    #[test]
    fn test_extract_tar_zst() {
        let dir = tempfile::tempdir().unwrap();
        let archive_path = dir.path().join("test.tar.zst");
        let cache_dir = dir.path().join("extracted");

        let out = std::fs::File::create(&archive_path).unwrap();
        let encoder = zstd::stream::write::Encoder::new(out, 3).unwrap();
        write_tar_with_file(encoder, "test.txt", b"hello from zstd")
            .finish()
            .unwrap();

        extract_tar_zst(&archive_path, &cache_dir, "http://example.com/test.tar.zst").unwrap();
        assert_eq!(
            std::fs::read_to_string(cache_dir.join("test.txt")).unwrap(),
            "hello from zstd"
        );
    }

    #[test]
    fn test_extract_tar_gz() {
        let dir = tempfile::tempdir().unwrap();
        let archive_path = dir.path().join("test.tar.gz");
        let cache_dir = dir.path().join("extracted");

        // Create a tar.gz archive with a test file.
        {
            let file = std::fs::File::create(&archive_path).unwrap();
            let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut builder = tar::Builder::new(encoder);

            let content = b"hello from tar";
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, "test.txt", &content[..])
                .unwrap();
            builder.finish().unwrap();
        }

        extract_tar_gz(&archive_path, &cache_dir, "http://example.com/test.tar.gz").unwrap();

        let extracted = std::fs::read_to_string(cache_dir.join("test.txt")).unwrap();
        assert_eq!(extracted, "hello from tar");
    }

    #[test]
    fn test_extract_zip() {
        let dir = tempfile::tempdir().unwrap();
        let archive_path = dir.path().join("test.zip");
        let cache_dir = dir.path().join("extracted");

        // Create a zip archive with a test file.
        {
            let file = std::fs::File::create(&archive_path).unwrap();
            let mut zip_writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            zip_writer.start_file("test.txt", options).unwrap();
            std::io::Write::write_all(&mut zip_writer, b"hello from zip").unwrap();
            zip_writer.finish().unwrap();
        }

        extract_zip(&archive_path, &cache_dir, "http://example.com/test.zip").unwrap();

        let extracted = std::fs::read_to_string(cache_dir.join("test.txt")).unwrap();
        assert_eq!(extracted, "hello from zip");
    }

    #[test]
    fn test_expand_env_vars() {
        // SAFETY: test-only; we set env vars for the duration of the test.
        unsafe {
            std::env::set_var("DON_TEST_TOKEN", "abc123");
            std::env::set_var("DON_TEST_OTHER", "xyz");
        }

        struct Case {
            name: &'static str,
            input: &'static str,
            expect: Result<&'static str, &'static str>,
        }
        let cases = vec![
            Case {
                name: "no vars",
                input: "Bearer plaintext",
                expect: Ok("Bearer plaintext"),
            },
            Case {
                name: "single var",
                input: "Bearer ${DON_TEST_TOKEN}",
                expect: Ok("Bearer abc123"),
            },
            Case {
                name: "two vars",
                input: "${DON_TEST_TOKEN}-${DON_TEST_OTHER}",
                expect: Ok("abc123-xyz"),
            },
            Case {
                name: "missing var",
                input: "${DON_TEST_MISSING_VAR_XYZ}",
                expect: Err("DON_TEST_MISSING_VAR_XYZ"),
            },
            Case {
                name: "malformed (no closing brace) — left as-is",
                input: "${UNCLOSED",
                expect: Ok("${UNCLOSED"),
            },
            Case {
                name: "literal dollar sign",
                input: "$something",
                expect: Ok("$something"),
            },
        ];
        for case in cases {
            let result = expand_env_vars(case.input, "Authorization");
            match (result, case.expect) {
                (Ok(s), Ok(expected)) => assert_eq!(s, expected, "case: {}", case.name),
                (Err(msg), Err(needle)) => assert!(
                    msg.contains(needle),
                    "case '{}': expected error containing '{needle}', got '{msg}'",
                    case.name
                ),
                (Ok(s), Err(_)) => panic!("case '{}': expected error, got Ok({s})", case.name),
                (Err(e), Ok(_)) => panic!("case '{}': expected Ok, got error: {e}", case.name),
            }
        }
    }

    #[test]
    fn test_prune_cache() {
        use std::collections::HashSet;
        let dir = tempfile::tempdir().unwrap();
        let cache_base = dir.path();

        // Set up: cache_base/<owner>/<hash>/file layout.
        let hash_keep = "a".repeat(64);
        let hash_remove = "b".repeat(64);
        let not_a_hash = "not-a-hash-dir";

        // Owner "alpha" has a kept and a removed hash.
        std::fs::create_dir_all(cache_base.join("alpha").join(&hash_keep)).unwrap();
        std::fs::write(
            cache_base.join("alpha").join(&hash_keep).join("file"),
            b"kept",
        )
        .unwrap();
        std::fs::create_dir_all(cache_base.join("alpha").join(&hash_remove)).unwrap();
        std::fs::write(
            cache_base.join("alpha").join(&hash_remove).join("file"),
            b"gone",
        )
        .unwrap();
        // Also a dotfile inside owner dir (lock file) — should be preserved.
        std::fs::write(cache_base.join("alpha").join(".lock-xyz"), b"").unwrap();
        // And a non-hash dir inside owner — skipped.
        std::fs::create_dir_all(cache_base.join("alpha").join(not_a_hash)).unwrap();

        // Owner "beta" has no entries in keep — whole dir should be removed.
        std::fs::create_dir_all(cache_base.join("beta").join(&hash_keep)).unwrap();

        // Top-level dotfile — should be preserved.
        std::fs::write(cache_base.join(".some-meta"), b"").unwrap();

        let mut keep = HashSet::new();
        keep.insert(("alpha".to_string(), hash_keep.clone()));

        let removed = prune_cache(cache_base, &keep).unwrap();

        assert_eq!(
            removed.len(),
            2,
            "should remove one hash dir + one owner dir"
        );
        assert!(cache_base.join("alpha").join(&hash_keep).exists());
        assert!(!cache_base.join("alpha").join(&hash_remove).exists());
        assert!(cache_base.join("alpha").join(not_a_hash).exists());
        assert!(cache_base.join("alpha").join(".lock-xyz").exists());
        assert!(
            !cache_base.join("beta").exists(),
            "orphaned owner dir removed"
        );
        assert!(cache_base.join(".some-meta").exists());
    }

    #[test]
    fn test_prune_cache_missing_dir_is_ok() {
        use std::collections::HashSet;
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nonexistent");
        let removed = prune_cache(&missing, &HashSet::new()).unwrap();
        assert!(removed.is_empty());
    }

    #[test]
    fn test_link_binary_creates_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let cache_base = dir.path().join("cache");
        let bin_dir = dir.path().join("bin");

        let artifact = PlatformDownload {
            url: "https://example.com/mytool".to_string(),
            sha256: "a".repeat(64),
            path: None,
            setup: None,
            headers: std::collections::HashMap::new(),
        };

        // Create a fake cached binary at the path link_binary will look for.
        let cache_dir = artifact.cache_dir(&cache_base, "svc");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let binary = cache_dir.join("mytool");
        std::fs::write(&binary, b"fake binary").unwrap();

        link_binary(&artifact, &cache_base, "svc", "mytool", &bin_dir).unwrap();

        let link = bin_dir.join("mytool");
        assert!(link.exists(), "symlink should exist");
        assert_eq!(std::fs::read(&link).unwrap(), b"fake binary");

        // Idempotent: calling again should succeed (symlink is replaced).
        link_binary(&artifact, &cache_base, "svc", "mytool", &bin_dir).unwrap();
        assert!(link.exists());
    }

    #[test]
    fn test_place_bare_binary() {
        let dir = tempfile::tempdir().unwrap();
        let temp_path = dir.path().join("download.tmp");
        let cache_dir = dir.path().join("cache-abc123");

        std::fs::write(&temp_path, b"#!/bin/sh\necho hello").unwrap();

        place_bare_binary(
            &temp_path,
            &cache_dir,
            "https://example.com/mytool-linux-amd64",
        )
        .unwrap();

        let binary_path = cache_dir.join("mytool-linux-amd64");
        assert!(binary_path.exists());
        assert_eq!(
            std::fs::read_to_string(&binary_path).unwrap(),
            "#!/bin/sh\necho hello"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&binary_path)
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o755);
        }
    }

    #[tokio::test]
    async fn test_setup_runs_once() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let counter_file = dir.path().join("counter");
        std::fs::write(&counter_file, "0").unwrap();

        // Setup command increments a counter file.
        let artifact = PlatformDownload {
            url: "http://example.com/tool".to_string(),
            sha256: "abc".to_string(),
            path: None,
            setup: Some(crate::config::types::Command {
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

        // First run: setup should execute.
        run_setup_if_needed(&artifact, &cache_dir, None)
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&counter_file).unwrap().trim(), "1");

        // Second run: setup should be skipped (marker file exists).
        run_setup_if_needed(&artifact, &cache_dir, None)
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&counter_file).unwrap().trim(), "1");
    }

    #[tokio::test]
    async fn test_setup_failure() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let artifact = PlatformDownload {
            url: "http://example.com/tool".to_string(),
            sha256: "abc".to_string(),
            path: None,
            setup: Some(crate::config::types::Command {
                cmd: "sh".to_string(),
                args: vec!["-c".to_string(), "exit 1".to_string()],
            }),
            headers: std::collections::HashMap::new(),
        };

        let result = run_setup_if_needed(&artifact, &cache_dir, None).await;
        assert!(result.is_err());

        // Marker file should NOT exist after failure.
        assert!(!cache_dir.join(SETUP_MARKER).exists());
    }

    #[tokio::test]
    async fn test_setup_timeout_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let artifact = PlatformDownload {
            url: "http://example.com/tool".to_string(),
            sha256: "abc".to_string(),
            path: None,
            setup: Some(crate::config::types::Command {
                cmd: "sleep".to_string(),
                args: vec!["5".to_string()],
            }),
            headers: std::collections::HashMap::new(),
        };

        let start = std::time::Instant::now();
        let result = run_setup_if_needed(&artifact, &cache_dir, None).await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "unexpected error: {err}"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(2),
            "setup timeout was not bounded: {:?}",
            start.elapsed()
        );
        assert!(!cache_dir.join(SETUP_MARKER).exists());
    }
}
