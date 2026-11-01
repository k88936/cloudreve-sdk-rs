//! Durable cross-process chunk-resume test.
//!
//! `CLOUDREVE_RESUME_PHASE=first` uploads until the first chunk is persisted,
//! then fails on purpose and exits. `CLOUDREVE_RESUME_PHASE=second` starts a new
//! process, loads the snapshot, skips completed chunks, finishes the upload,
//! and verifies the downloaded content hash. `all` runs both attempts in one
//! process. `scripts/e2e_podman.py` runs the two-process variant.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use cloudreve_sdk_api::api::{ExplorerApi, UserApi};
use cloudreve_sdk_api::models::explorer::FileURLService;
use cloudreve_sdk_api::models::user::LoginResponse;
use cloudreve_sdk_api::{Client, ClientConfig};
use cloudreve_sdk_uploader::{
    ProgressCallback, ProgressUpdate, UploadParams, UploadSource, Uploader, UploaderConfig,
};
use sha2::{Digest, Sha256};
use tokio::io::AsyncRead;

struct NoopProgress;

impl ProgressCallback for NoopProgress {
    fn on_progress(&self, _update: ProgressUpdate) {}
}

fn reader(
    data: Arc<Vec<u8>>,
    offset: u64,
    len: u64,
) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
    let start = offset as usize;
    let end = (offset + len) as usize;
    Ok(Box::new(Cursor::new(data[start..end].to_vec())))
}

fn snapshot_count(dir: &Path) -> Result<usize> {
    Ok(std::fs::read_dir(dir)?.count())
}

/// The download URL is built from Cloudreve's configured SiteURL, which is
/// `http://localhost:5212` in the Podman harness while the published port is
/// random. Rewrite only loopback hosts so the signature (path + query) is kept.
fn retarget(raw: &str, server: &str) -> Result<reqwest::Url> {
    let server = reqwest::Url::parse(server)?;
    let mut url = reqwest::Url::parse(raw).or_else(|_| server.join(raw))?;
    if matches!(url.host_str(), Some("localhost") | Some("127.0.0.1")) {
        url.set_scheme(server.scheme())
            .map_err(|_| anyhow!("invalid server scheme"))?;
        url.set_host(server.host_str())
            .map_err(|_| anyhow!("invalid server host"))?;
        url.set_port(server.port())
            .map_err(|_| anyhow!("invalid server port"))?;
    }
    Ok(url)
}

async fn verify_download(client: &Client, server: &str, uri: &str, expected: &[u8]) -> Result<()> {
    let response = client
        .get_file_url(&FileURLService {
            uris: vec![uri.to_string()],
            download: Some(true),
            redirect: Some(false),
            no_cache: Some(true),
            ..Default::default()
        })
        .await?;
    let raw = response
        .urls
        .first()
        .context("no download URL returned")?
        .url
        .clone();
    let url = retarget(&raw, server)?;

    let response = reqwest::Client::new()
        .get(url.clone())
        .send()
        .await
        .context("download request failed")?;
    if !response.status().is_success() {
        bail!("download failed with {} for {url}", response.status());
    }
    let bytes = response.bytes().await?;

    let expected_hash: [u8; 32] = Sha256::digest(expected).into();
    let actual_hash: [u8; 32] = Sha256::digest(&bytes).into();
    if actual_hash != expected_hash {
        bail!(
            "downloaded content hash mismatch ({} bytes); resume produced different data",
            bytes.len()
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let server = std::env::var("CLOUDREVE_SERVER")?;
    let email = std::env::var("CLOUDREVE_EMAIL")?;
    let password = std::env::var("CLOUDREVE_PASSWORD")?;
    let root = std::env::var("CLOUDREVE_URI").unwrap_or_else(|_| "cloudreve://my/".into());

    let client = Arc::new(Client::new(ClientConfig::new(&server)));
    let LoginResponse { token, .. } = client.login(&email, &password).await?;
    client.set_tokens_with_expiry(&token).await?;

    let phase = std::env::var("CLOUDREVE_RESUME_PHASE").unwrap_or_else(|_| "all".into());
    if !matches!(phase.as_str(), "first" | "second" | "all") {
        bail!("CLOUDREVE_RESUME_PHASE must be first, second, or all");
    }
    let split = phase != "all";

    let size_mb = std::env::var("CLOUDREVE_RESUME_MB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);
    let fail_delay_ms = std::env::var("CLOUDREVE_RESUME_FAIL_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(3000);
    let data = Arc::new(vec![0x5a; size_mb * 1024 * 1024]);

    let session_dir = std::env::var("CLOUDREVE_RESUME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("cloudreve-resume-e2e"));
    let name =
        std::env::var("CLOUDREVE_RESUME_NAME").unwrap_or_else(|_| "resume-e2e.bin".to_string());
    let remote_uri = format!("{}/{}", root.trim_end_matches('/'), name);

    // 0 = first attempt (inject failure after chunk 0), 1 = resume attempt.
    let phase_state = Arc::new(AtomicU8::new(if phase == "second" { 1 } else { 0 }));
    let opened: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let source = UploadSource::from_reader(
        data.len() as u64,
        format!("resume-e2e:{}", data.len()),
        "resume-e2e-reader",
        {
            let data = Arc::clone(&data);
            let phase_state = Arc::clone(&phase_state);
            let opened = Arc::clone(&opened);
            Arc::new(move |offset, len| {
                let data = Arc::clone(&data);
                let phase_state = Arc::clone(&phase_state);
                let opened = Arc::clone(&opened);
                Box::pin(async move {
                    opened.lock().unwrap().push(offset);
                    match phase_state.load(Ordering::Relaxed) {
                        0 if offset > 0 => {
                            // Give chunk 0 time to finish and persist before
                            // failing the next chunk, even with concurrency.
                            tokio::time::sleep(Duration::from_millis(fail_delay_ms)).await;
                            return Err(anyhow!("injected chunk failure at offset {offset}"));
                        }
                        1 if offset == 0 => {
                            return Err(anyhow!("resume re-read already completed chunk 0"));
                        }
                        _ => {}
                    }
                    reader(data, offset, len)
                })
            })
        },
    );

    let params = UploadParams {
        source,
        remote_uri: remote_uri.clone(),
        mime_type: Some("application/octet-stream".into()),
        last_modified: None,
        // The ephemeral container starts empty; a repeat run should not turn
        // this into a new version entity.
        overwrite: false,
        policy_id: String::new(),
        metadata: None,
        previous_version: String::new(),
    };

    if phase != "second" {
        let _ = std::fs::remove_dir_all(&session_dir);
        std::fs::create_dir_all(&session_dir)?;
    }

    let config = UploaderConfig {
        max_retries: 0,
        ..Default::default()
    }
    .with_session_dir(&session_dir);
    // In `all` mode the same Uploader is reused after the injected chunk
    // failure, reproducing the "cancelled root token" regression.
    let uploader = Uploader::new(Arc::clone(&client), config);

    if phase != "second" {
        let result = uploader.upload(params.clone(), NoopProgress).await;
        let Err(error) = result else {
            bail!("first attempt unexpectedly succeeded; increase CLOUDREVE_RESUME_MB");
        };
        let snapshots = snapshot_count(&session_dir)?;
        if snapshots != 1 {
            bail!("expected one persisted session snapshot, found {snapshots}");
        }
        println!(
            "first attempt failed as intended ({error:#}); snapshot retained in {}",
            session_dir.display()
        );

        if split {
            return Ok(());
        }

        opened.lock().unwrap().clear();
        phase_state.store(1, Ordering::Relaxed);
    } else {
        let snapshots = snapshot_count(&session_dir)?;
        if snapshots != 1 {
            bail!("second phase expected the first phase's snapshot, found {snapshots}");
        }
    }

    uploader.upload(params, NoopProgress).await?;

    let snapshots = snapshot_count(&session_dir)?;
    if snapshots != 0 {
        bail!("completed upload left {snapshots} snapshots behind");
    }

    {
        let opened = opened.lock().unwrap();
        if opened.contains(&0) {
            bail!("resume re-opened chunk 0; completed chunks were not skipped");
        }
        println!("resume skipped chunk 0; re-opened offsets: {:?}", *opened);
    }

    verify_download(&client, &server, &remote_uri, &data).await?;
    println!("downloaded content hash matches; resume upload passed");

    let _ = std::fs::remove_dir_all(&session_dir);
    Ok(())
}
