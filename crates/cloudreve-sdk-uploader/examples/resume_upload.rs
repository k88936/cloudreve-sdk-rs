//! Journaled resume: interrupt an upload, resume it from the journal, verify.
//!
//! One process, one run:
//!
//! 1. upload and append every finished chunk to `<dir>/<session_id>.jsonl`;
//! 2. abort on purpose after the first chunk (`max_retries = 0`);
//! 3. read the journal back into a session plus completion records;
//! 4. `try_resume`, which skips the chunks the journal recorded;
//! 5. verify the downloaded bytes and that the journal is gone.
//!
//! The journal here is *example code*: the library ships the event stream and
//! the serializable records, not a storage format.
//!
//! Env: `CLOUDREVE_SERVER`, `CLOUDREVE_EMAIL`, `CLOUDREVE_PASSWORD`, and
//! optionally `CLOUDREVE_URI` (default `cloudreve://my/`),
//! `CLOUDREVE_RESUME_MB` (default 64), `CLOUDREVE_RESUME_DIR`,
//! `CLOUDREVE_RESUME_NAME` and `CLOUDREVE_RESUME_FAIL_DELAY_MS` (default 3000).

use std::env;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use cloudreve_sdk_api::api::{ExplorerApi, UserApi};
use cloudreve_sdk_api::models::explorer::{FileURLService, UploadCredential};
use cloudreve_sdk_api::models::user::LoginResponse;
use cloudreve_sdk_api::{Client, ClientConfig};
use cloudreve_sdk_uploader::{
    ChunkProgress, Tracker, UploadError, UploadEvent, UploadParams, UploadSession, UploadSource,
    UploadStream, Uploader, UploaderConfig,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncRead;

// =========================================================================
// The journal
// =========================================================================

/// Line 1 of the journal: the *cold* half of the session.
///
/// Chunk geometry, policy, relay flag and expiry are recomputed by
/// `UploadSession::new`, so they are not stored. There is no version field: a
/// journal that fails to parse is simply discarded and the upload starts over.
#[derive(Debug, Serialize, Deserialize)]
struct StoredSession {
    remote_uri: String,
    source_id: String,
    file_size: u64,
    credential: UploadCredential,
}

/// Append-only journal of one upload session.
///
/// Line 1 is the session, every following line is one [`ChunkProgress`]. A
/// retried chunk appears twice; the reader keeps the last record per index.
struct Journal {
    path: PathBuf,
}

impl Journal {
    fn new(dir: &Path, session_id: &str) -> Self {
        Self {
            path: dir.join(format!("{session_id}.jsonl")),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// Write the session. Called once, before the first chunk is polled.
    fn create(&self, session: &UploadSession) -> Result<()> {
        let stored = StoredSession {
            remote_uri: session.remote_uri.clone(),
            source_id: session.source_id.clone(),
            file_size: session.file_size,
            credential: session.credential().clone(),
        };

        let mut file = File::create(&self.path)
            .with_context(|| format!("creating {}", self.path.display()))?;
        writeln!(file, "{}", serde_json::to_string(&stored)?)?;
        file.sync_all()?;
        Ok(())
    }

    /// Append one finished chunk.
    fn append(&self, record: &ChunkProgress) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .with_context(|| format!("opening {}", self.path.display()))?;
        writeln!(file, "{}", serde_json::to_string(record)?)?;
        Ok(())
    }

    /// Read the session and every completion record back.
    fn load(&self) -> Result<(UploadSession, Vec<ChunkProgress>)> {
        let reader = BufReader::new(
            File::open(&self.path).with_context(|| format!("opening {}", self.path.display()))?,
        );
        let mut lines = reader.lines();

        let first = lines
            .next()
            .with_context(|| format!("{} is empty", self.path.display()))??;
        let stored: StoredSession = serde_json::from_str(&first)?;
        let session = UploadSession::new(
            stored.source_id,
            stored.remote_uri,
            stored.file_size,
            stored.credential,
        );

        let mut records = Vec::new();
        for line in lines {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            records.push(serde_json::from_str(&line)?);
        }

        Ok((session, records))
    }

    /// A finished upload needs no journal.
    fn finish(&self) -> Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

// =========================================================================
// Driving a stream
// =========================================================================

/// How an upload ended.
enum Outcome {
    Completed,
    Failed(UploadError),
}

/// Consume a stream, journaling every finished chunk, and report the outcome.
///
/// This is the whole consumer: one loop feeds the journal and the progress
/// tracker, because the tracker borrows the event instead of taking it.
async fn drain(
    stream: UploadStream,
    journal: &Journal,
    already_done: &[ChunkProgress],
) -> Result<Outcome> {
    let mut tracker = Tracker::new(&stream.session, already_done);
    let mut events = stream.events;

    while let Some(event) = events.next().await {
        if let Some(update) = tracker.observe(&event) {
            eprintln!(
                "  {:>5.1}%  {}/{} chunks",
                update.progress * 100.0,
                update.completed_chunks,
                update.total_chunks
            );
        }

        match event {
            UploadEvent::ChunkDone(record) => journal.append(&record)?,
            UploadEvent::Completed => {
                journal.finish()?;
                return Ok(Outcome::Completed);
            }
            UploadEvent::Failed(error) => return Ok(Outcome::Failed(error)),
        }
    }

    bail!("upload stream ended without a terminal event")
}

// =========================================================================
// Settings and helpers
// =========================================================================

struct Settings {
    server: String,
    email: String,
    password: String,
    root: String,
    name: String,
    dir: PathBuf,
    size_mb: usize,
    fail_delay: Duration,
}

impl Settings {
    fn from_env() -> Result<Self> {
        Ok(Self {
            server: env::var("CLOUDREVE_SERVER")?,
            email: env::var("CLOUDREVE_EMAIL")?,
            password: env::var("CLOUDREVE_PASSWORD")?,
            root: env::var("CLOUDREVE_URI").unwrap_or_else(|_| "cloudreve://my/".into()),
            name: env::var("CLOUDREVE_RESUME_NAME")
                .unwrap_or_else(|_| "resume-e2e.bin".to_string()),
            dir: env::var("CLOUDREVE_RESUME_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| env::temp_dir().join("cloudreve-resume-e2e")),
            size_mb: env::var("CLOUDREVE_RESUME_MB")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(64),
            fail_delay: Duration::from_millis(
                env::var("CLOUDREVE_RESUME_FAIL_DELAY_MS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(3000),
            ),
        })
    }

    fn remote_uri(&self) -> String {
        format!("{}/{}", self.root.trim_end_matches('/'), self.name)
    }
}

async fn login(settings: &Settings) -> Result<Arc<Client>> {
    let client = Arc::new(Client::new(ClientConfig::new(&settings.server)));
    let LoginResponse { token, .. } = client.login(&settings.email, &settings.password).await?;
    client.set_tokens_with_expiry(&token).await?;
    Ok(client)
}

fn params(source: &UploadSource, settings: &Settings) -> UploadParams {
    UploadParams {
        source: source.clone(),
        remote_uri: settings.remote_uri(),
        mime_type: Some("application/octet-stream".into()),
        last_modified: None,
        // The container starts empty; a repeat run must not create a version.
        overwrite: false,
        previous_version: String::new(),
        policy_id: String::new(),
        metadata: None,
    }
}

/// A source over `data` whose reader sabotages the upload on purpose.
///
/// Phase 0 is the first attempt: any chunk but the first fails after a delay,
/// which gives chunk 0 time to finish and be journaled. Phase 1 is the resume:
/// re-reading chunk 0 is a failure, so the test proves the recorded chunk was
/// skipped instead of being uploaded again.
fn sabotaged_source(
    data: Arc<Vec<u8>>,
    phase: Arc<AtomicU8>,
    opened: Arc<Mutex<Vec<u64>>>,
    fail_delay: Duration,
) -> UploadSource {
    let size = data.len() as u64;

    UploadSource::from_reader(
        size,
        format!("resume-e2e:{size}"),
        "resume-e2e-reader",
        Arc::new(move |offset, len| {
            let data = Arc::clone(&data);
            let phase = Arc::clone(&phase);
            let opened = Arc::clone(&opened);

            Box::pin(async move {
                opened.lock().unwrap().push(offset);

                match phase.load(Ordering::Relaxed) {
                    0 if offset > 0 => {
                        tokio::time::sleep(fail_delay).await;
                        return Err(anyhow!("injected chunk failure at offset {offset}"));
                    }
                    1 if offset == 0 => {
                        return Err(anyhow!("resume re-read an already finished chunk"));
                    }
                    _ => {}
                }

                let start = offset as usize;
                let end = (offset + len) as usize;
                Ok(Box::new(Cursor::new(data[start..end].to_vec()))
                    as Box<dyn AsyncRead + Send + Sync + Unpin>)
            })
        }),
    )
}

/// The download URL is built from Cloudreve's configured `SiteURL`, which is
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

// =========================================================================
// Main
// =========================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    let client = login(&settings).await?;

    let data = Arc::new(vec![0x5a; settings.size_mb * 1024 * 1024]);
    let phase = Arc::new(AtomicU8::new(0));
    let opened: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let source = sabotaged_source(
        Arc::clone(&data),
        Arc::clone(&phase),
        Arc::clone(&opened),
        settings.fail_delay,
    );

    std::fs::create_dir_all(&settings.dir)?;
    let uploader = Uploader::new(
        Arc::clone(&client),
        UploaderConfig {
            max_retries: 0,
            ..Default::default()
        },
    );

    // --- 1. an upload that gets interrupted ------------------------------
    let stream = uploader.upload(params(&source, &settings)).await?;
    let journal = Journal::new(&settings.dir, stream.session.session_id());
    journal.create(&stream.session)?;

    let Outcome::Failed(error) = drain(stream, &journal, &[]).await? else {
        bail!("the first attempt unexpectedly succeeded; increase CLOUDREVE_RESUME_MB");
    };
    println!("interrupted as intended: {error}");

    // --- 2. what the journal knows --------------------------------------
    let (session, records) = journal.load()?;
    if records.is_empty() {
        bail!("the journal kept no chunk records; increase CLOUDREVE_RESUME_FAIL_DELAY_MS");
    }
    println!(
        "journal has {} of {} chunks for session {}",
        records.len(),
        session.num_chunks(),
        session.session_id()
    );

    // --- 3. resume from the records --------------------------------------
    phase.store(1, Ordering::Relaxed);
    opened.lock().unwrap().clear();

    let stream = uploader
        .try_resume(session, source.clone(), records.clone())
        .await?;
    match drain(stream, &journal, &records).await? {
        Outcome::Completed => {}
        Outcome::Failed(error) => bail!("resume failed: {error}"),
    }

    // --- 4. the journal is gone, the recorded chunks were skipped --------
    if journal.path().exists() {
        bail!("{} survived a completed upload", journal.path().display());
    }
    if std::fs::read_dir(&settings.dir)?.next().is_some() {
        bail!(
            "{} is not empty after a completed upload",
            settings.dir.display()
        );
    }
    {
        let opened = opened.lock().unwrap();
        if opened.contains(&0) {
            bail!("resume re-opened chunk 0; recorded chunks were not skipped");
        }
        println!("resume skipped the recorded chunks; re-opened offsets: {opened:?}");
    }

    // --- 5. the uploaded bytes are the source bytes ----------------------
    verify_download(&client, &settings.server, &settings.remote_uri(), &data).await?;
    println!("downloaded content hash matches; interrupted upload resumed and completed");

    Ok(())
}
