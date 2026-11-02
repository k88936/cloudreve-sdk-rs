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
use cloudreve_sdk_api::models::explorer::UploadCredential;
use cloudreve_sdk_uploader::{
    ChunkProgress, Tracker, UploadEvent, UploadParams, UploadSession, UploadSource, UploadStream,
    Uploader, UploaderConfig,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;

#[path = "example_utils/mod.rs"]
mod example_utils;

use example_utils::download::{digest, verify};
use example_utils::drain::{Outcome, drain};
use example_utils::env::Settings;
use example_utils::login::login;

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

/// Consume a stream, journaling every finished chunk, and report the outcome.
///
/// One loop feeds both the journal and the progress tracker, because the tracker
/// borrows the event instead of taking it.
async fn drain_journaled(
    stream: UploadStream,
    journal: &Journal,
    already_done: &[ChunkProgress],
) -> Result<Outcome> {
    let mut tracker = Tracker::new(&stream.session, already_done);

    let outcome = drain(stream, |event| {
        if let Some(update) = tracker.observe(event) {
            eprintln!(
                "  {:>5.1}%  {}/{} chunks",
                update.progress * 100.0,
                update.completed_chunks,
                update.total_chunks
            );
        }

        if let UploadEvent::ChunkDone(record) = event {
            journal.append(record)?;
        }
        Ok(())
    })
    .await?;

    if let Outcome::Completed = outcome {
        journal.finish()?;
    }

    Ok(outcome)
}

// =========================================================================
// Settings and helpers
// =========================================================================

/// Resume-only knobs; the connection settings come from [`Settings`].
struct ResumeSettings {
    name: String,
    dir: PathBuf,
    size_mb: usize,
    fail_delay: Duration,
}

impl ResumeSettings {
    fn from_env() -> Result<Self> {
        Ok(Self {
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
}

fn remote_uri(root: &str, name: &str) -> String {
    format!("{}/{}", root.trim_end_matches('/'), name)
}

fn params(source: &UploadSource, remote_uri: &str) -> UploadParams {
    UploadParams {
        source: source.clone(),
        remote_uri: remote_uri.to_owned(),
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

// =========================================================================
// Main
// =========================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    let resume = ResumeSettings::from_env()?;
    let (client, _) = login(&settings, "cloudreve-sdk-uploader-example").await?;
    let client = Arc::new(client);
    let remote_uri = remote_uri(&settings.root, &resume.name);

    let data = Arc::new(vec![0x5a; resume.size_mb * 1024 * 1024]);
    let phase = Arc::new(AtomicU8::new(0));
    let opened: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let source = sabotaged_source(
        Arc::clone(&data),
        Arc::clone(&phase),
        Arc::clone(&opened),
        resume.fail_delay,
    );

    std::fs::create_dir_all(&resume.dir)?;
    let uploader = Uploader::new(
        Arc::clone(&client),
        UploaderConfig {
            max_retries: 0,
            ..Default::default()
        },
    );

    // --- 1. an upload that gets interrupted ------------------------------
    let stream = uploader.upload(params(&source, &remote_uri)).await?;
    let journal = Journal::new(&resume.dir, stream.session.session_id());
    journal.create(&stream.session)?;

    let Outcome::Failed(error) = drain_journaled(stream, &journal, &[]).await? else {
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
    match drain_journaled(stream, &journal, &records).await? {
        Outcome::Completed => {}
        Outcome::Failed(error) => bail!("resume failed: {error}"),
    }

    // --- 4. the journal is gone, the recorded chunks were skipped --------
    if journal.path().exists() {
        bail!("{} survived a completed upload", journal.path().display());
    }
    if std::fs::read_dir(&resume.dir)?.next().is_some() {
        bail!(
            "{} is not empty after a completed upload",
            resume.dir.display()
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
    verify(&client, &settings.server, &remote_uri, digest(&data)).await?;
    println!("downloaded content hash matches; interrupted upload resumed and completed");

    Ok(())
}
