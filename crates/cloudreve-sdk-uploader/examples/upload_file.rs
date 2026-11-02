//! Minimal upload: create a session, consume the event stream, print progress,
//! then download the result back and compare its hash.
//!
//! Usage: `upload_file <LOCAL_PATH> <REMOTE_FILE_URI>`
//!
//! Needs `CLOUDREVE_SERVER`, `CLOUDREVE_EMAIL` and `CLOUDREVE_PASSWORD`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use anyhow::{Result, bail};
use cloudreve_sdk_uploader::{Tracker, UploadParams, UploadSource, Uploader, UploaderConfig};

#[path = "example_utils/mod.rs"]
mod example_utils;

use example_utils::download::{digest_file, verify};
use example_utils::drain::{Outcome, drain};
use example_utils::env::Settings;
use example_utils::login::login;

const USAGE: &str = "usage: upload_file <LOCAL_PATH> <REMOTE_FILE_URI>";

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (Some(local_path), Some(remote_uri)) = (args.next(), args.next()) else {
        bail!(USAGE);
    };
    if args.next().is_some() {
        bail!("too many arguments\n{USAGE}");
    }

    let settings = Settings::from_env()?;
    let (client, _) = login(&settings, "cloudreve-sdk-uploader-example").await?;
    let client = Arc::new(client);

    let local_path = PathBuf::from(local_path);
    let metadata = tokio::fs::metadata(&local_path).await?;
    if !metadata.is_file() {
        bail!("{} is not a file", local_path.display());
    }
    let last_modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64);

    let uploader = Uploader::new(Arc::clone(&client), UploaderConfig::default());
    let stream = uploader
        .upload(UploadParams {
            source: UploadSource::from_path(&local_path)?,
            remote_uri: remote_uri.clone(),
            mime_type: None,
            last_modified,
            overwrite: false,
            previous_version: String::new(),
            policy_id: String::new(),
            metadata: None,
        })
        .await?;

    // Progress is a fold over the events, not a callback: the shared drain loop
    // hands every finished chunk to this closure, which only formats it.
    let mut tracker = Tracker::new(&stream.session, &[]);
    let outcome = drain(stream, |event| {
        if let Some(update) = tracker.observe(event) {
            eprintln!(
                "{:.1}% ({}/{}) at {} bytes/s, chunks {}/{}",
                update.progress * 100.0,
                update.uploaded,
                update.total_size,
                update.speed_bytes_per_sec,
                update.completed_chunks,
                update.total_chunks,
            );
        }
        Ok(())
    })
    .await?;

    match outcome {
        Outcome::Completed => {}
        Outcome::Failed(error) => bail!("upload failed: {error}"),
    }

    println!("Upload completed; checking the stored bytes");
    let expected = digest_file(&local_path).await?;
    verify(&client, &settings.server, &remote_uri, expected).await?;
    println!("downloaded content hash matches");

    Ok(())
}
