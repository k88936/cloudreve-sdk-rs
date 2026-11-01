//! Minimal upload: create a session, consume the event stream, print progress.
//!
//! Usage: `upload_file <LOCAL_PATH> <REMOTE_FILE_URI>`
//!
//! Needs `CLOUDREVE_SERVER`, `CLOUDREVE_EMAIL` and `CLOUDREVE_PASSWORD`.

use cloudreve_sdk_api::api::UserApi;
use cloudreve_sdk_api::{Client, ClientConfig};
use cloudreve_sdk_uploader::{
    Tracker, UploadEvent, UploadParams, UploadSource, Uploader, UploaderConfig,
};
use futures::StreamExt;
use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let local_path = args.next().map(PathBuf::from).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: upload_file <LOCAL_PATH> <REMOTE_FILE_URI>",
        )
    })?;
    let remote_uri = args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: upload_file <LOCAL_PATH> <REMOTE_FILE_URI>",
        )
    })?;

    if args.next().is_some() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "too many arguments").into());
    }

    let server = env::var("CLOUDREVE_SERVER")?;
    let email = env::var("CLOUDREVE_EMAIL")?;
    let password = env::var("CLOUDREVE_PASSWORD")?;

    let client = Arc::new(Client::new(
        ClientConfig::new(server)
            .with_timeout(60)
            .with_user_agent("cloudreve-sdk-uploader-example"),
    ));

    let login = client.login(&email, &password).await?;
    client.set_tokens_with_expiry(&login.token).await?;

    let metadata = tokio::fs::metadata(&local_path).await?;
    if !metadata.is_file() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "path is not a file").into());
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
            remote_uri,
            mime_type: None,
            last_modified,
            overwrite: false,
            previous_version: String::new(),
            policy_id: String::new(),
            metadata: None,
        })
        .await?;

    // Progress is a fold over the events, not a callback: one loop feeds the
    // tracker and handles the outcome.
    let mut tracker = Tracker::new(&stream.session, &[]);
    let mut events = stream.events;

    while let Some(event) = events.next().await {
        if let Some(update) = tracker.observe(&event) {
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

        match event {
            UploadEvent::Completed => {
                println!("Upload completed");
                return Ok(());
            }
            UploadEvent::Failed(error) => return Err(error.into()),
            UploadEvent::ChunkDone(_) => {}
        }
    }

    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "upload stream ended without a terminal event",
    )
    .into())
}
