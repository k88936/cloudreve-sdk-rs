use cloudreve_sdk_api::api::UserApi;
use cloudreve_sdk_api::{Client, ClientConfig};
use cloudreve_sdk_uploader::{
    ProgressCallback, ProgressUpdate, UploadParams, Uploader, UploaderConfig,
};
use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

struct ConsoleProgress;

impl ProgressCallback for ConsoleProgress {
    fn on_progress(&self, update: ProgressUpdate) {
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
}

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
    uploader
        .upload(
            UploadParams {
                local_path,
                remote_uri,
                file_size: metadata.len(),
                mime_type: None,
                last_modified,
                overwrite: false,
                previous_version: String::new(),
            },
            ConsoleProgress,
        )
        .await?;

    println!("Upload completed");
    Ok(())
}
