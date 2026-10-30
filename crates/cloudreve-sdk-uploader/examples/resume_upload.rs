//! Durable chunk-resume smoke test.
//!
//! First attempt deliberately fails after the first chunk; the second attempt
//! must load the snapshot, skip completed chunks, finish the upload, and remove
//! the snapshot. Run through `scripts/e2e_podman.py`.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, anyhow};
use cloudreve_sdk_api::api::UserApi;
use cloudreve_sdk_api::models::user::LoginResponse;
use cloudreve_sdk_api::{Client, ClientConfig};
use cloudreve_sdk_uploader::{
    ProgressCallback, ProgressUpdate, UploadParams, UploadSource, Uploader, UploaderConfig,
};
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

#[tokio::main]
async fn main() -> Result<()> {
    let server = std::env::var("CLOUDREVE_SERVER")?;
    let email = std::env::var("CLOUDREVE_EMAIL")?;
    let password = std::env::var("CLOUDREVE_PASSWORD")?;
    let root = std::env::var("CLOUDREVE_URI").unwrap_or_else(|_| "cloudreve://my/".into());

    let client = Arc::new(Client::new(ClientConfig::new(&server)));
    let LoginResponse { token, .. } = client.login(&email, &password).await?;
    client.set_tokens_with_expiry(&token).await?;

    let size_mb = std::env::var("CLOUDREVE_RESUME_MB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);
    let data = Arc::new(vec![0x5a; size_mb * 1024 * 1024]);
    let fail_after_first = Arc::new(AtomicBool::new(true));
    let source = UploadSource::from_reader(
        data.len() as u64,
        format!("resume-test:{}:{}", std::process::id(), data.len()),
        "resume-test-reader",
        {
            let data = Arc::clone(&data);
            let fail = Arc::clone(&fail_after_first);
            Arc::new(move |offset, len| {
                let data = Arc::clone(&data);
                let fail = Arc::clone(&fail);
                Box::pin(async move {
                    if fail.load(Ordering::SeqCst) && offset > 0 {
                        return Err(anyhow!("injected chunk failure"));
                    }
                    reader(data, offset, len)
                })
            })
        },
    );

    let session_dir =
        std::env::temp_dir().join(format!("cloudreve-resume-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&session_dir);
    let config = UploaderConfig::default().with_session_dir(&session_dir);
    let name = format!("resume-{}.bin", std::process::id());
    let params = UploadParams {
        source,
        remote_uri: format!("{}/{}", root.trim_end_matches('/'), name),
        mime_type: Some("application/octet-stream".into()),
        last_modified: None,
        overwrite: false,
        policy_id: String::new(),
        metadata: None,
        previous_version: String::new(),
    };

    let first = Uploader::new(Arc::clone(&client), config.clone())
        .upload(params.clone(), NoopProgress)
        .await;
    if first.is_ok() {
        return Err(anyhow!("first attempt unexpectedly succeeded"));
    }
    let snapshots = std::fs::read_dir(&session_dir)?.count();
    if snapshots != 1 {
        return Err(anyhow!(
            "expected one persisted session snapshot, found {snapshots}"
        ));
    }
    println!("first attempt failed as intended; snapshot retained");

    fail_after_first.store(false, Ordering::SeqCst);
    Uploader::new(Arc::clone(&client), config)
        .upload(params, NoopProgress)
        .await?;
    let snapshots = std::fs::read_dir(&session_dir)?.count();
    if snapshots != 0 {
        return Err(anyhow!(
            "completed upload left {snapshots} snapshots behind"
        ));
    }
    let _ = std::fs::remove_dir_all(&session_dir);
    println!("resume upload passed");
    Ok(())
}
