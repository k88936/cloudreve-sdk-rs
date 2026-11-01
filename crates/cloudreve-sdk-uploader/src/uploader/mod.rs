//! File uploader implementation for CLI clients.
//!
//! This module provides chunked file upload functionality with support for
//! multiple storage providers, encryption and retries.
//!
//! An upload is an [`UploadStream`]: `upload` creates the session and returns it
//! together with the events the upload produces. Consuming the events is what
//! drives the upload, so a durable log, a progress bar and a resume decision can
//! all be folds over the same stream.

mod chunk;
mod chunk_progress;
#[cfg(target_os = "android")]
mod content_uri;
mod encrypt;
mod error;
#[cfg(feature = "progress")]
mod progress;
mod providers;
mod session;
mod source;
mod stream;
mod transfer;

use anyhow::{Context, Result};
use chunk::ChunkInfo;
use encrypt::EncryptionConfig;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use transfer::{ChunkFuture, ChunkStarter, ChunkTransfer, Finalizer};
use tracing::{debug, info};

pub use chunk_progress::ChunkProgress;
pub use error::{UploadError, UploadResult};
#[cfg(feature = "progress")]
pub use progress::{ProgressUpdate, Tracker};
pub use session::UploadSession;
pub use source::{RangeOpener, SourceReader, UploadSource};
pub use stream::{UploadEvent, UploadStream};

use cloudreve_api::{Client as CrClient, api::ExplorerApi};
use reqwest::Client as HttpClient;

/// Configuration for the uploader
#[derive(Debug, Clone)]
pub struct UploaderConfig {
    /// Maximum number of retry attempts per chunk
    pub max_retries: u32,
    /// Base delay between retries (exponential backoff)
    pub retry_base_delay: Duration,
    /// Maximum delay between retries
    pub retry_max_delay: Duration,
    /// Request timeout for chunk uploads
    pub request_timeout: Duration,
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_base_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
        }
    }
}

/// Parameters for initiating an upload
#[derive(Debug, Clone)]
pub struct UploadParams {
    /// File/reader source. Use `UploadSource::from_path`,
    /// `from_content_uri` (Android), or `from_reader` for custom readers.
    pub source: UploadSource,
    /// Remote URI (cloudreve path)
    pub remote_uri: String,
    /// File MIME type (optional)
    pub mime_type: Option<String>,
    /// Last modified timestamp (optional)
    pub last_modified: Option<i64>,
    /// Whether to overwrite existing file (creates new version)
    pub overwrite: bool,
    /// Previous version ETag (optional)
    pub previous_version: String,
    /// Storage policy id; empty lets Cloudreve pick the default policy.
    pub policy_id: String,
    /// Metadata written with the upload session (optional).
    pub metadata: Option<HashMap<String, String>>,
}

/// Main uploader struct
pub struct Uploader {
    /// Cloudreve API client for session management
    cr_client: Arc<CrClient>,
    /// HTTP client for direct uploads to storage providers
    http_client: HttpClient,
    /// Uploader configuration
    config: UploaderConfig,
}

impl Uploader {
    /// Create a new uploader instance
    pub fn new(cr_client: Arc<CrClient>, config: UploaderConfig) -> Self {
        let http_client = HttpClient::builder()
            .connect_timeout(config.request_timeout)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            cr_client,
            http_client,
            config,
        }
    }

    /// Create an upload session and return it with its event stream.
    ///
    /// The session is created before this returns; the chunks are not. Nothing
    /// is uploaded until the returned stream is polled, and dropping the stream
    /// cancels the upload.
    pub async fn upload(&self, params: UploadParams) -> Result<UploadStream> {
        info!(
            target: "uploader",
            source = %params.source.display(),
            remote_uri = %params.remote_uri,
            file_size = params.source.size(),
            "Starting upload"
        );

        let session = self.create_session(&params).await?;
        self.start_transfer(session, params.source, Vec::new(), false)
    }

    /// Continue a session with the chunks that already finished.
    ///
    /// `completed` are the records the caller kept, usually read back from a log
    /// it wrote from a previous [`UploadStream`]. Records are deduplicated (the
    /// last record per chunk wins) and records outside the session are ignored,
    /// so a damaged log resumes what it can instead of failing outright.
    ///
    /// Nothing compares `source` with the file the records came from: the caller
    /// must pass the bytes that produced them, or the server will assemble a file
    /// out of mismatched chunks.
    ///
    /// When the server no longer knows the session, the first chunk of the
    /// attempt is rejected and the stream reports
    /// [`UploadError::SessionExpired`]; the caller owns the log and should delete
    /// it.
    pub async fn try_resume(
        &self,
        session: UploadSession,
        source: UploadSource,
        completed: impl IntoIterator<Item = ChunkProgress>,
    ) -> Result<UploadStream> {
        let completed: Vec<ChunkProgress> = completed.into_iter().collect();
        info!(
            target: "uploader",
            session_id = %session.session_id(),
            source = %source.display(),
            file_size = session.file_size,
            completed_chunks = completed.len(),
            total_chunks = session.num_chunks(),
            "Resuming upload"
        );

        self.start_transfer(session, source, completed, true)
    }

    /// Build the transfer behind [`UploadStream::events`].
    fn start_transfer(
        &self,
        session: UploadSession,
        source: UploadSource,
        completed: Vec<ChunkProgress>,
        resumed: bool,
    ) -> Result<UploadStream> {
        let encryption = session
            .encrypt_metadata
            .as_ref()
            .map(EncryptionConfig::from_metadata)
            .transpose()?;

        let policy_type = if session.is_relay() {
            providers::PolicyType::Local
        } else {
            session.policy_type()
        };

        // Shared by both callbacks: the starter uploads chunks, the finalizer
        // tells the provider the multipart upload is complete.
        let shared_session = Arc::new(session.clone());

        let starter: ChunkStarter = {
            let http_client = self.http_client.clone();
            let cr_client = Arc::clone(&self.cr_client);
            let config = self.config.clone();
            let session = Arc::clone(&shared_session);

            Box::new(move |chunk: ChunkInfo| {
                let http_client = http_client.clone();
                let cr_client = Arc::clone(&cr_client);
                let config = config.clone();
                let session = Arc::clone(&session);
                let source = source.clone();
                let encryption = encryption.clone();

                Box::pin(async move {
                    let index = chunk.index;
                    let result = chunk::upload_chunk_with_retry(
                        &http_client,
                        &cr_client,
                        policy_type,
                        &config,
                        &source,
                        &chunk,
                        encryption,
                        &session,
                    )
                    .await;
                    (index, result)
                }) as ChunkFuture
            })
        };

        let finalize: Finalizer = {
            let http_client = self.http_client.clone();
            let cr_client = Arc::clone(&self.cr_client);
            let session = Arc::clone(&shared_session);

            Box::new(move |completed: Vec<ChunkProgress>| {
                let http_client = http_client.clone();
                let cr_client = Arc::clone(&cr_client);
                let session = Arc::clone(&session);

                Box::pin(async move {
                    debug!(
                        target: "uploader",
                        session_id = %session.session_id(),
                        policy_type = ?session.policy_type(),
                        chunks = completed.len(),
                        "Completing upload"
                    );
                    providers::complete_upload(&http_client, &cr_client, &session, &completed)
                        .await
                })
            })
        };

        let events = ChunkTransfer::new(session.clone(), completed, resumed, starter, finalize);

        Ok(UploadStream {
            session,
            events: events.boxed(),
        })
    }

    /// Create a new upload session via Cloudreve API
    async fn create_session(&self, params: &UploadParams) -> Result<UploadSession> {
        use cloudreve_api::models::explorer::{EncryptionCipher, UploadSessionRequest};
        let request = UploadSessionRequest {
            uri: params.remote_uri.clone(),
            size: params.source.size() as i64,
            policy_id: params.policy_id.clone(),
            last_modified: params.last_modified,
            previous: (!params.previous_version.is_empty()).then(|| params.previous_version.clone()),
            entity_type: params.overwrite.then(|| "version".to_string()),
            mime_type: params.mime_type.clone(),
            metadata: params.metadata.clone(),
            encryption_supported: Some(vec![EncryptionCipher::Aes256Ctr]),
        };

        let credential = self
            .cr_client
            .create_upload_session(&request)
            .await
            .context("failed to create upload session")?;

        debug!(
            target: "uploader",
            session_id = %credential.session_id,
            chunk_size = credential.chunk_size,
            "Upload session created"
        );

        Ok(UploadSession::new(
            params.source.identifier().to_string(),
            params.remote_uri.clone(),
            params.source.size(),
            credential,
        ))
    }

    /// Delete the remote upload session (cancel on server side)
    #[allow(dead_code)]
    async fn delete_remote_session(&self, session: &UploadSession) -> UploadResult<()> {
        use cloudreve_api::models::explorer::DeleteUploadSessionService;

        let request = DeleteUploadSessionService {
            id: session.session_id().to_string(),
            uri: session.remote_uri.clone(),
        };

        self.cr_client
            .delete_upload_session(&request)
            .await
            .map_err(|e| UploadError::SessionDeletionFailed(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudreve_api::ClientConfig;

    fn params() -> UploadParams {
        UploadParams {
            source: UploadSource::from_bytes([1u8; 4].to_vec()),
            remote_uri: "cloudreve://my/unreachable.bin".into(),
            mime_type: None,
            last_modified: None,
            overwrite: false,
            policy_id: String::new(),
            metadata: None,
            previous_version: String::new(),
        }
    }

    #[tokio::test]
    async fn session_creation_failure_is_an_error_not_a_stream() {
        let uploader = Uploader::new(
            Arc::new(CrClient::new(ClientConfig::new("http://127.0.0.1:1"))),
            UploaderConfig::default(),
        );

        let error = uploader.upload(params()).await.unwrap_err();

        assert!(
            format!("{error:#}").contains("upload session"),
            "unexpected error: {error:#}"
        );
    }

    /// A session with `num_chunks` chunks of 5 bytes, reachable without a server.
    fn session(num_chunks: usize) -> UploadSession {
        use cloudreve_api::models::explorer::UploadCredential;

        UploadSession::new(
            "file:/a".into(),
            "cloudreve://my/a.bin".into(),
            num_chunks as u64 * 5,
            UploadCredential {
                session_id: "session-1".into(),
                expires: 4_000_000_000,
                chunk_size: 5,
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn try_resume_hands_back_a_lazy_stream() {
        let uploader = Uploader::new(
            Arc::new(CrClient::new(ClientConfig::new("http://127.0.0.1:1"))),
            UploaderConfig::default(),
        );
        let session = session(4);

        let stream = uploader
            .try_resume(
                session.clone(),
                UploadSource::from_bytes(vec![0u8; 20]),
                [ChunkProgress::new(0, 5, None)],
            )
            .await
            .expect("resuming must not touch the network");

        assert_eq!(stream.session.session_id(), session.session_id());
        assert_eq!(stream.session.num_chunks(), 4);
    }
}
