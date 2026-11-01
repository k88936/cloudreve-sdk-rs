//! File uploader implementation for CLI clients.
//!
//! This module provides chunked file upload functionality with support for
//! multiple storage providers, encryption, retries, cancellation, and progress
//! tracking.

mod chunk;
mod chunk_progress;
#[cfg(target_os = "android")]
mod content_uri;
mod encrypt;
mod error;
mod progress;
#[allow(dead_code)]
mod providers;
#[allow(dead_code)]
mod session;
mod source;

use anyhow::{Context, Result};
pub use chunk_progress::ChunkProgress;
use chunk_progress::CompletionSet;
use chunk::ChunkUploader;
pub use error::{UploadError, UploadResult};
pub use progress::{ProgressCallback, ProgressUpdate};
pub use session::UploadSession;
pub use source::{RangeOpener, SourceReader, UploadSource};

use cloudreve_api::{Client as CrClient, api::ExplorerApi};
use reqwest::Client as HttpClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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
    /// Cancellation token for stopping uploads
    cancel_token: CancellationToken,
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
            cancel_token: CancellationToken::new(),
        }
    }

    /// Create uploader with a custom cancellation token
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = token;
        self
    }

    /// Upload a file with progress reporting
    ///
    /// This method handles:
    /// - Creating an upload session
    /// - Splitting the file into chunks
    /// - Uploading chunks with retries
    /// - Reporting progress
    /// - Completing the upload
    pub async fn upload<P: ProgressCallback + 'static>(
        &self,
        params: UploadParams,
        progress: P,
    ) -> Result<()> {
        info!(
            target: "uploader",
            source = %params.source.display(),
            remote_uri = %params.remote_uri,
            file_size = params.source.size(),
            "Starting upload"
        );

        // Each upload gets its own cancellation scope. A chunk failure cancels
        // this child only; reusing the same `Uploader` must not inherit a
        // cancelled root token from a previous attempt.
        let upload_token = self.cancel_token.child_token();
        if upload_token.is_cancelled() {
            return Err(UploadError::Cancelled.into());
        }

        let session = self.create_session(&params).await?;
        let chunk_uploader = self.create_chunk_uploader(&session)?;

        let progress = Arc::new(progress);
        let completed = chunk_uploader
            .upload_all(&params.source, &session, progress, &upload_token)
            .await;

        match completed {
            Ok(completed) => {
                self.complete_upload(&session, &completed).await?;
                info!(
                    target: "uploader",
                    source = %params.source.display(),
                    "Upload completed successfully"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    target: "uploader",
                    source = %params.source.display(),
                    error = %e,
                    "Upload failed"
                );
                if let Err(e) = self.delete_remote_session(&session).await {
                    warn!(
                        target: "uploader",
                        source = %params.source.display(),
                        error = %e,
                        "Failed to delete remote upload session"
                    );
                }
                Err(e)
            }
        }
    }

    /// Cancel the current upload
    #[allow(dead_code)]
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// Check if upload is cancelled
    #[allow(dead_code)]
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
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

    /// Create appropriate chunk uploader based on policy type
    fn create_chunk_uploader(&self, session: &UploadSession) -> UploadResult<ChunkUploader> {
        let policy_type = if session.is_relay() {
            providers::PolicyType::Local
        } else {
            session.policy_type()
        };
        let uploader = ChunkUploader::new(
            self.http_client.clone(),
            self.cr_client.clone(),
            policy_type,
            self.config.clone(),
        );
        Ok(uploader)
    }

    /// Complete the upload (provider-specific finalization)
    async fn complete_upload(
        &self,
        session: &UploadSession,
        completed: &CompletionSet,
    ) -> Result<()> {
        let policy_type = session.policy_type();
        let records: Vec<ChunkProgress> = completed.records().cloned().collect();
        debug!(
            target: "uploader",
            session_id = %session.session_id(),
            policy_type = ?policy_type,
            chunks = records.len(),
            "Completing upload"
        );

        providers::complete_upload(&self.http_client, &self.cr_client, session, &records).await
    }

    /// Delete the remote upload session (cancel on server side)
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

    struct NoopProgress;

    impl ProgressCallback for NoopProgress {
        fn on_progress(&self, _update: ProgressUpdate) {}
    }

    fn params() -> UploadParams {
        UploadParams {
            source: UploadSource::from_bytes([1u8; 4].to_vec()),
            remote_uri: "cloudreve://my/cancelled.bin".into(),
            mime_type: None,
            last_modified: None,
            overwrite: false,
            policy_id: String::new(),
            metadata: None,
            previous_version: String::new(),
        }
    }

    #[tokio::test]
    async fn pre_cancelled_upload_fails_without_network() {
        let token = CancellationToken::new();
        token.cancel();
        let uploader = Uploader::new(
            Arc::new(CrClient::new(ClientConfig::new("http://127.0.0.1:1"))),
            UploaderConfig::default(),
        )
        .with_cancel_token(token);

        let error = uploader.upload(params(), NoopProgress).await.unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<UploadError>(),
                Some(UploadError::Cancelled)
            ),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn cancelling_a_child_does_not_cancel_the_uploader() {
        let uploader = Uploader::new(
            Arc::new(CrClient::new(ClientConfig::new("http://127.0.0.1:1"))),
            UploaderConfig::default(),
        );
        let first = uploader.cancel_token.child_token();
        first.cancel();
        assert!(first.is_cancelled());
        assert!(!uploader.cancel_token.is_cancelled());
        assert!(!uploader.cancel_token.child_token().is_cancelled());
    }
}
