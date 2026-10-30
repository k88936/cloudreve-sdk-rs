//! File uploader implementation for CLI clients.
//!
//! This module provides chunked file upload functionality with support for
//! multiple storage providers, encryption, retries, cancellation, and progress tracking.

mod chunk;
#[cfg(target_os = "android")]
mod content_uri;
mod encrypt;
mod error;
mod progress;
#[allow(dead_code)]
mod providers;
#[allow(dead_code)]
mod session;
mod session_store;
mod source;

use anyhow::{Context, Result};
pub(crate) use chunk::ChunkProgress;
use chunk::ChunkUploader;
pub use error::{UploadError, UploadResult};
pub use progress::{ProgressCallback, ProgressUpdate};
pub use source::{RangeOpener, SourceReader, UploadSource};
use session::UploadSession;
use session_store::SessionStore;

use cloudreve_api::{Client as CrClient, api::ExplorerApi};
use reqwest::Client as HttpClient;
use std::collections::HashMap;
use std::path::PathBuf;
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
    /// Directory for durable session/chunk snapshots. `None` disables resume.
    pub session_dir: Option<PathBuf>,
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_base_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
            session_dir: None,
        }
    }
}

impl UploaderConfig {
    /// Enable resumable uploads with snapshots under `dir`.
    pub fn with_session_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.session_dir = Some(dir.into());
        self
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
    /// Optional durable session store.
    session_store: Option<Arc<SessionStore>>,
}

impl Uploader {
    /// Create a new uploader instance
    pub fn new(cr_client: Arc<CrClient>, config: UploaderConfig) -> Self {
        let http_client = HttpClient::builder()
            .connect_timeout(config.request_timeout)
            .build()
            .expect("Failed to create HTTP client");

        let session_store = config.session_dir.as_ref().and_then(|dir| {
            match SessionStore::new(dir) {
                Ok(store) => Some(Arc::new(store)),
                Err(error) => {
                    warn!(
                        target: "uploader",
                        error = %error,
                        "Upload session persistence disabled"
                    );
                    None
                }
            }
        });

        Self {
            cr_client,
            http_client,
            config,
            cancel_token: CancellationToken::new(),
            session_store,
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

        let key = self.session_key(&params);
        let mut session = match self.load_session(&params, &key).await {
            Some(session) => {
                info!(
                    target: "uploader",
                    session_id = %session.session_id(),
                    uploaded = session.total_uploaded(),
                    "Resuming persisted upload session"
                );
                session
            }
            None => {
                let session = self.create_session(&params).await?;
                self.persist_session(&key, &session).await;
                session
            }
        };

        // Create chunk uploader based on policy type
        let chunk_uploader = self.create_chunk_uploader(&session)?;
        let persistence = self.session_store.as_ref().map(|store| {
            chunk::SessionPersistence {
                store: Arc::clone(store),
                key: key.clone(),
            }
        });

        // Upload all chunks
        let progress = Arc::new(progress);
        let result = chunk_uploader
            .upload_all(
                &params.source,
                &mut session,
                progress,
                &self.cancel_token,
                persistence,
            )
            .await;

        match result {
            Ok(()) => {
                // Complete the upload
                self.complete_upload(&session).await?;
                if let Some(store) = &self.session_store {
                    store.remove(&key).await;
                }
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
                if self.session_store.is_some() {
                    warn!(
                        target: "uploader",
                        source = %params.source.display(),
                        "Keeping upload session snapshot for resume"
                    );
                } else if let Err(e) = self.delete_remote_session(&session).await {
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

    fn session_key(&self, params: &UploadParams) -> String {
        SessionStore::key(
            &params.remote_uri,
            params.source.identifier(),
            params.source.size(),
            &params.policy_id,
        )
    }

    async fn load_session(&self, params: &UploadParams, key: &str) -> Option<UploadSession> {
        let store = self.session_store.as_ref()?;
        let session = match store.load(key).await {
            Ok(Some(session)) => session,
            Ok(None) => return None,
            Err(error) => {
                warn!(
                    target: "uploader",
                    error = %error,
                    "Ignoring unreadable upload session snapshot"
                );
                store.remove(key).await;
                return None;
            }
        };
        let valid = session.remote_uri == params.remote_uri
            && session.file_size == params.source.size()
            && session.source_id == params.source.identifier()
            && !session.is_expired()
            && session.num_chunks() > 0;
        if !valid {
            warn!(
                target: "uploader",
                session_id = %session.session_id(),
                "Discarding stale upload session snapshot"
            );
            store.remove(key).await;
            return None;
        }
        Some(session)
    }

    async fn persist_session(&self, key: &str, session: &UploadSession) {
        if let Some(store) = &self.session_store
            && let Err(error) = store.save(key, session).await
        {
            warn!(
                target: "uploader",
                error = %error,
                "Failed to persist upload session; upload will continue"
            );
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

        // Create session object
        let session = UploadSession::new(
            String::new(),
            String::new(),
            params.source.identifier().to_string(),
            params.remote_uri.clone(),
            params.source.size(),
            credential,
        );

        Ok(session)
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
    async fn complete_upload(&self, session: &UploadSession) -> Result<()> {
        let policy_type = session.policy_type();
        debug!(
            target: "uploader",
            session_id = %session.session_id(),
            policy_type = ?policy_type,
            "Completing upload"
        );

        providers::complete_upload(&self.http_client, &self.cr_client, session).await
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
