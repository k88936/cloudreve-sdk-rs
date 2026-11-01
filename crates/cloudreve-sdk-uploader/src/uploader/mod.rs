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
#[cfg(feature = "session-store-fs")]
mod session_store_fs;
mod snapshot;
mod source;

use anyhow::{Context, Result};
pub(crate) use chunk::ChunkProgress;
use chunk::ChunkUploader;
pub use error::{UploadError, UploadResult};
pub use progress::{ProgressCallback, ProgressUpdate};
pub use session::UploadSession;
pub use session_store::{SessionKey, SessionStore, UploadIdentity};
#[cfg(feature = "session-store-fs")]
pub use session_store_fs::FileSessionStore;
pub use snapshot::{
    SNAPSHOT_VERSION, Snapshot, decode as decode_snapshot, encode as encode_snapshot,
};
pub use source::{RangeOpener, SourceReader, UploadSource};

use cloudreve_api::{Client as CrClient, api::ExplorerApi};
use reqwest::Client as HttpClient;
use std::collections::HashMap;
use std::fmt;
#[cfg(feature = "session-store-fs")]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Configuration for the uploader
#[derive(Clone)]
pub struct UploaderConfig {
    /// Maximum number of retry attempts per chunk
    pub max_retries: u32,
    /// Base delay between retries (exponential backoff)
    pub retry_base_delay: Duration,
    /// Maximum delay between retries
    pub retry_max_delay: Duration,
    /// Request timeout for chunk uploads
    pub request_timeout: Duration,
    /// Custom durable session store. `None` disables resume unless a
    /// file-system store is configured below.
    pub session_store: Option<Arc<dyn SessionStore>>,
    /// Directory for the built-in file-system session store. Takes effect only
    /// when `session_store` is `None`.
    #[cfg(feature = "session-store-fs")]
    pub session_dir: Option<PathBuf>,
}

impl fmt::Debug for UploaderConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("UploaderConfig");
        debug
            .field("max_retries", &self.max_retries)
            .field("retry_base_delay", &self.retry_base_delay)
            .field("retry_max_delay", &self.retry_max_delay)
            .field("request_timeout", &self.request_timeout)
            .field(
                "session_store",
                &self.session_store.as_ref().map(|_| "<configured>"),
            );
        #[cfg(feature = "session-store-fs")]
        debug.field("session_dir", &self.session_dir);
        debug.finish()
    }
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_base_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
            session_store: None,
            #[cfg(feature = "session-store-fs")]
            session_dir: None,
        }
    }
}

impl UploaderConfig {
    /// Use a custom durable session store.
    pub fn with_session_store(mut self, store: Arc<dyn SessionStore>) -> Self {
        self.session_store = Some(store);
        self
    }

    /// Enable resumable uploads with the built-in file-system store under `dir`.
    #[cfg(feature = "session-store-fs")]
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
    session_store: Option<Arc<dyn SessionStore>>,
}

impl Uploader {
    /// Create a new uploader instance
    pub fn new(cr_client: Arc<CrClient>, config: UploaderConfig) -> Self {
        let http_client = HttpClient::builder()
            .connect_timeout(config.request_timeout)
            .build()
            .expect("Failed to create HTTP client");

        #[cfg(feature = "session-store-fs")]
        let session_store = if let Some(store) = config.session_store.clone() {
            Some(store)
        } else {
            config.session_dir.as_ref().and_then(|dir| {
                match FileSessionStore::new(dir) {
                    Ok(store) => Some(Arc::new(store) as Arc<dyn SessionStore>),
                    Err(error) => {
                        warn!(
                            target: "uploader",
                            error = %error,
                            "Upload session persistence disabled"
                        );
                        None
                    }
                }
            })
        };
        #[cfg(not(feature = "session-store-fs"))]
        let session_store = config.session_store.clone();

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

        // Each upload gets its own cancellation scope. A chunk failure cancels
        // this child only; reusing the same `Uploader` must not inherit a
        // cancelled root token from a previous attempt.
        let upload_token = self.cancel_token.child_token();
        if upload_token.is_cancelled() {
            return Err(UploadError::Cancelled.into());
        }

        // Without a store there is no snapshot key to compute; skipping it also
        // avoids hashing an in-memory source when resume is disabled.
        let key = self
            .session_store
            .as_ref()
            .map(|_| self.session_key(&params));
        let loaded = match &key {
            Some(key) => self.load_session(&params, key).await,
            None => None,
        };
        // A resumed snapshot only stays usable while the server still holds the
        // session; remember whether this attempt continues an old one and how
        // far it had progressed.
        let resumed = loaded.is_some();
        let mut session = match loaded {
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
                if let Some(key) = &key {
                    self.persist_session(key, &session).await;
                }
                session
            }
        };

        let completed_before = completed_chunks(&session);

        // Create chunk uploader based on policy type
        let chunk_uploader = self.create_chunk_uploader(&session)?;
        let persistence = match (&self.session_store, &key) {
            (Some(store), Some(key)) => Some(chunk::SessionPersistence {
                store: Arc::clone(store),
                key: key.clone(),
            }),
            _ => None,
        };

        // Upload all chunks
        let progress = Arc::new(progress);
        let result = chunk_uploader
            .upload_all(
                &params.source,
                &mut session,
                progress,
                &upload_token,
                persistence,
            )
            .await;

        // A cancelled worker can return without setting an error; never let a
        // partial upload reach `complete_upload` and be reported as success.
        let result = match result {
            Ok(()) if !session.all_chunks_complete() => {
                let chunk_index = session.pending_chunks().first().copied().unwrap_or(0);
                Err(UploadError::ChunkUploadFailed {
                    chunk_index,
                    message: "upload ended with pending chunks".into(),
                }
                .into())
            }
            other => other,
        };

        match result {
            Ok(()) => {
                // Complete the upload
                self.complete_upload(&session).await?;
                if let (Some(store), Some(key)) = (&self.session_store, &key)
                    && let Err(error) = store.remove(key).await
                {
                    warn!(
                        target: "uploader",
                        error = %error,
                        "Failed to remove upload session snapshot"
                    );
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
                // A resumed session is only as durable as the server's copy of
                // it. When the server rejects the session before this attempt
                // made any progress, the snapshot can never be resumed: drop it
                // and report an expired session, so the caller starts over with
                // a fresh one instead of replaying a dead session forever.
                if resumed
                    && completed_chunks(&session) == completed_before
                    && error::is_remote_session_lost(&e)
                {
                    warn!(
                        target: "uploader",
                        source = %params.source.display(),
                        "Discarding unusable upload session snapshot"
                    );
                    if let (Some(store), Some(key)) = (&self.session_store, &key)
                        && let Err(error) = store.remove(key).await
                    {
                        warn!(
                            target: "uploader",
                            error = %error,
                            "Failed to remove unusable upload session snapshot"
                        );
                    }
                    return Err(UploadError::SessionExpired.into());
                }
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

    fn session_key(&self, params: &UploadParams) -> SessionKey {
        SessionKey::for_upload(&UploadIdentity {
            remote_uri: &params.remote_uri,
            source_id: params.source.identifier(),
            size: params.source.size(),
            policy_id: &params.policy_id,
            mime_type: params.mime_type.as_deref(),
            last_modified: params.last_modified,
            overwrite: params.overwrite,
            previous_version: &params.previous_version,
            metadata: params.metadata.as_ref(),
        })
    }

    async fn load_session(&self, params: &UploadParams, key: &SessionKey) -> Option<UploadSession> {
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
                if let Err(error) = store.remove(key).await {
                    warn!(
                        target: "uploader",
                        error = %error,
                        "Failed to remove unreadable upload session snapshot"
                    );
                }
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
            if let Err(error) = store.remove(key).await {
                warn!(
                    target: "uploader",
                    error = %error,
                    "Failed to remove stale upload session snapshot"
                );
            }
            return None;
        }
        Some(session)
    }

    async fn persist_session(&self, key: &SessionKey, session: &UploadSession) {
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

    /// Remove expired snapshots from the configured store.
    ///
    /// Returns the number of removed snapshots; stores without garbage
    /// collection return `0`.
    pub async fn prune_sessions(&self, now_unix: i64) -> Result<usize> {
        match &self.session_store {
            Some(store) => store.prune(now_unix).await,
            None => Ok(0),
        }
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

/// Number of chunks the session has already uploaded.
fn completed_chunks(session: &UploadSession) -> usize {
    session
        .chunk_progress
        .iter()
        .filter(|chunk| chunk.is_complete())
        .count()
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
