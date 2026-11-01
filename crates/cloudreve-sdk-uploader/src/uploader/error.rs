//! Error types for the uploader module

use thiserror::Error;

/// Result type for upload operations
pub type UploadResult<T> = Result<T, UploadError>;

/// Upload error types
#[derive(Debug, Error)]
pub enum UploadError {
    /// Upload was cancelled
    #[error("Upload cancelled")]
    Cancelled,

    /// Failed to create upload session
    #[error("Failed to create upload session: {0}")]
    SessionCreationFailed(String),

    /// Failed to delete upload session
    #[error("Failed to delete upload session: {0}")]
    SessionDeletionFailed(String),

    /// Upload session expired
    #[error("Upload session expired")]
    SessionExpired,

    /// Failed to read local file
    #[error("Failed to read local file: {0}")]
    FileReadError(String),

    /// Chunk upload failed
    #[error("Chunk {chunk_index} upload failed: {message}")]
    ChunkUploadFailed { chunk_index: usize, message: String },

    /// Failed to complete multipart upload
    #[error("Failed to complete upload: {0}")]
    CompletionFailed(String),

    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    HttpError(String),

    /// Database error
    #[error("Database error: {0}")]
    DatabaseError(String),

    /// Encryption error
    #[error("Encryption error: {0}")]
    EncryptionError(String),

    /// Invalid policy type
    #[error("Invalid policy type: {0}")]
    InvalidPolicyType(String),

    /// Storage provider error (with provider-specific details)
    #[error("Storage provider error ({provider}): {message}")]
    ProviderError { provider: String, message: String },

    /// Maximum retries exceeded
    #[error("Maximum retries ({max_retries}) exceeded for chunk {chunk_index}")]
    MaxRetriesExceeded {
        chunk_index: usize,
        max_retries: u32,
    },

    /// OneDrive specific: Empty file not supported
    #[error("OneDrive does not support empty file uploads")]
    OneDriveEmptyFile,

    /// OneDrive specific: Chunk overlap error
    #[error("OneDrive chunk overlap error: {0}")]
    OneDriveChunkOverlap(String),

    /// S3-like error response
    #[error("S3 error ({code}): {message}")]
    S3Error { code: String, message: String },

    /// Qiniu error response
    #[error("Qiniu error: {0}")]
    QiniuError(String),

    /// Upyun error response
    #[error("Upyun error ({code}): {message}")]
    UpyunError { code: i32, message: String },

    /// Callback to Cloudreve server failed
    #[error("Upload callback failed: {0}")]
    CallbackFailed(String),

    /// Other errors
    #[error("{0}")]
    Other(String),
}

impl UploadError {
    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            UploadError::HttpError(_)
                | UploadError::ChunkUploadFailed { .. }
                | UploadError::ProviderError { .. }
        )
    }

    /// Check if this error is due to cancellation
    pub fn is_cancelled(&self) -> bool {
        matches!(self, UploadError::Cancelled)
    }

    /// Create a chunk upload error
    pub fn chunk_failed(chunk_index: usize, message: impl Into<String>) -> Self {
        UploadError::ChunkUploadFailed {
            chunk_index,
            message: message.into(),
        }
    }

    /// Create a provider error
    pub fn provider_error(provider: impl Into<String>, message: impl Into<String>) -> Self {
        UploadError::ProviderError {
            provider: provider.into(),
            message: message.into(),
        }
    }

    /// Create an S3 error
    pub fn s3_error(code: impl Into<String>, message: impl Into<String>) -> Self {
        UploadError::S3Error {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Create an Upyun error
    pub fn upyun_error(code: i32, message: impl Into<String>) -> Self {
        UploadError::UpyunError {
            code,
            message: message.into(),
        }
    }
}

impl From<std::io::Error> for UploadError {
    fn from(err: std::io::Error) -> Self {
        UploadError::FileReadError(err.to_string())
    }
}

impl From<reqwest::Error> for UploadError {
    fn from(err: reqwest::Error) -> Self {
        UploadError::HttpError(err.to_string())
    }
}

impl From<anyhow::Error> for UploadError {
    fn from(err: anyhow::Error) -> Self {
        UploadError::Other(err.to_string())
    }
}

/// Whether a failed chunk upload means the server refused the request.
///
/// The distinction that matters is "the server answered and rejected this
/// request" versus "the request never made it": transport failures,
/// cancellations and token problems leave the snapshot perfectly usable, while
/// an application error means the server looked at the session and said no.
/// Cloudreve has no single error code for a vanished session, so the check is
/// deliberately structural instead of matching specific codes.
pub(crate) fn is_remote_session_lost(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<cloudreve_api::ApiError>(),
        Some(
            cloudreve_api::ApiError::ApiError { .. }
                | cloudreve_api::ApiError::LockConflict { .. }
                | cloudreve_api::ApiError::BatchError { .. }
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api_error(code: i32) -> anyhow::Error {
        anyhow::anyhow!(cloudreve_api::ApiError::ApiError {
            code,
            message: "upload session not found".into(),
            error_detail: None,
            correlation_id: None,
            aggregated_errors: None,
        })
    }

    #[test]
    fn server_rejections_count_as_lost_sessions() {
        assert!(is_remote_session_lost(&api_error(40004)));

        let contextual = api_error(40004).context("failed to upload chunk");
        assert!(is_remote_session_lost(&contextual));
    }

    #[test]
    fn local_and_transport_failures_do_not() {
        assert!(!is_remote_session_lost(&anyhow::anyhow!("stream closed")));
        assert!(!is_remote_session_lost(
            &anyhow::anyhow!(cloudreve_api::ApiError::Other("boom".into()))
        ));
    }
}
