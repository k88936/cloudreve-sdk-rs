//! Upload session description.
//!
//! [`UploadSession`] is a plain data transfer object: it describes what the
//! server agreed to upload (file size, chunk geometry, credential, expiry) and
//! never changes while an upload runs. How far the upload got is not part of
//! the session; finished chunks are described by
//! [`ChunkProgress`](super::ChunkProgress) records owned by the caller.

use crate::uploader::providers::PolicyType;
use chrono::Utc;
use cloudreve_api::models::explorer::{EncryptMetadata, UploadCredential};

/// An upload session created by Cloudreve.
#[derive(Debug, Clone)]
pub struct UploadSession {
    /// Stable source identifier (path/fingerprint/content URI).
    pub source_id: String,
    /// Remote URI (cloudreve path).
    pub remote_uri: String,
    /// Total file size.
    pub file_size: u64,
    /// Chunk size the server chose for this upload.
    pub chunk_size: u64,
    /// Encryption metadata (if encrypted).
    pub encrypt_metadata: Option<EncryptMetadata>,
    /// Session expiration timestamp.
    pub expires_at: i64,
    /// Storage policy type.
    policy_type: PolicyType,
    /// Whether the upload is relayed through Cloudreve.
    relay: bool,
    /// Upload credential from Cloudreve.
    credential: UploadCredential,
}

impl UploadSession {
    /// Create a session from a credential response.
    pub fn new(
        source_id: String,
        remote_uri: String,
        file_size: u64,
        credential: UploadCredential,
    ) -> Self {
        let chunk_size = credential.chunk_size.max(0) as u64;
        let policy_type = credential
            .storage_policy
            .as_ref()
            .map(|policy| PolicyType::from_api(&policy.policy_type))
            .unwrap_or(PolicyType::Local);
        let relay = credential
            .storage_policy
            .as_ref()
            .and_then(|policy| policy.relay)
            .unwrap_or(false);
        let expires_at = credential.expires;

        Self {
            source_id,
            remote_uri,
            file_size,
            chunk_size,
            encrypt_metadata: credential.encrypt_metadata.clone(),
            expires_at,
            policy_type,
            relay,
            credential,
        }
    }

    /// Number of chunks this session uploads.
    ///
    /// An empty file (or a zero chunk size) still has one chunk.
    pub fn num_chunks(&self) -> usize {
        if self.file_size == 0 || self.chunk_size == 0 {
            return 1;
        }
        self.file_size.div_ceil(self.chunk_size) as usize
    }

    /// Get the session ID from the credential.
    pub fn session_id(&self) -> &str {
        &self.credential.session_id
    }

    /// Get the upload credential.
    pub fn credential(&self) -> &UploadCredential {
        &self.credential
    }

    /// Get the policy type.
    pub fn policy_type(&self) -> PolicyType {
        self.policy_type
    }

    /// Whether the upload is relayed through Cloudreve.
    pub fn is_relay(&self) -> bool {
        self.relay
    }

    /// Check if the session has expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at > 0 && Utc::now().timestamp() >= self.expires_at
    }

    /// Get the expected size for a specific chunk.
    pub fn chunk_size_for(&self, chunk_index: usize) -> u64 {
        if self.chunk_size == 0 {
            return self.file_size;
        }
        let start = chunk_index as u64 * self.chunk_size;
        let remaining = self.file_size.saturating_sub(start);
        remaining.min(self.chunk_size)
    }

    /// Get the byte range `[start, end)` for a specific chunk.
    pub fn chunk_range(&self, chunk_index: usize) -> (u64, u64) {
        if self.chunk_size == 0 {
            return (0, self.file_size);
        }
        let start = chunk_index as u64 * self.chunk_size;
        let size = self.chunk_size_for(chunk_index);
        (start, start + size)
    }

    /// Get the upload URL for a specific chunk.
    pub fn upload_url_for_chunk(&self, chunk_index: usize) -> Option<&str> {
        self.credential
            .upload_urls
            .as_ref()
            .and_then(|urls| urls.get(chunk_index))
            .map(|s| s.as_str())
    }

    /// Get the first upload URL (for providers that use a single URL).
    pub fn upload_url(&self) -> Option<&str> {
        self.credential
            .upload_urls
            .as_ref()
            .and_then(|urls| urls.first().map(|s| s.as_str()))
    }

    /// Get the completion URL for multipart uploads.
    pub fn complete_url(&self) -> &str {
        self.credential.complete_url.as_deref().unwrap_or_default()
    }

    /// Get the callback secret.
    pub fn callback_secret(&self) -> &str {
        &self.credential.callback_secret
    }

    /// Get the upload credential string.
    pub fn credential_string(&self) -> &str {
        &self.credential.credential
    }

    /// Get upload policy (for Upyun).
    pub fn upload_policy(&self) -> Option<&str> {
        self.credential.upload_policy.as_deref()
    }

    /// Get MIME type.
    pub fn mime_type(&self) -> Option<&str> {
        self.credential.mime_type.as_deref()
    }

    /// Check if encryption is enabled.
    pub fn is_encrypted(&self) -> bool {
        self.encrypt_metadata.is_some()
    }

    /// Check if streaming encryption is supported.
    pub fn supports_streaming_encryption(&self) -> bool {
        self.credential
            .storage_policy
            .as_ref()
            .and_then(|policy| policy.streaming_encryption)
            .unwrap_or(false)
    }

    /// Get chunk upload concurrency from the storage policy.
    ///
    /// Returns the configured concurrency level for concurrent chunk uploads.
    /// Defaults to 1 (sequential uploads) if not specified.
    pub fn chunk_concurrency(&self) -> usize {
        self.credential
            .storage_policy
            .as_ref()
            .and_then(|policy| policy.chunk_concurrency)
            .map(|concurrency| concurrency.max(1) as usize)
            .unwrap_or(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudreve_api::models::explorer::UploadCredential;

    fn session(file_size: u64, chunk_size: i64) -> UploadSession {
        UploadSession::new(
            "file:/a".into(),
            "cloudreve://my/a.bin".into(),
            file_size,
            UploadCredential {
                session_id: "session-1".into(),
                expires: 4_000_000_000,
                chunk_size,
                ..Default::default()
            },
        )
    }

    #[test]
    fn chunk_geometry_covers_the_whole_file() {
        let session = session(12, 5);

        assert_eq!(session.num_chunks(), 3);
        assert_eq!(session.chunk_range(0), (0, 5));
        assert_eq!(session.chunk_range(2), (10, 12));
        assert_eq!(session.chunk_size_for(2), 2);
    }

    #[test]
    fn empty_file_still_has_one_chunk() {
        let session = session(0, 5);

        assert_eq!(session.num_chunks(), 1);
        assert_eq!(session.chunk_size_for(0), 0);
        assert_eq!(session.chunk_range(0), (0, 0));
    }

    #[test]
    fn zero_chunk_size_means_one_whole_file_chunk() {
        let session = session(12, 0);

        assert_eq!(session.num_chunks(), 1);
        assert_eq!(session.chunk_range(0), (0, 12));
    }

    #[test]
    fn session_id_comes_from_the_credential() {
        assert_eq!(session(1, 1).session_id(), "session-1");
    }
}
