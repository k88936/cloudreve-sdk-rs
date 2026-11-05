//! Upload session description.
//!
//! [`UploadSession`] is a plain data transfer object: it holds the file size
//! and the [`UploadCredential`] the server returned. Everything else - chunk
//! geometry, expiry, policy, encryption metadata - is read from the credential
//! on demand, so the two cannot disagree. How far the upload got is not part of
//! the session; finished chunks are described by
//! [`ChunkProgress`](super::ChunkProgress) records owned by the caller.

use crate::uploader::providers::PolicyType;
use chrono::Utc;
use cloudreve_api::models::explorer::{EncryptMetadata, UploadCredential};
use serde::{Deserialize, Serialize};

/// An upload session created by Cloudreve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSession {
    /// Total file size, from the upload request.
    file_size: u64,
    /// Upload credential from Cloudreve; the single source of everything else.
    credential: UploadCredential,
}

impl UploadSession {
    /// Create a session from a credential response.
    pub fn new(file_size: u64, credential: UploadCredential) -> Self {
        let relayed = credential
            .storage_policy
            .as_ref()
            .and_then(|policy| policy.relay)
            .unwrap_or(false);
        if relayed {
            debug_assert_eq!(
                credential.chunk_size, 0,
                "the server never chunks a relayed upload"
            );
            debug_assert!(
                credential.encrypt_metadata.is_none(),
                "the server never encrypts a relayed upload"
            );
        }
        debug_assert!(
            credential.expires > 0,
            "the server always sets an expiry on the credential"
        );

        Self {
            file_size,
            credential,
        }
    }

    /// Total size of the file this session uploads.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Chunk size the server chose; `0` means "the whole file is one chunk".
    pub fn chunk_size(&self) -> u64 {
        self.credential.chunk_size.max(0) as u64
    }

    /// Number of chunks this session uploads.
    ///
    /// An empty file (or a zero chunk size) still has one chunk.
    pub fn num_chunks(&self) -> usize {
        let chunk_size = self.chunk_size();
        if self.file_size == 0 || chunk_size == 0 {
            return 1;
        }
        self.file_size.div_ceil(chunk_size) as usize
    }

    /// Get the session ID from the credential.
    pub fn session_id(&self) -> &str {
        &self.credential.session_id
    }

    /// Get the upload credential.
    pub fn credential(&self) -> &UploadCredential {
        &self.credential
    }

    /// Policy type that actually handles the chunks.
    ///
    /// A relayed upload is proxied by Cloudreve itself, whatever the policy is.
    pub(crate) fn effective_policy_type(&self) -> PolicyType {
        let policy = self.credential.storage_policy.as_ref();
        if policy.and_then(|policy| policy.relay).unwrap_or(false) {
            return PolicyType::Local;
        }
        policy
            .map(|policy| PolicyType::from_api(&policy.policy_type))
            .unwrap_or(PolicyType::Local)
    }

    /// When the credential expires, as a Unix timestamp.
    pub fn expires_at(&self) -> i64 {
        self.credential.expires
    }

    /// Check if the session has expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at() > 0 && Utc::now().timestamp() >= self.expires_at()
    }

    /// Get the expected size for a specific chunk.
    pub fn chunk_size_for(&self, chunk_index: usize) -> u64 {
        let chunk_size = self.chunk_size();
        if chunk_size == 0 {
            return self.file_size;
        }
        let start = chunk_index as u64 * chunk_size;
        let remaining = self.file_size.saturating_sub(start);
        remaining.min(chunk_size)
    }

    /// Get the byte range `[start, end)` for a specific chunk.
    pub fn chunk_range(&self, chunk_index: usize) -> (u64, u64) {
        let chunk_size = self.chunk_size();
        if chunk_size == 0 {
            return (0, self.file_size);
        }
        let start = chunk_index as u64 * chunk_size;
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

    /// Encryption metadata the server sent for this upload, if any.
    pub fn encrypt_metadata(&self) -> Option<&EncryptMetadata> {
        self.credential.encrypt_metadata.as_ref()
    }

    /// Check if encryption is enabled.
    pub fn is_encrypted(&self) -> bool {
        self.encrypt_metadata().is_some()
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
