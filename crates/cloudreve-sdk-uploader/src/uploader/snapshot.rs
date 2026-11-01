//! Versioned snapshot format shared by every [`super::SessionStore`] backend.
//!
//! Backends that are not file systems (databases, key-value stores) persist the
//! same bytes the built-in file store writes, so tooling that understands one
//! backend understands all of them. Encoding lives here instead of in the file
//! store so the format has exactly one owner.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::session::UploadSession;

/// Version of the serialized snapshot format.
///
/// Bump this whenever the encoded shape changes incompatibly. Snapshots with a
/// different version are never resumed: they cannot be interpreted safely.
pub const SNAPSHOT_VERSION: u32 = 1;

/// One persisted upload session together with the version it was written in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Format version the snapshot was written with.
    pub version: u32,
    /// The persisted session.
    pub session: UploadSession,
}

impl Snapshot {
    /// Wrap a session in a snapshot of the current format version.
    pub fn new(session: &UploadSession) -> Self {
        Self {
            version: SNAPSHOT_VERSION,
            session: session.clone(),
        }
    }

    /// Whether this build can interpret the snapshot.
    pub fn is_supported(&self) -> bool {
        self.version == SNAPSHOT_VERSION
    }

    /// Encode the snapshot into the shared byte format.
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).context("failed to encode upload session snapshot")
    }

    /// Decode a snapshot from the shared byte format.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).context("failed to decode upload session snapshot")
    }
}

/// Encode a session into the shared snapshot byte format.
pub fn encode(session: &UploadSession) -> Result<Vec<u8>> {
    Snapshot::new(session).encode()
}

/// Decode snapshot bytes.
///
/// Returns `Ok(None)` when the bytes are a valid snapshot of an unsupported
/// format version: such a snapshot can never be resumed, so callers should drop
/// it instead of surfacing the version mismatch as an error.
pub fn decode(bytes: &[u8]) -> Result<Option<UploadSession>> {
    let snapshot = Snapshot::decode(bytes)?;
    Ok(snapshot.is_supported().then_some(snapshot.session))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudreve_api::models::explorer::UploadCredential;

    fn session() -> UploadSession {
        UploadSession::new(
            "task".into(),
            "drive".into(),
            "file:/a".into(),
            "cloudreve://my/a.bin".into(),
            10,
            UploadCredential {
                session_id: "session-1".into(),
                expires: 4_000_000_000,
                chunk_size: 5,
                upload_urls: None,
                credential: "secret".into(),
                upload_id: String::new(),
                callback_secret: "callback".into(),
                ak: None,
                key_time: None,
                complete_url: None,
                storage_policy: None,
                uri: "cloudreve://my/a.bin".into(),
                mime_type: None,
                upload_policy: None,
                encrypt_metadata: None,
            },
        )
    }

    #[test]
    fn round_trips_through_bytes() {
        let mut session = session();
        session.complete_chunk(0, Some("etag".into()));

        let bytes = encode(&session).unwrap();
        let decoded = decode(&bytes).unwrap().expect("supported snapshot");

        assert_eq!(decoded.session_id(), "session-1");
        assert_eq!(decoded.total_uploaded(), 5);
        assert_eq!(decoded.pending_chunks(), vec![1]);
    }

    #[test]
    fn unsupported_version_decodes_to_nothing() {
        let mut snapshot = Snapshot::new(&session());
        snapshot.version = SNAPSHOT_VERSION + 1;

        assert!(decode(&snapshot.encode().unwrap()).unwrap().is_none());
    }

    #[test]
    fn garbage_bytes_are_an_error() {
        assert!(decode(b"not a snapshot").is_err());
    }
}
