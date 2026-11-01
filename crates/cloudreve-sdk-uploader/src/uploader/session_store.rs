//! Durable upload-session/chunk snapshots.
//!
//! The [`SessionStore`] trait is deliberately narrow: it maps a
//! [`SessionKey`] to an [`UploadSession`] and back. Key derivation, versioning,
//! and snapshot encoding stay here so every backend agrees on them.
//!
//! Snapshots contain Cloudreve upload credentials. The file backend writes
//! them atomically with mode 0600 on Unix and removes them as soon as the
//! upload completes or the session becomes unusable.

use std::collections::HashMap;
use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use sha2::{Digest, Sha256};

use super::session::UploadSession;

/// Stable identity of one upload attempt.
///
/// This is intentionally separate from [`super::UploadParams`]: `UploadParams`
/// contains a boxed source and a `HashMap`, neither of which hashes
/// deterministically. Only fields that change the remote upload session are
/// included here.
#[derive(Clone, Copy, Debug)]
pub struct UploadIdentity<'a> {
    pub remote_uri: &'a str,
    pub source_id: &'a str,
    pub size: u64,
    pub policy_id: &'a str,
    pub mime_type: Option<&'a str>,
    pub last_modified: Option<i64>,
    pub overwrite: bool,
    pub previous_version: &'a str,
    pub metadata: Option<&'a HashMap<String, String>>,
}

/// Hashed, filesystem-safe key for a persisted upload session.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionKey(String);

impl SessionKey {
    /// Derive a key with unambiguous, length-prefixed field encoding.
    ///
    /// A plain `\n`-joined string lets a caller-supplied source id containing
    /// a newline collide with a different `(remote_uri, source_id)` pair; the
    /// length prefixes make such collisions impossible. Metadata is sorted so
    /// `HashMap` iteration order cannot change the key.
    pub fn for_upload(identity: &UploadIdentity<'_>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"cloudreve-upload-session-v2\0");

        put_str(&mut hasher, identity.remote_uri);
        put_str(&mut hasher, identity.source_id);
        hasher.update(identity.size.to_le_bytes());
        put_str(&mut hasher, identity.policy_id);
        put_opt_str(&mut hasher, identity.mime_type);
        put_opt_i64(&mut hasher, identity.last_modified);
        hasher.update([u8::from(identity.overwrite)]);
        put_str(&mut hasher, identity.previous_version);

        match identity.metadata {
            Some(metadata) => {
                hasher.update([1]);
                let mut entries: Vec<_> = metadata.iter().collect();
                entries.sort_by_key(|(left, _)| *left);
                hasher.update((entries.len() as u64).to_le_bytes());
                for (key, value) in entries {
                    put_str(&mut hasher, key);
                    put_str(&mut hasher, value);
                }
            }
            None => hasher.update([0]),
        }

        Self(format!("{:x}", hasher.finalize()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn put_str(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

fn put_opt_str(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            put_str(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn put_opt_i64(hasher: &mut Sha256, value: Option<i64>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_le_bytes());
        }
        None => hasher.update([0]),
    }
}

/// Persistence backend for resumable upload sessions.
///
/// Contract:
/// - `save` is atomic per key: a concurrent `load` sees either the old or the
///   new snapshot, never a partially written one. Concurrent saves may be
///   last-writer-wins.
/// - `load` returns `Ok(None)` for a missing key or an unsupported snapshot
///   version.
/// - `remove` is idempotent: removing a missing key succeeds.
/// - `prune` removes expired snapshots and returns how many were removed;
///   implementations that do not support garbage collection may return `0`.
///
/// Implementations must be safe to call from multiple tasks and should treat
/// the snapshot as sensitive (it contains upload credentials).
#[async_trait]
pub trait SessionStore: Send + Sync + 'static {
    async fn load(&self, key: &SessionKey) -> Result<Option<UploadSession>>;
    async fn save(&self, key: &SessionKey, session: &UploadSession) -> Result<()>;
    async fn remove(&self, key: &SessionKey) -> Result<()>;

    async fn prune(&self, _now_unix: i64) -> Result<usize> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_metadata() -> HashMap<String, String> {
        HashMap::new()
    }

    #[test]
    fn key_depends_on_policy_and_source() {
        let metadata = empty_metadata();
        let base = UploadIdentity {
            remote_uri: "cloudreve://my/a",
            source_id: "file:/a",
            size: 1,
            policy_id: "policy-a",
            mime_type: None,
            last_modified: None,
            overwrite: false,
            previous_version: "",
            metadata: Some(&metadata),
        };

        let a = SessionKey::for_upload(&base);
        let b = SessionKey::for_upload(&UploadIdentity {
            policy_id: "policy-b",
            ..base
        });
        let c = SessionKey::for_upload(&UploadIdentity {
            source_id: "file:/b",
            ..base
        });
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn key_encoding_is_unambiguous() {
        let metadata = empty_metadata();
        let left = UploadIdentity {
            remote_uri: "a",
            source_id: "b\nc",
            size: 1,
            policy_id: "",
            mime_type: None,
            last_modified: None,
            overwrite: false,
            previous_version: "",
            metadata: Some(&metadata),
        };
        let right = UploadIdentity {
            remote_uri: "a\nb",
            source_id: "c",
            ..left
        };
        assert_ne!(
            SessionKey::for_upload(&left),
            SessionKey::for_upload(&right)
        );
    }

    #[test]
    fn metadata_order_does_not_change_key() {
        let mut first = HashMap::new();
        first.insert("a".to_string(), "1".to_string());
        first.insert("b".to_string(), "2".to_string());
        let mut second = HashMap::new();
        second.insert("b".to_string(), "2".to_string());
        second.insert("a".to_string(), "1".to_string());

        let base = UploadIdentity {
            remote_uri: "cloudreve://my/a",
            source_id: "file:/a",
            size: 1,
            policy_id: "",
            mime_type: None,
            last_modified: None,
            overwrite: false,
            previous_version: "",
            metadata: Some(&first),
        };
        assert_eq!(
            SessionKey::for_upload(&base),
            SessionKey::for_upload(&UploadIdentity {
                metadata: Some(&second),
                ..base
            })
        );
    }
}
