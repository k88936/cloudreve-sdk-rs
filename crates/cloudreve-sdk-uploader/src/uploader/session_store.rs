//! Durable upload-session/chunk snapshots.
//!
//! Snapshots contain Cloudreve upload credentials, so files are written
//! atomically with mode 0600 on Unix and removed as soon as the upload
//! completes or the session becomes unusable.

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::session::UploadSession;

const SNAPSHOT_VERSION: u32 = 1;

pub(crate) struct SessionStore {
    dir: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    session: UploadSession,
}

impl SessionStore {
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create upload session dir {}", dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        }
        Ok(Self { dir })
    }

    pub fn key(remote_uri: &str, source_id: &str, size: u64, policy_id: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(remote_uri.as_bytes());
        hasher.update(b"\n");
        hasher.update(source_id.as_bytes());
        hasher.update(b"\n");
        hasher.update(size.to_le_bytes());
        hasher.update(b"\n");
        hasher.update(policy_id.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{key}.json"))
    }

    pub async fn load(&self, key: &str) -> Result<Option<UploadSession>> {
        let path = self.path(key);
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read upload session {}", path.display()));
            }
        };
        let snapshot: Snapshot = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to decode upload session {}", path.display()))?;
        if snapshot.version != SNAPSHOT_VERSION {
            let _ = tokio::fs::remove_file(&path).await;
            return Ok(None);
        }
        Ok(Some(snapshot.session))
    }

    pub async fn save(&self, key: &str, session: &UploadSession) -> Result<()> {
        let path = self.path(key);
        let tmp = path.with_extension("tmp");
        let bytes = serde_json::to_vec(&Snapshot {
            version: SNAPSHOT_VERSION,
            session: session.clone(),
        })
        .context("failed to encode upload session")?;
        tokio::fs::write(&tmp, bytes)
            .await
            .with_context(|| format!("failed to write upload session {}", tmp.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))
                .await
                .context("failed to protect upload session")?;
        }
        tokio::fs::rename(&tmp, &path)
            .await
            .with_context(|| format!("failed to commit upload session {}", path.display()))?;
        Ok(())
    }

    pub async fn remove(&self, key: &str) {
        let _ = tokio::fs::remove_file(self.path(key)).await;
        let _ = tokio::fs::remove_file(self.path(key).with_extension("tmp")).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudreve_api::models::explorer::UploadCredential;

    fn credential() -> UploadCredential {
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
            uri: "cloudreve://my/test.bin".into(),
            mime_type: None,
            upload_policy: None,
            encrypt_metadata: None,
        }
    }

    #[tokio::test]
    async fn saves_resumes_and_removes_chunk_progress() {
        let dir = std::env::temp_dir().join(format!(
            "upload-session-store-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        let store = SessionStore::new(&dir).unwrap();
        let key = SessionStore::key("cloudreve://my/test.bin", "content://media/1", 10, "");
        let mut session = UploadSession::new(
            "task".into(),
            "drive".into(),
            "content://media/1".into(),
            "cloudreve://my/test.bin".into(),
            10,
            credential(),
        );
        session.complete_chunk(0, Some("etag-0".into()));
        store.save(&key, &session).await.unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.join(format!("{key}.json")))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }

        let loaded = store.load(&key).await.unwrap().unwrap();
        assert_eq!(loaded.total_uploaded(), 5);
        assert_eq!(loaded.pending_chunks(), vec![1]);
        assert_eq!(loaded.source_id, "content://media/1");

        store.remove(&key).await;
        assert!(store.load(&key).await.unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn key_depends_on_policy_and_source() {
        let a = SessionStore::key("cloudreve://my/a", "file:/a", 1, "policy-a");
        let b = SessionStore::key("cloudreve://my/a", "file:/a", 1, "policy-b");
        let c = SessionStore::key("cloudreve://my/a", "file:/b", 1, "policy-a");
        assert_ne!(a, b);
        assert_ne!(a, c);
    }
}
