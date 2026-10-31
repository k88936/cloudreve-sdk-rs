//! File-system backed [`SessionStore`].
//!
//! Enabled by the default `session-store-fs` feature.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use async_trait::async_trait;

use super::session::UploadSession;
use super::session_store::{SNAPSHOT_VERSION, SessionKey, SessionStore, Snapshot};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Stores one JSON snapshot per session key under a directory.
pub struct FileSessionStore {
    dir: PathBuf,
}

impl FileSessionStore {
    /// Create the directory (mode 0700 on Unix) and return a store for it.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).with_context(|| {
            format!("failed to create upload session dir {}", dir.display())
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        }
        Ok(Self { dir })
    }

    fn path(&self, key: &SessionKey) -> PathBuf {
        self.dir.join(format!("{key}.json"))
    }

    fn temp_path(&self, key: &SessionKey) -> PathBuf {
        let nonce = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        self.dir.join(format!(
            ".{key}.{}.{nonce}.tmp",
            std::process::id()
        ))
    }
}

#[async_trait]
impl SessionStore for FileSessionStore {
    async fn load(&self, key: &SessionKey) -> Result<Option<UploadSession>> {
        let path = self.path(key);
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to read upload session {}", path.display())
                });
            }
        };
        let snapshot: Snapshot = serde_json::from_slice(&bytes).with_context(|| {
            format!("failed to decode upload session {}", path.display())
        })?;
        if snapshot.version != SNAPSHOT_VERSION {
            let _ = tokio::fs::remove_file(&path).await;
            return Ok(None);
        }
        Ok(Some(snapshot.session))
    }

    async fn save(&self, key: &SessionKey, session: &UploadSession) -> Result<()> {
        let path = self.path(key);
        let tmp = self.temp_path(key);
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

    async fn remove(&self, key: &SessionKey) -> Result<()> {
        match tokio::fs::remove_file(self.path(key)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error)
                .with_context(|| format!("failed to remove upload session {key}")),
        }
    }

    async fn prune(&self, now_unix: i64) -> Result<usize> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to read upload session dir {}", self.dir.display())
                });
            }
        };

        let mut removed = 0;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            match path.extension().and_then(|value| value.to_str()) {
                Some("tmp") => {
                    let _ = tokio::fs::remove_file(&path).await;
                }
                Some("json") => {
                    let expired = match tokio::fs::read(&path).await {
                        Ok(bytes) => match serde_json::from_slice::<Snapshot>(&bytes) {
                            Ok(snapshot) => {
                                snapshot.version != SNAPSHOT_VERSION
                                    || (snapshot.session.expires_at > 0
                                        && snapshot.session.expires_at <= now_unix)
                            }
                            Err(_) => true,
                        },
                        Err(_) => true,
                    };
                    if expired {
                        let _ = tokio::fs::remove_file(&path).await;
                        removed += 1;
                    }
                }
                _ => {}
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uploader::session_store::UploadIdentity;
    use cloudreve_api::models::explorer::UploadCredential;

    fn credential(expires: i64) -> UploadCredential {
        UploadCredential {
            session_id: "session-1".into(),
            expires,
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

    fn key() -> SessionKey {
        SessionKey::for_upload(&UploadIdentity {
            remote_uri: "cloudreve://my/test.bin",
            source_id: "content://media/1",
            size: 10,
            policy_id: "",
            mime_type: None,
            last_modified: None,
            overwrite: false,
            previous_version: "",
            metadata: None,
        })
    }

    fn dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{name}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ))
    }

    #[tokio::test]
    async fn saves_resumes_and_removes_chunk_progress() {
        let dir = dir("upload-session-store");
        let store = FileSessionStore::new(&dir).unwrap();
        let key = key();
        let mut session = UploadSession::new(
            "task".into(),
            "drive".into(),
            "content://media/1".into(),
            "cloudreve://my/test.bin".into(),
            10,
            credential(4_000_000_000),
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

        store.remove(&key).await.unwrap();
        assert!(store.load(&key).await.unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn prune_removes_expired_snapshots() {
        let dir = dir("upload-session-prune");
        let store = FileSessionStore::new(&dir).unwrap();
        let key = key();
        let session = UploadSession::new(
            "task".into(),
            "drive".into(),
            "content://media/1".into(),
            "cloudreve://my/test.bin".into(),
            10,
            credential(10),
        );
        store.save(&key, &session).await.unwrap();
        assert_eq!(store.prune(11).await.unwrap(), 1);
        assert!(store.load(&key).await.unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }
}
