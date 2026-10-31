//! Random-access upload sources.
//!
//! The uploader never needs a local path: every chunk asks the source for a
//! fresh range stream. This keeps Android `content://` URIs (and any other
//! platform reader) out of the app cache.

use std::fmt;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result, bail};
use futures::future::BoxFuture;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};

/// A stream limited to one chunk range.
pub type SourceReader = Box<dyn AsyncRead + Send + Sync + Unpin>;
/// Opens `[offset, offset + len)` from a source. The returned reader must not
/// read beyond `len`; the uploader also enforces the limit.
pub type RangeOpener =
    Arc<dyn Fn(u64, u64) -> BoxFuture<'static, Result<SourceReader>> + Send + Sync>;

trait SourceImpl: Send + Sync {
    fn size(&self) -> u64;
    fn identifier(&self) -> &str;
    fn display(&self) -> &str;
    fn view(&self, offset: u64, len: u64) -> BoxFuture<'_, Result<SourceReader>>;
}

/// A reusable, cloneable upload source.
#[derive(Clone)]
pub struct UploadSource {
    inner: Arc<dyn SourceImpl>,
}

impl fmt::Debug for UploadSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadSource")
            .field("identifier", &self.inner.identifier())
            .field("size", &self.inner.size())
            .finish()
    }
}

impl UploadSource {
    /// Open a regular file. The file is re-opened and seeked for every chunk,
    /// so the source is safe to use concurrently.
    pub fn from_path(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let metadata = std::fs::metadata(&path)
            .with_context(|| format!("failed to stat upload source {}", path.display()))?;
        if !metadata.is_file() {
            bail!("upload source is not a file: {}", path.display());
        }
        let modified = metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| format!("{}.{}", value.as_secs(), value.subsec_nanos()))
            .unwrap_or_else(|| "unknown".to_string());
        let absolute = std::fs::canonicalize(&path).unwrap_or(path);
        let identifier = format!(
            "file:{}:{}:{}",
            absolute.display(),
            metadata.len(),
            modified
        );
        Ok(Self {
            inner: Arc::new(PathSource {
                path: absolute.clone(),
                display: absolute.to_string_lossy().to_string(),
                size: metadata.len(),
                identifier,
            }),
        })
    }

    /// Upload an in-memory buffer.
    ///
    /// The default identifier is a SHA-256 of the content, computed lazily on
    /// first use (and only when a session store needs a key). For large
    /// buffers where the caller already has a stable id, prefer
    /// [`Self::from_bytes_with_identifier`].
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: Arc::new(BytesSource {
                bytes: Arc::new(bytes.into()),
                identifier: OnceLock::new(),
                explicit_identifier: None,
            }),
        }
    }

    /// Upload an in-memory buffer with a caller-supplied stable identifier.
    ///
    /// Use this when the caller owns an id that survives restarts (a document
    /// id, a database row id, etc.); it avoids hashing the whole buffer.
    pub fn from_bytes_with_identifier(
        bytes: impl Into<Vec<u8>>,
        identifier: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::new(BytesSource {
                bytes: Arc::new(bytes.into()),
                identifier: OnceLock::new(),
                explicit_identifier: Some(identifier.into()),
            }),
        }
    }

    /// Build a source from a platform reader. `identifier` must stay stable
    /// across restarts for resumable uploads (use a content URI, not a cache
    /// path). `display` is only used for logs.
    pub fn from_reader(
        size: u64,
        identifier: impl Into<String>,
        display: impl Into<String>,
        opener: RangeOpener,
    ) -> Self {
        Self {
            inner: Arc::new(ReaderSource {
                size,
                identifier: identifier.into(),
                display: display.into(),
                opener,
            }),
        }
    }

    /// Android `content://` URI, read directly through `ContentResolver`.
    ///
    /// The descriptor is re-opened for each chunk; MediaStore providers return
    /// seekable descriptors, so no cache copy is made.
    #[cfg(target_os = "android")]
    pub fn from_content_uri(uri: impl Into<String>, size: u64) -> Result<Self> {
        let uri = uri.into();
        let identifier = format!("content:{uri}:{size}");
        let display = uri.clone();
        let opener: RangeOpener = Arc::new(move |offset, len| {
            let uri = uri.clone();
            Box::pin(async move {
                crate::uploader::content_uri::view(&uri, offset, len).await
            })
        });
        Ok(Self {
            inner: Arc::new(ReaderSource {
                size,
                identifier,
                display,
                opener,
            }),
        })
    }

    pub fn size(&self) -> u64 {
        self.inner.size()
    }

    pub fn identifier(&self) -> &str {
        self.inner.identifier()
    }

    pub fn display(&self) -> &str {
        self.inner.display()
    }

    // TODO: this is the per-chunk range view; consider a `RangeView` that keeps
    // offset/len instead of erasing them behind the boxed `SourceReader`.
    pub(crate) async fn view(&self, offset: u64, len: u64) -> Result<SourceReader> {
        if offset.checked_add(len).is_none_or(|end| end > self.size()) {
            bail!(
                "upload range {offset}+{len} exceeds source size {}",
                self.size()
            );
        }
        self.inner.view(offset, len).await
    }
}

struct PathSource {
    path: PathBuf,
    display: String,
    size: u64,
    identifier: String,
}

impl SourceImpl for PathSource {
    fn size(&self) -> u64 {
        self.size
    }

    fn identifier(&self) -> &str {
        &self.identifier
    }

    fn display(&self) -> &str {
        &self.display
    }

    fn view(&self, offset: u64, len: u64) -> BoxFuture<'_, Result<SourceReader>> {
        Box::pin(async move {
            let file = File::open(&self.path)
                .await
                .with_context(|| format!("failed to open {}", self.path.display()))?;
            let mut reader = BufReader::with_capacity(64 * 1024, file);
            reader.seek(SeekFrom::Start(offset)).await?;
            Ok(Box::new(reader.take(len)) as SourceReader)
        })
    }
}

struct BytesSource {
    bytes: Arc<Vec<u8>>,
    identifier: OnceLock<String>,
    explicit_identifier: Option<String>,
}

impl BytesSource {
    fn identifier(&self) -> &str {
        if let Some(identifier) = &self.explicit_identifier {
            return identifier;
        }
        self.identifier.get_or_init(|| {
            let mut hasher = Sha256::new();
            hasher.update(self.bytes.as_slice());
            format!("bytes:{:x}", hasher.finalize())
        })
    }
}

impl SourceImpl for BytesSource {
    fn size(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn identifier(&self) -> &str {
        self.identifier()
    }

    fn display(&self) -> &str {
        self.explicit_identifier
            .as_deref()
            .unwrap_or("inline-buffer")
    }

    fn view(&self, offset: u64, len: u64) -> BoxFuture<'_, Result<SourceReader>> {
        Box::pin(async move {
            let start = offset as usize;
            let end = (offset + len) as usize;
            let slice = self.bytes[start..end].to_vec();
            Ok(Box::new(Cursor::new(slice)) as SourceReader)
        })
    }
}

struct ReaderSource {
    size: u64,
    identifier: String,
    display: String,
    opener: RangeOpener,
}

impl SourceImpl for ReaderSource {
    fn size(&self) -> u64 {
        self.size
    }

    fn identifier(&self) -> &str {
        &self.identifier
    }

    fn display(&self) -> &str {
        &self.display
    }

    fn view(&self, offset: u64, len: u64) -> BoxFuture<'_, Result<SourceReader>> {
        (self.opener)(offset, len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn bytes_source_reads_ranges() {
        let source = UploadSource::from_bytes(b"0123456789".to_vec());
        assert_eq!(source.size(), 10);
        let mut reader = source.view(2, 4).await.unwrap();
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"2345");
    }

    #[tokio::test]
    async fn path_source_reads_ranges_and_rejects_overflow() {
        let path = std::env::temp_dir().join(format!("source-range-{}", std::process::id()));
        std::fs::write(&path, b"abcdefgh").unwrap();
        let source = UploadSource::from_path(&path).unwrap();
        let mut reader = source.view(3, 3).await.unwrap();
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"def");
        assert!(source.view(7, 2).await.is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn bytes_identifier_is_stable_and_can_be_explicit() {
        let source = UploadSource::from_bytes(b"hello".to_vec());
        let first = source.identifier().to_string();
        assert_eq!(first, source.identifier());
        assert!(first.starts_with("bytes:"));

        let explicit = UploadSource::from_bytes_with_identifier(b"hello".to_vec(), "doc-42");
        assert_eq!(explicit.identifier(), "doc-42");
        assert_eq!(explicit.display(), "doc-42");
    }
}
