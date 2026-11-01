//! Chunk transport: read one byte range and stream it to the provider.
//!
//! This module knows nothing about progress, sessions or persistence. It turns
//! a chunk into an HTTP body, retries what is worth retrying, and returns the
//! provider's ETag. The upload stream ([`super::transfer`]) decides what
//! happens next.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use cloudreve_api::Client as CrClient;
use futures::Stream;
use reqwest::Client as HttpClient;
use tokio::io::{AsyncRead, ReadBuf};
use tokio_util::io::ReaderStream;
use tracing::{debug, error, warn};

use crate::uploader::UploaderConfig;
use crate::uploader::encrypt::EncryptionConfig;
use crate::uploader::error::{self, UploadError};
use crate::uploader::providers::{self, PolicyType};
use crate::uploader::session::UploadSession;
use crate::uploader::source::{SourceReader, UploadSource};

/// Metadata about a single chunk (without the data)
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    /// Chunk index
    pub index: usize,
    /// Expected chunk size
    pub size: u64,
    /// Byte offset in file
    pub offset: u64,
}

impl ChunkInfo {
    /// Create new chunk info
    pub fn new(index: usize, offset: u64, size: u64) -> Self {
        Self {
            index,
            offset,
            size,
        }
    }

    /// Describe chunk `index` of `session`.
    pub fn of(session: &UploadSession, index: usize) -> Self {
        let (offset, _end) = session.chunk_range(index);
        Self::new(index, offset, session.chunk_size_for(index))
    }
}

/// Buffer size for streaming reads (64KB)
const STREAM_BUFFER_SIZE: usize = 64 * 1024;

/// A limited async reader that reads only one chunk range, optionally applying
/// encryption on-the-fly. The source has already been range-limited.
pub struct ChunkReader {
    reader: SourceReader,
    encryption: Option<EncryptionConfig>,
    start_offset: u64,
    position: u64,
    remaining: u64,
}

impl ChunkReader {
    pub fn new(
        reader: SourceReader,
        offset: u64,
        size: u64,
        encryption: Option<EncryptionConfig>,
    ) -> Self {
        Self {
            reader,
            encryption,
            start_offset: offset,
            position: 0,
            remaining: size,
        }
    }

    /// Get the total size of this chunk
    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.position + self.remaining
    }
}

impl AsyncRead for ChunkReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        // Limit read to remaining bytes
        let max_read = (self.remaining as usize).min(buf.remaining());
        let mut limited_buf = buf.take(max_read);
        let before = limited_buf.filled().len();

        let reader = Pin::new(&mut self.reader);

        match reader.poll_read(cx, &mut limited_buf) {
            Poll::Ready(Ok(())) => {
                let bytes_read = limited_buf.filled().len() - before;
                if bytes_read == 0 {
                    // EOF reached
                    return Poll::Ready(Ok(()));
                }

                // Apply encryption if configured
                if let Some(ref config) = self.encryption {
                    let file_offset = self.start_offset + self.position;
                    // Get the newly read bytes and encrypt them in place
                    let start = buf.filled().len();
                    unsafe {
                        buf.assume_init(bytes_read);
                    }
                    buf.advance(bytes_read);
                    let filled = buf.filled_mut();
                    let encrypted_slice = &mut filled[start..start + bytes_read];
                    config.encrypt_at_offset(encrypted_slice, file_offset);
                } else {
                    unsafe {
                        buf.assume_init(bytes_read);
                    }
                    buf.advance(bytes_read);
                }

                self.position += bytes_read as u64;
                self.remaining -= bytes_read as u64;

                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A stream that yields chunks of bytes from a ChunkReader.
/// Uses tokio_util's ReaderStream internally for simplicity.
pub struct ChunkStream {
    inner: ReaderStream<ChunkReader>,
}

impl ChunkStream {
    /// Create a new chunk stream from a reader
    pub fn new(reader: ChunkReader) -> Self {
        Self {
            inner: ReaderStream::with_capacity(reader, STREAM_BUFFER_SIZE),
        }
    }

    /// Create a chunk stream from an upload source and chunk info.
    pub async fn from_source(
        source: &UploadSource,
        chunk: &ChunkInfo,
        encryption: Option<EncryptionConfig>,
    ) -> Result<Self> {
        let reader = source.view(chunk.offset, chunk.size).await?;
        Ok(Self::new(ChunkReader::new(
            reader,
            chunk.offset,
            chunk.size,
            encryption,
        )))
    }
}

impl Stream for ChunkStream {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Upload one chunk, retrying failures that are worth retrying.
///
/// The future owns the request, so dropping it (which is what dropping the
/// upload stream does) cancels the transfer.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn upload_chunk_with_retry(
    http_client: &HttpClient,
    cr_client: &Arc<CrClient>,
    policy_type: PolicyType,
    config: &UploaderConfig,
    source: &UploadSource,
    chunk: &ChunkInfo,
    encryption: Option<EncryptionConfig>,
    session: &UploadSession,
) -> Result<Option<String>> {
    for attempt in 0..=config.max_retries {
        if attempt > 0 {
            let base = config.retry_base_delay.as_millis() as u64;
            let delay_ms = base * (1 << attempt.min(10));
            let delay = Duration::from_millis(delay_ms).min(config.retry_max_delay);

            debug!(
                target: "uploader::chunk",
                chunk = chunk.index,
                attempt,
                delay_ms = delay.as_millis(),
                "Retrying chunk upload"
            );

            tokio::time::sleep(delay).await;
        }

        // A fresh stream per attempt: the previous body is spent.
        let stream = ChunkStream::from_source(source, chunk, encryption.clone())
            .await
            .map_err(|error| UploadError::FileReadError(format!("Failed to create stream: {error}")))?;

        match providers::upload_chunk(http_client, cr_client, policy_type, chunk, stream, session)
            .await
        {
            Ok(etag) => {
                debug!(
                    target: "uploader::chunk",
                    chunk = chunk.index,
                    etag = ?etag,
                    "Chunk uploaded successfully"
                );
                return Ok(etag);
            }
            Err(error) => {
                // The server answered and rejected the request: another
                // attempt cannot help, and the session may be gone for good.
                if attempt == config.max_retries || error::is_remote_session_lost(&error) {
                    error!(
                        target: "uploader::chunk",
                        chunk = chunk.index,
                        error = ?error,
                        attempt,
                        "Chunk upload failed"
                    );
                    return Err(error);
                }
                warn!(
                    target: "uploader::chunk",
                    chunk = chunk.index,
                    error = ?error,
                    attempt,
                    "Chunk upload failed, will retry"
                );
            }
        }
    }

    // The loop always returns on the last attempt; this keeps the signature
    // total without an unreachable arm inside it.
    Err(anyhow::anyhow!(
        "chunk {} upload failed, max retries exceeded",
        chunk.index
    ))
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
    fn chunk_info_follows_the_session_geometry() {
        let session = session(12, 5);

        assert_eq!(ChunkInfo::of(&session, 0).offset, 0);
        assert_eq!(ChunkInfo::of(&session, 0).size, 5);
        assert_eq!(ChunkInfo::of(&session, 2).offset, 10);
        assert_eq!(ChunkInfo::of(&session, 2).size, 2);
    }
}
