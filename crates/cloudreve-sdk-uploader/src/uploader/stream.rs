//! The upload as a stream of events.
//!
//! [`UploadStream`] pairs the session that was created with the events the
//! upload produces. The events are the only source of truth: a durable log, a
//! progress bar and a resume decision are all folds over the same stream.

use std::pin::Pin;

use futures::Stream;

use crate::uploader::chunk_progress::ChunkProgress;
use crate::uploader::error::UploadError;
use crate::uploader::session::UploadSession;

/// One upload, driven by consuming [`UploadStream::events`].
pub struct UploadStream {
    /// The session as created (or as restored). It is a snapshot: it does not
    /// change while the stream runs.
    pub session: UploadSession,
    /// Events of this upload. Nothing happens until this is polled.
    pub events: Pin<Box<dyn Stream<Item = UploadEvent> + Send>>,
}

impl std::fmt::Debug for UploadStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadStream")
            .field("session_id", &self.session.session_id())
            .finish_non_exhaustive()
    }
}

/// What an upload reports while it runs.
#[derive(Debug)]
pub enum UploadEvent {
    /// One chunk finished. This is also the record a durable log writes, so
    /// "save what happened" needs no extra API.
    ChunkDone(ChunkProgress),
    /// Terminal: every chunk is uploaded and the provider accepted the upload.
    Completed,
    /// Terminal: the upload failed. The stream ends right after this item.
    Failed(UploadError),
}

impl UploadEvent {
    /// Whether this event ends the stream.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed(_))
    }
}
