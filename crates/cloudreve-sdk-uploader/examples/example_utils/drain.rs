//! Drive an upload stream to its end.

use anyhow::{Result, bail};
use cloudreve_sdk_uploader::{UploadError, UploadEvent, UploadStream};
use futures::StreamExt;

/// How an upload ended.
pub enum Outcome {
    /// Every chunk is uploaded and the server knows the file.
    Completed,
    /// The upload stopped early; what was recorded can be resumed.
    Failed(UploadError),
}

/// Consume `stream`, handing every `ChunkDone` event to `on_chunk`.
///
/// Both examples need the same loop: one place that feeds the progress tracker
/// and the journal, and one place that decides how the upload ended. Returning an
/// error from `on_chunk` drops the stream, which cancels the upload.
pub async fn drain<F>(stream: UploadStream, mut on_chunk: F) -> Result<Outcome>
where
    F: FnMut(&UploadEvent) -> Result<()>,
{
    let mut events = stream.events;
    while let Some(event) = events.next().await {
        match event {
            UploadEvent::Completed => return Ok(Outcome::Completed),
            UploadEvent::Failed(error) => return Ok(Outcome::Failed(error)),
            event @ UploadEvent::ChunkDone(_) => on_chunk(&event)?,
        }
    }

    bail!("upload stream ended without a terminal event")
}
