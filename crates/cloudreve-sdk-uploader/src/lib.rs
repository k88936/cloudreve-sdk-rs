//! High-level file uploads for Cloudreve clients.
//!
//! The crate owns upload-session creation, provider-specific chunk transfer,
//! encryption, retries and the event stream an upload produces. Callers only
//! need a configured Cloudreve API client and an [`UploadParams`] value.
//!
//! ```ignore
//! let stream = uploader.upload(params).await?;
//! let mut events = stream.events;
//! while let Some(event) = events.next().await {
//!     match event {
//!         UploadEvent::ChunkDone(_) => {}
//!         UploadEvent::Completed => break,
//!         UploadEvent::Failed(error) => return Err(error.into()),
//!     }
//! }
//! ```

// Keep the official uploader sources mergeable while the published SDK crate
// uses its new package name.
extern crate cloudreve_sdk_api as cloudreve_api;

#[rustfmt::skip]
mod uploader;

pub use uploader::{
    ChunkProgress, RangeOpener, SourceReader, UploadError, UploadEvent, UploadParams, UploadResult,
    UploadSession, UploadSource, UploadStream, Uploader, UploaderConfig,
};
#[cfg(feature = "progress")]
pub use uploader::{ProgressUpdate, Tracker};
