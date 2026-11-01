//! High-level file uploads for Cloudreve clients.
//!
//! The crate owns upload-session creation, provider-specific chunk transfer,
//! encryption, retries, cancellation, and progress reporting. Callers only
//! need a configured Cloudreve API client and an [`UploadParams`] value.

// Keep the official uploader sources mergeable while the published SDK crate
// uses its new package name.
extern crate cloudreve_sdk_api as cloudreve_api;

#[rustfmt::skip]
mod uploader;

#[cfg(feature = "session-store-fs")]
pub use uploader::FileSessionStore;
pub use uploader::{
    ProgressCallback, ProgressUpdate, RangeOpener, SessionKey, SessionStore, SourceReader,
    UploadError, UploadIdentity, UploadParams, UploadResult, UploadSession, UploadSource, Uploader,
    UploaderConfig,
};
