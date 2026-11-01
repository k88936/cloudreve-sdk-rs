# Cloudreve SDK Uploader

Provider-aware, chunked file uploads for Cloudreve CLI clients.

Add both SDK crates to your project:

```toml
[dependencies]
cloudreve-sdk-api = "0.1"
cloudreve-sdk-uploader = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

An upload is a stream of events. `upload()` creates the session and hands back the session
plus the events the upload produces; nothing is transferred until the stream is polled, and
dropping the stream cancels the upload.

```rust
let stream = uploader.upload(params).await?;
let mut events = stream.events;

while let Some(event) = events.next().await {
    match event {
        UploadEvent::ChunkDone(_) => {}   // one chunk finished; also the line a log writes
        UploadEvent::Completed => break,  // or UploadEvent::Failed(error)
        UploadEvent::Failed(error) => return Err(error.into()),
    }
}
```

Progress is a fold over those events: the `Tracker` (behind the default-on `progress`
feature) borrows each event and yields a `ProgressUpdate`, so a single loop can print
progress and persist the finished chunks at the same time.

The runnable example logs in, creates an uploader, and uploads one file while printing
progress. The remote argument must be a complete Cloudreve file URI, including the
destination filename.

```bash
export CLOUDREVE_SERVER="https://cloudreve.example"
export CLOUDREVE_EMAIL="user@example.com"
export CLOUDREVE_PASSWORD="your-password"

cargo run -p cloudreve-sdk-uploader --example upload_file -- \
  ./photo.jpg cloudreve://my/Uploads/photo.jpg
```

See [`examples/upload_file.rs`](examples/upload_file.rs) for the complete code.

`examples/resume_upload.rs` shows the other half: an upload that is interrupted on purpose,
a journal of its finished chunks, and `Uploader::try_resume` picking it back up without
re-sending them.
