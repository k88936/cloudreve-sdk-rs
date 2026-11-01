# Uploader redesign: the upload *is* an event stream

Status: agreed design; implemented by the commits that follow this document.

## Motivation

The `SessionStore` abstraction had exactly one real implementation, yet it shaped the
whole uploader: the upload path had to read a snapshot before starting and rewrite the
whole snapshot after every chunk, every backend had to implement `prune`, and the
caller had two ways to configure the same thing (`session_store` and `session_dir`).

Reviewing the concrete consequences:

- Every completed chunk rewrote the full snapshot. With concurrent workers each worker
  cloned its own session and wrote the file, so the last writer could drop a chunk
  record written by another worker (a duplicate upload, not data loss, but still wrong).
- Write amplification is `O(completed_chunks)` per chunk.
- `prune` had no production caller; only `Uploader::prune_sessions` forwarded to it, and
  that had no caller either.
- Progress could only be *observed* through `ProgressCallback`; it could not be composed,
  filtered, or folded into anything else.

So: remove the store abstraction, let the upload itself produce the durable records, and
make persistence an ordinary consumer.

## Core idea

`upload()` creates the session and returns `{ session, events }`. The event stream is the
only source of truth for both UI progress and the durable log; persistence is a consumer,
not a dependency. `ChunkDone(ChunkProgress)` is simultaneously the progress event and the
log line, so "save what happened" needs no extra API.

## Public API

```rust
pub struct UploadStream {
    pub session: UploadSession,
    pub events: Pin<Box<dyn Stream<Item = UploadEvent> + Send>>,
}

pub enum UploadEvent {
    /// One chunk finished. Same record a durable log writes.
    ChunkDone(ChunkProgress),
    /// Terminal: the upload finished and was completed server-side.
    Completed,
    /// Terminal: the upload failed. The stream ends right after this item.
    Failed(UploadError),
}

impl Uploader {
    pub async fn upload(&self, params: UploadParams) -> Result<UploadStream>;

    /// Continue an existing session, skipping the chunks already recorded.
    pub async fn try_resume(
        &self,
        session: UploadSession,
        source: UploadSource,
        completed: impl IntoIterator<Item = ChunkProgress>,
    ) -> Result<UploadStream>;
}
```

`ProgressCallback`, `NoOpProgress` and `FnProgress` are removed. `ProgressUpdate` and the
new `Tracker` live behind a default-on `progress` feature:

```rust
#[cfg(feature = "progress")]
pub struct ProgressUpdate {
    pub total_size: u64,
    pub uploaded: u64,
    pub progress: f64,
    pub speed_bytes_per_sec: u64,
    pub eta_seconds: Option<u64>,
    pub total_chunks: usize,
    pub completed_chunks: usize,
    // `concurrent_chunks` is removed: chunk events cannot derive it.
}

#[cfg(feature = "progress")]
pub struct Tracker { /* ... */ }

#[cfg(feature = "progress")]
impl Tracker {
    /// Seed from the session and the chunks already recorded in a log.
    pub fn new(session: &UploadSession, completed: &[ChunkProgress]) -> Self;

    /// Fold one event. Returns `Some` only for `ChunkDone`.
    pub fn observe(&mut self, event: &UploadEvent) -> Option<ProgressUpdate>;
}
```

`Tracker` is a state machine, not a stream combinator: it borrows the event instead of
consuming it, so one loop can feed the journal, the UI and anything else without
buffering, teeing, or back-pressure. Speed and ETA are computed from event arrival times
over a sliding window. `Tracker::new` folds the surviving completion records (last record
per index wins, so retried chunks are not counted twice) and `current()` renders the seed
state before the first event arrives.

### `UploadSession` becomes a pure DTO

```rust
pub struct UploadSession {
    pub source_id: String,
    pub remote_uri: String,
    pub file_size: u64,
    pub chunk_size: u64,
    pub encrypt_metadata: Option<EncryptMetadata>,
    pub expires_at: i64,
    policy_type: PolicyType,
    relay: bool,
    credential: UploadCredential,
}

impl UploadSession {
    pub fn new(
        source_id: String,
        remote_uri: String,
        file_size: u64,
        credential: UploadCredential,
    ) -> Self;
}
```

Removed: `id`, `task_id`, `drive_id`, `created_at`, `updated_at`, `chunk_progress` and
every mutating method (`complete_chunk`, `update_chunk_progress`). Progress is no longer
part of the session; `ChunkProgress` becomes a *completion record* rather than an array
slot. The session has no `id` field either: `credential.session_id` is the identity and
`session_id()` already reads it, so keeping a second copy would only allow the two to
disagree.

Read-only derivations stay: `session_id()`, `credential()`, `policy_type()`, `is_relay()`,
`is_expired()`, `num_chunks()`, `chunk_size_for()`, `chunk_range()`, `chunk_concurrency()`,
`upload_url*()`, `complete_url()`, `callback_secret()`.

## Semantics

- **Lazy.** Nothing runs until the stream is polled. The first poll starts the chunk
  workers; `create_session` has already happened because `upload()` is `async`.
- **Concurrent, but inside the stream.** Chunk futures live in a `FuturesUnordered` in the
  stream's state, bounded by the policy-derived concurrency. No `tokio::spawn`, no channel.
- **Drop = cancel.** Dropping the stream drops the in-flight chunk futures. There is no
  background mode and no cancellation token; `Uploader::with_cancel_token` is removed.
- **Exactly one terminal event.** Every stream ends with `Completed` or `Failed`. A stream
  that ends without either (panic, abort) is an abnormal end and the consumer must treat it
  as an error.
- **Error split.** Failures before the first poll (session creation) are `Err` from
  `upload()`/`try_resume()`. Everything afterwards arrives as `Failed(UploadError)`.
- **No automatic resume.** `upload()` always creates a new session. Resuming is an explicit
  `try_resume()` call, normally fed by a log the caller just read.
- **`SessionExpired` is reported, not repaired.** When the server no longer knows the
  session, `try_resume()` (or the stream) surfaces `UploadError::SessionExpired`; the caller
  owns the log and deletes it. `error::is_remote_session_lost` still does the detection.
- **No source validation.** The caller must pass the bytes that produced the recorded
  chunks. Nothing compares sizes, mtimes or content. See Risks. `try_resume` only warns
  when the source size differs from the session, it never refuses.
- **A record means done.** Completion is the presence of a record for that index, not a
  byte count, so an empty chunk (empty file, or a policy chunk size of zero) is resumable
  too. `loaded` is the chunk's size, kept for logs and diagnostics.
- **Incomplete input is tolerated.** `try_resume` drops records whose index is outside the
  session's chunk range and keeps the last record per index, so a log that repeats a retry
  or names an impossible index still resumes instead of failing.
- **Provider completion sees sorted records.** S3-like completion XML needs ascending part
  numbers, so the accumulated records are kept in index order.

## Persistence (example code, not the library)

The library ships serializable data types only. The reference journal lives in
`examples/resume_upload.rs`:

```
<dir>/<session_id>.jsonl
  line 1   : StoredSession { remote_uri, source_id, file_size, credential }
  line 2..N: ChunkProgress { index, loaded, etag }
```

- `StoredSession` is private to the example. Everything derived (`chunk_size`,
  `policy_type`, `relay`, `expires_at`) is recomputed by `UploadSession::new`, so the first
  line stays minimal.
- Appending is the only write. A resumed upload appends to the **same** file; when reading,
  the last record for a given index wins (a retry simply appears twice).
- `finish()` removes the file in one `unlink`; no tombstone, no compaction.
- There is no format version: a log that fails to parse is treated as unusable and deleted,
  and the upload starts over.

Read side (example code):

```rust
let (session, completed) = journal.load(path)?; // DTO + completion records
let stream = uploader.try_resume(session, source, completed).await?;
```

Consume side:

```rust
let mut tracker = Tracker::new(&stream.session, &completed);
let mut events = stream.events;
while let Some(event) = events.next().await {
    if let Some(update) = tracker.observe(&event) {
        ui.render(update);
    }
    match event {
        UploadEvent::ChunkDone(chunk) => journal.append(&chunk)?,
        UploadEvent::Completed => journal.finish()?,
        UploadEvent::Failed(e) if is_session_expired(&e) => journal.remove()?,
        UploadEvent::Failed(_) => {} // keep the log for the next resume
    }
}
```

`UploadStream.session` is the session as created (or as restored) and is not mutated while
the stream runs; a caller that wants the final `chunk_progress` equivalent folds
`ChunkDone` records itself (or just asks the log it is writing anyway).

## Deletions

Files:

- `uploader/session_store.rs`
- `uploader/session_store_fs.rs`
- `uploader/snapshot.rs`

Types and functions:

- `SessionStore`, `FileSessionStore`, `UploadIdentity`, `SessionKey`
- `Snapshot`, `SNAPSHOT_VERSION`, `encode_snapshot`, `decode_snapshot`
- `ProgressCallback`, `NoOpProgress`, `FnProgress`
- `chunk::SessionPersistence`

Members:

- `UploaderConfig::{session_store, session_dir}`, `UploaderConfig::{with_session_store, with_session_dir}`
- `Uploader::{session_store, with_cancel_token, prune_sessions}`

Features:

- remove `session-store-fs`
- add `progress` (default on)

`UploadSession`: the fields and methods listed above.

## Module layout after

```
uploader/
  mod.rs            Uploader, UploadParams, UploaderConfig, session creation/completion
  session.rs        UploadSession DTO + read-only derivations
  chunk_progress.rs ChunkProgress record + CompletionSet (dedupe, lookup, totals)
  chunk.rs          chunk transfer: reader, stream, retry loop, ChunkUploader
  transfer.rs       ChunkTransfer: the FuturesUnordered stream driver
  stream.rs         UploadStream + UploadEvent
  progress.rs       ProgressUpdate + Tracker (feature = "progress")
  source.rs         unchanged
  error.rs          unchanged (keeps `is_remote_session_lost`, `SessionExpired`)
  providers/        completion takes `&[ChunkProgress]` instead of reading the session
```

`transfer.rs` takes the chunk upload and the finalization as boxed callbacks, so the
driver can be tested with fakes instead of HTTP clients.

## Examples and e2e

- `examples/resume_upload.rs`: rewritten from scratch as a single-process all-in-one run:
  create → write the log → consume until an injected chunk failure → read the log back →
  `try_resume` → verify the downloaded hash → assert the log was removed. No
  `CLOUDREVE_RESUME_PHASE` branch.
- `examples/upload_file.rs`: minimal usage — `upload()`, consume events, `Tracker` prints.
- `scripts/e2e_podman.py`: one invocation instead of three. Cross-process resume is no
  longer covered by automation; this was an explicit trade-off.

## Tests

- `CompletionSet`: dedupes retries (last wins), drops out-of-range indices, keeps index
  order, sums uploaded bytes.
- `Tracker`: seeds from completion records, emits on `ChunkDone`, reaches 100% at the end,
  ignores terminal events.
- Stream driver (with fake upload/finalize callbacks): exactly `num_chunks` `ChunkDone`
  items then exactly one `Completed`; an injected chunk error produces one `Failed` and no
  further items; a resumed session that fails before any new chunk reports `SessionExpired`.
- `try_resume`: an expired session is refused before anything is polled; recorded chunks are
  never handed to the upload callback.
- `upload`: an unreachable API endpoint fails before a stream is returned.
- Example level: journal round-trip inside one process (podman run).

## Decisions (previously open questions)

1. `try_resume` takes `impl IntoIterator<Item = ChunkProgress>`: a `Vec` from a log reader,
   a filtered iterator and an array all work without a wrapper type.
2. Chunk geometry stays on `UploadSession` (`num_chunks`, `chunk_size_for`, `chunk_range`).
   "Which chunks are left" and "how many bytes are done" live in the private
   `CompletionSet`, and `Tracker` owns the uploaded/completed numbers it exposes.
3. `ChunkProgress` keeps its name: it is public, it names one chunk's progress, and renaming
   it would churn every caller for no semantic gain.
4. `UploadSession::id` is removed; `session_id()` still returns `credential.session_id`.
5. In-flight (intra-chunk) bytes are simply gone, so there is nothing extra to gate: the
   `progress` feature gates `ProgressUpdate` and `Tracker` only.

## Risks and accepted trade-offs

- **No source validation.** A replaced, truncated or different file passed to `try_resume`
  produces a server-side file assembled from mismatched chunks. The old `SessionKey` hash
  prevented this; removing it moves the responsibility to the caller and must be documented
  prominently.
- **No log version.** Any field change orphans existing logs; the example can only discard
  them and re-upload.
- **Drop = cancel.** There is no fire-and-forget upload; callers who want one must own the
  driving task themselves.
- **Progress granularity is per chunk.** In-flight bytes are no longer observable, so a
  single-chunk upload (small file, large policy chunk size) shows one step.
