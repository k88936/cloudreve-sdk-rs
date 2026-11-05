//! Drives the chunk uploads of one session as a stream of events.
//!
//! The driver is a small state machine:
//!
//! 1. keep up to `concurrency` chunk futures in a [`FuturesUnordered`];
//! 2. yield one [`UploadEvent::ChunkDone`] per finished chunk;
//! 3. once the queue is empty, run the provider finalization;
//! 4. yield exactly one terminal event and end.
//!
//! The chunk upload itself and the finalization are injected as callbacks, so
//! the driver can be tested without HTTP clients, and so the chunk layer stays
//! free of stream concerns.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::{Future, Stream, StreamExt};
use tracing::{debug, error, info, warn};

use crate::uploader::chunk::ChunkInfo;
use crate::uploader::chunk_progress::{ChunkProgress, CompletionSet};
use crate::uploader::error::{self, UploadError};
use crate::uploader::session::UploadSession;
use crate::uploader::stream::UploadEvent;

/// One chunk upload in flight: the chunk index and its result.
pub(crate) type ChunkFuture =
    Pin<Box<dyn Future<Output = (usize, Result<Option<String>>)> + Send>>;

/// Starts the upload of one chunk.
pub(crate) type ChunkStarter = Box<dyn Fn(ChunkInfo) -> ChunkFuture + Send + Sync>;

/// Finalizes the session with the records of every finished chunk, in index
/// order.
pub(crate) type Finalizer = Box<
    dyn FnOnce(Vec<ChunkProgress>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send,
>;

/// Where the transfer is in its life, independent of the chunks in flight.
enum Phase {
    Uploading,
    Completing(Pin<Box<dyn Future<Output = Result<()>> + Send>>),
    Done,
}

/// The [`Stream`] behind [`super::UploadStream::events`].
pub(crate) struct ChunkTransfer {
    session: Arc<UploadSession>,
    /// Chunks this transfer still has to start, in index order.
    pending: VecDeque<ChunkInfo>,
    /// Chunks known to be done: replayed records plus this transfer's.
    completed: CompletionSet,
    /// Chunks finished by *this* transfer, as opposed to replayed ones.
    produced: usize,
    /// Whether this transfer continues an earlier attempt.
    resumed: bool,
    concurrency: usize,
    inflight: FuturesUnordered<ChunkFuture>,
    phase: Phase,
    start_chunk: ChunkStarter,
    finalize: Option<Finalizer>,
}

impl ChunkTransfer {
    /// Build a transfer for `session`, skipping the chunks in `completed`.
    pub(crate) fn new(
        session: UploadSession,
        completed: Vec<ChunkProgress>,
        resumed: bool,
        start_chunk: ChunkStarter,
        finalize: Finalizer,
    ) -> Self {
        let num_chunks = session.num_chunks();
        let completed = CompletionSet::new(num_chunks, completed);
        let pending: VecDeque<ChunkInfo> = (0..num_chunks)
            .filter(|index| !completed.contains(*index))
            .map(|index| ChunkInfo::of(&session, index))
            .collect();
        let concurrency = session.chunk_concurrency();

        Self {
            session: Arc::new(session),
            pending,
            completed,
            produced: 0,
            resumed,
            concurrency,
            inflight: FuturesUnordered::new(),
            phase: Phase::Uploading,
            start_chunk,
            finalize: Some(finalize),
        }
    }

    /// Turn a chunk failure into the error the caller sees.
    fn failure(&self, index: usize, error: anyhow::Error) -> UploadError {
        // A resumed session is only as durable as the server's copy of it.
        // When the server rejects the very first chunk of the attempt, the
        // records the caller replayed are worthless: say so instead of
        // reporting a chunk error the caller can only interpret by matching on
        // Cloudreve's error codes.
        if self.resumed && self.produced == 0 && error::is_remote_session_lost(&error) {
            warn!(
                target: "uploader",
                session_id = %self.session.session_id(),
                "The server no longer knows this upload session"
            );
            return UploadError::SessionExpired;
        }

        match error.downcast::<UploadError>() {
            Ok(error) => error,
            Err(error) => UploadError::chunk_failed(index, format!("{error:#}")),
        }
    }

    /// Record a finished chunk and describe it as an event.
    fn record(&mut self, index: usize, etag: Option<String>) -> UploadEvent {
        let record = ChunkProgress::new(index, self.session.chunk_size_for(index), etag);
        self.completed.insert(record.clone());
        self.produced += 1;
        debug!(
            target: "uploader",
            chunk = index,
            "Chunk uploaded"
        );
        UploadEvent::ChunkDone(record)
    }
}

impl Stream for ChunkTransfer {
    type Item = UploadEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.phase {
                Phase::Done => return Poll::Ready(None),
                Phase::Completing(future) => {
                    let outcome = std::task::ready!(future.as_mut().poll(cx));
                    this.phase = Phase::Done;
                    return Poll::Ready(Some(match outcome {
                        Ok(()) => {
                            info!(
                                target: "uploader",
                                session_id = %this.session.session_id(),
                                "Upload completed"
                            );
                            UploadEvent::Completed
                        }
                        Err(error) => {
                            error!(
                                target: "uploader",
                                session_id = %this.session.session_id(),
                                error = %error,
                                "Upload finalization failed"
                            );
                            UploadEvent::Failed(UploadError::from(error))
                        }
                    }));
                }
                Phase::Uploading => {}
            }

            // Keep the pipeline full; each started chunk takes a future slot.
            while this.inflight.len() < this.concurrency {
                let Some(chunk) = this.pending.pop_front() else {
                    break;
                };
                debug!(
                    target: "uploader",
                    chunk = chunk.index,
                    "Starting chunk upload"
                );
                this.inflight.push((this.start_chunk)(chunk));
            }

            if this.inflight.is_empty() {
                // Every chunk finished: hand the records to the provider.
                let Some(finalize) = this.finalize.take() else {
                    this.phase = Phase::Done;
                    return Poll::Ready(None);
                };
                let records = this.completed.clone().into_vec();
                this.phase = Phase::Completing(finalize(records));
                continue;
            }

            match this.inflight.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => continue,
                Poll::Ready(Some((index, Ok(etag)))) => {
                    let event = this.record(index, etag);
                    return Poll::Ready(Some(event));
                }
                Poll::Ready(Some((index, Err(error)))) => {
                    let failure = this.failure(index, error);
                    this.phase = Phase::Done;
                    error!(
                        target: "uploader",
                        chunk = index,
                        error = %failure,
                        "Upload failed"
                    );
                    return Poll::Ready(Some(UploadEvent::Failed(failure)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudreve_api::models::explorer::UploadCredential;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn session(num_chunks: usize) -> UploadSession {
        UploadSession::new(
            num_chunks as u64 * 5,
            UploadCredential {
                session_id: "session-1".into(),
                expires: 4_000_000_000,
                chunk_size: 5,
                ..Default::default()
            },
        )
    }

    fn record(index: usize) -> ChunkProgress {
        ChunkProgress::new(index, 5, Some(format!("etag-{index}")))
    }

    /// The kind of error the server answers with when the session is gone.
    fn rejection_error() -> cloudreve_api::ApiError {
        cloudreve_api::ApiError::ApiError {
            code: 40004,
            message: "upload session not found".into(),
            error_detail: None,
            correlation_id: None,
            aggregated_errors: None,
        }
    }

    /// A starter that finishes immediately, failing on `failing` if given, and
    /// appending every started index to `started`.
    fn starter(failing: Option<usize>, started: Arc<Mutex<Vec<usize>>>) -> ChunkStarter {
        Box::new(move |chunk: ChunkInfo| {
            let index = chunk.index;
            let started = Arc::clone(&started);
            Box::pin(async move {
                started.lock().unwrap().push(index);
                if Some(index) == failing {
                    return (index, Err(anyhow::anyhow!("chunk {index} exploded")));
                }
                (index, Ok(Some(format!("etag-{index}"))))
            }) as ChunkFuture
        })
    }

    /// A finalizer that records which chunks it was given.
    fn finalizer(records: Arc<Mutex<Vec<usize>>>) -> Finalizer {
        Box::new(move |completed| {
            let indices: Vec<usize> = completed.iter().map(|record| record.index).collect();
            records.lock().unwrap().extend(indices);
            Box::pin(async { Ok(()) })
        })
    }

    async fn drive(transfer: ChunkTransfer) -> Vec<UploadEvent> {
        transfer.collect().await
    }

    #[tokio::test]
    async fn emits_one_chunk_per_record_then_completes() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let transfer = ChunkTransfer::new(
            session(3),
            Vec::new(),
            false,
            starter(None, Arc::new(Mutex::new(Vec::new()))),
            finalizer(Arc::clone(&records)),
        );

        let events = drive(transfer).await;

        assert_eq!(events.len(), 4);
        for (index, event) in events[..3].iter().enumerate() {
            match event {
                UploadEvent::ChunkDone(record) => assert_eq!(record.index, index),
                other => panic!("expected ChunkDone, got {other:?}"),
            }
        }
        assert!(matches!(events[3], UploadEvent::Completed));
        assert_eq!(*records.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn a_chunk_failure_ends_the_stream() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let transfer = ChunkTransfer::new(
            session(3),
            Vec::new(),
            false,
            starter(Some(0), Arc::new(Mutex::new(Vec::new()))),
            finalizer(Arc::clone(&records)),
        );

        let events = drive(transfer).await;

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], UploadEvent::Failed(_)));
        assert!(records.lock().unwrap().is_empty(), "finalized a failure");
    }

    #[tokio::test]
    async fn replayed_chunks_are_never_started_again() {
        let started = Arc::new(Mutex::new(Vec::new()));
        let records = Arc::new(Mutex::new(Vec::new()));
        let transfer = ChunkTransfer::new(
            session(3),
            vec![record(1)],
            true,
            starter(None, Arc::clone(&started)),
            finalizer(Arc::clone(&records)),
        );

        let events = drive(transfer).await;

        assert_eq!(*started.lock().unwrap(), vec![0, 2]);
        assert_eq!(events.len(), 3);
        // The provider still has to see every part, including the replayed one.
        assert_eq!(*records.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn a_rejected_resumed_session_reports_expiry() {
        let transfer = ChunkTransfer::new(
            session(3),
            vec![record(0)],
            true,
            Box::new(move |chunk: ChunkInfo| {
                let index = chunk.index;
                Box::pin(async move { (index, Err(rejection_error().into())) }) as ChunkFuture
            }),
            finalizer(Arc::new(Mutex::new(Vec::new()))),
        );

        let events = drive(transfer).await;

        assert_eq!(events.len(), 1);
        assert!(
            matches!(
                events[0],
                UploadEvent::Failed(UploadError::SessionExpired)
            ),
            "expected SessionExpired, got {:?}",
            events[0]
        );
    }

    #[tokio::test]
    async fn a_fresh_session_rejection_stays_a_chunk_error() {
        let transfer = ChunkTransfer::new(
            session(1),
            Vec::new(),
            false,
            Box::new(|chunk: ChunkInfo| {
                let index = chunk.index;
                Box::pin(async move {
                    let rejection = rejection_error();
                    (index, Err(rejection.into()))
                }) as ChunkFuture
            }),
            finalizer(Arc::new(Mutex::new(Vec::new()))),
        );

        let events = drive(transfer).await;

        assert!(matches!(events[0], UploadEvent::Failed(_)));
        assert!(!matches!(
            events[0],
            UploadEvent::Failed(UploadError::SessionExpired)
        ));
    }

    #[tokio::test]
    async fn dropping_the_stream_cancels_the_chunk_uploads() {
        let dropped = Arc::new(AtomicUsize::new(0));

        /// Counts its own drop, so the test sees when the chunk future dies.
        struct Probe(Arc<AtomicUsize>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let probe_counter = Arc::clone(&dropped);
        let transfer = ChunkTransfer::new(
            session(1),
            Vec::new(),
            false,
            Box::new(move |_chunk: ChunkInfo| {
                let probe = Probe(Arc::clone(&probe_counter));
                Box::pin(async move {
                    let _probe = probe;
                    futures::future::pending::<()>().await;
                    (0usize, Ok::<Option<String>, anyhow::Error>(None))
                }) as ChunkFuture
            }),
            finalizer(Arc::new(Mutex::new(Vec::new()))),
        );

        let mut pinned = Box::pin(transfer);
        assert!(futures::poll!(pinned.next()).is_pending());
        assert_eq!(dropped.load(Ordering::SeqCst), 0, "chunk still in flight");

        drop(pinned);

        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}
