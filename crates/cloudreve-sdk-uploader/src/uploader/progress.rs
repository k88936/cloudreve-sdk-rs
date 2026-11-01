//! Progress as a fold over upload events.
//!
//! The uploader reports what happened — one chunk finished — and how far the
//! whole file got is a fold over those events. [`Tracker`] is that fold as an
//! explicit state machine instead of a stream combinator: it *borrows* an event
//! rather than consuming it, so one loop can feed a log, a progress bar and
//! anything else without buffering, teeing or back-pressure.
//!
//! Everything here is client-side arithmetic. Cloudreve reports no upload
//! progress, so the numbers are derived from completed chunks and event arrival
//! times.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::uploader::chunk_progress::ChunkProgress;
use crate::uploader::session::UploadSession;
use crate::uploader::stream::UploadEvent;

/// How far an upload got, recomputed after every finished chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct ProgressUpdate {
    /// Total file size in bytes
    pub total_size: u64,
    /// Bytes held by finished chunks
    pub uploaded: u64,
    /// Progress ratio (0.0 - 1.0)
    pub progress: f64,
    /// Recent upload speed in bytes per second
    pub speed_bytes_per_sec: u64,
    /// Estimated time remaining in seconds (`None` until a speed is known)
    pub eta_seconds: Option<u64>,
    /// Total number of chunks
    pub total_chunks: usize,
    /// Finished chunk count
    pub completed_chunks: usize,
}

impl ProgressUpdate {
    fn build(
        total_size: u64,
        uploaded: u64,
        speed_bytes_per_sec: u64,
        total_chunks: usize,
        completed_chunks: usize,
    ) -> Self {
        let uploaded = uploaded.min(total_size);
        let progress = if total_size == 0 {
            1.0
        } else {
            (uploaded as f64 / total_size as f64).clamp(0.0, 1.0)
        };
        let eta_seconds = if speed_bytes_per_sec > 0 && uploaded < total_size {
            Some((total_size - uploaded).div_ceil(speed_bytes_per_sec))
        } else {
            None
        };

        Self {
            total_size,
            uploaded,
            progress,
            speed_bytes_per_sec,
            eta_seconds,
            total_chunks,
            completed_chunks,
        }
    }
}

/// Folds [`UploadEvent`]s into [`ProgressUpdate`]s.
///
/// Seed it with the records replayed from a resume log, then hand it every event
/// of the stream:
///
/// ```ignore
/// let mut tracker = Tracker::new(&stream.session, &completed);
/// println!("{:?}", tracker.current());
/// while let Some(event) = events.next().await {
///     if let Some(update) = tracker.observe(&event) {
///         println!("{update:?}");
///     }
///     // the same event can still be matched and written to a log
/// }
/// ```
pub struct Tracker {
    total_size: u64,
    total_chunks: usize,
    uploaded: u64,
    completed_chunks: usize,
    /// Sliding window for the speed estimate.
    window: Duration,
    samples: Vec<(Instant, u64)>,
}

impl Tracker {
    /// Seed a tracker for `session`.
    ///
    /// `completed` are the records already known, usually read back from a
    /// resume log. The last record per index wins, so a retried chunk is not
    /// counted twice, and records outside the session are ignored.
    pub fn new(session: &UploadSession, completed: &[ChunkProgress]) -> Self {
        let total_chunks = session.num_chunks();
        let mut known = BTreeMap::new();
        for record in completed {
            if record.index < total_chunks {
                known.insert(record.index, record.loaded);
            }
        }

        Self {
            total_size: session.file_size,
            total_chunks,
            uploaded: known.values().sum(),
            completed_chunks: known.len(),
            window: Duration::from_secs(10),
            samples: Vec::with_capacity(16),
        }
    }

    /// The progress before any event of this run arrives.
    pub fn current(&self) -> ProgressUpdate {
        ProgressUpdate::build(
            self.total_size,
            self.uploaded,
            0,
            self.total_chunks,
            self.completed_chunks,
        )
    }

    /// Fold one event.
    ///
    /// Returns a fresh [`ProgressUpdate`] for every finished chunk and `None`
    /// for terminal events: they carry no progress of their own.
    pub fn observe(&mut self, event: &UploadEvent) -> Option<ProgressUpdate> {
        let UploadEvent::ChunkDone(record) = event else {
            return None;
        };

        self.uploaded += record.loaded;
        self.completed_chunks += 1;
        let speed = self.measure_speed();

        Some(ProgressUpdate::build(
            self.total_size,
            self.uploaded,
            speed,
            self.total_chunks,
            self.completed_chunks,
        ))
    }

    /// Bytes per second over the last [`Self::window`], or `0` without enough
    /// samples yet.
    fn measure_speed(&mut self) -> u64 {
        let now = Instant::now();
        self.samples.push((now, self.uploaded));

        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        self.samples.retain(|(at, _)| *at >= cutoff);

        let Some((oldest_at, oldest_uploaded)) = self.samples.first().copied() else {
            return 0;
        };
        let elapsed = now.duration_since(oldest_at).as_secs_f64();
        if self.samples.len() < 2 || elapsed <= 0.0 {
            return 0;
        }

        ((self.uploaded - oldest_uploaded) as f64 / elapsed) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudreve_api::models::explorer::UploadCredential;

    fn session(num_chunks: usize) -> UploadSession {
        UploadSession::new(
            "file:/a".into(),
            "cloudreve://my/a.bin".into(),
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
        ChunkProgress::new(index, 5, None)
    }

    #[test]
    fn seeds_from_replayed_records_without_double_counting() {
        let tracker = Tracker::new(&session(3), &[record(0), record(1), record(1)]);
        let update = tracker.current();

        assert_eq!(update.uploaded, 10);
        assert_eq!(update.completed_chunks, 2);
        assert_eq!(update.total_chunks, 3);
        assert_eq!(update.total_size, 15);
        assert_eq!(update.speed_bytes_per_sec, 0);
        assert!((update.progress - 2.0 / 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ignores_records_outside_the_session() {
        let tracker = Tracker::new(&session(2), &[record(0), record(7)]);
        let update = tracker.current();

        assert_eq!(update.uploaded, 5);
        assert_eq!(update.completed_chunks, 1);
    }

    #[test]
    fn emits_only_for_finished_chunks() {
        let mut tracker = Tracker::new(&session(2), &[]);

        assert!(tracker.observe(&UploadEvent::Completed).is_none());
        assert!(
            tracker
                .observe(&UploadEvent::Failed(
                    crate::uploader::error::UploadError::SessionExpired
                ))
                .is_none()
        );
        assert!(tracker.observe(&UploadEvent::ChunkDone(record(0))).is_some());
    }

    #[test]
    fn reaches_full_progress_on_the_last_chunk() {
        let mut tracker = Tracker::new(&session(2), &[]);

        tracker.observe(&UploadEvent::ChunkDone(record(0)));
        let last = tracker
            .observe(&UploadEvent::ChunkDone(record(1)))
            .expect("chunk event");

        assert_eq!(last.uploaded, last.total_size);
        assert_eq!(last.progress, 1.0);
        assert_eq!(last.completed_chunks, 2);
        assert_eq!(last.eta_seconds, None);
    }

    #[test]
    fn an_empty_file_is_complete_before_it_starts() {
        let tracker = Tracker::new(&session(1), &[]);
        let mut empty = Tracker::new(&session(1), &[]);
        empty.total_size = 0;

        assert_eq!(tracker.current().progress, 0.0);
        assert_eq!(empty.current().progress, 1.0);
    }
}
