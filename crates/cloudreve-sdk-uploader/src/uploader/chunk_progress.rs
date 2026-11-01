//! Chunk completion records.
//!
//! [`ChunkProgress`] is the durable record of one finished chunk. It is the
//! payload of [`UploadEvent::ChunkDone`](super::UploadEvent::ChunkDone) and the
//! line a caller writes to its own log, so a single type describes both "one
//! chunk is done" and "what to replay on resume".
//!
//! [`CompletionSet`] is the uploader's in-memory index over those records.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One finished chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkProgress {
    /// Chunk index (0-based).
    pub index: usize,
    /// Bytes the chunk holds; `0` for the single chunk of an empty file.
    pub loaded: u64,
    /// ETag returned by the storage provider, when the provider returns one.
    pub etag: Option<String>,
}

impl ChunkProgress {
    /// Build a completion record.
    pub fn new(index: usize, loaded: u64, etag: Option<String>) -> Self {
        Self {
            index,
            loaded,
            etag,
        }
    }
}

/// Completion records of one session, keyed by chunk index.
///
/// A record's presence means "this chunk is done"; `loaded` is kept for
/// progress and diagnostics only, which is why an empty chunk is a valid
/// completion. Records outside `0..num_chunks` are dropped, so a damaged or
/// foreign log cannot make the uploader reopen an impossible chunk.
#[derive(Debug, Clone, Default)]
pub(crate) struct CompletionSet {
    records: BTreeMap<usize, ChunkProgress>,
}

impl CompletionSet {
    /// Collect records for a session with `num_chunks` chunks.
    ///
    /// The last record per index wins, so a retried chunk is stored once.
    pub(crate) fn new(
        num_chunks: usize,
        records: impl IntoIterator<Item = ChunkProgress>,
    ) -> Self {
        let mut set = Self::default();
        for record in records {
            if record.index < num_chunks {
                set.insert(record);
            }
        }
        set
    }

    /// Record a finished chunk, replacing any earlier record for its index.
    pub(crate) fn insert(&mut self, record: ChunkProgress) {
        self.records.insert(record.index, record);
    }

    pub(crate) fn contains(&self, index: usize) -> bool {
        self.records.contains_key(&index)
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    /// Bytes held by the finished chunks.
    pub(crate) fn uploaded(&self) -> u64 {
        self.records.values().map(|record| record.loaded).sum()
    }

    /// Records in ascending chunk order, as providers that number parts need.
    pub(crate) fn records(&self) -> impl Iterator<Item = &ChunkProgress> {
        self.records.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(index: usize, loaded: u64) -> ChunkProgress {
        ChunkProgress::new(index, loaded, None)
    }

    #[test]
    fn keeps_the_last_record_for_a_retried_chunk() {
        let set = CompletionSet::new(4, [record(1, 5), record(1, 5), record(2, 3)]);

        assert_eq!(set.len(), 2);
        assert_eq!(
            set.records().map(|r| r.index).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(set.uploaded(), 8);
    }

    #[test]
    fn drops_records_outside_the_session() {
        let set = CompletionSet::new(2, [record(0, 1), record(2, 9), record(99, 9)]);

        assert!(set.contains(0));
        assert!(!set.contains(2));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn records_are_sorted_by_index() {
        let set = CompletionSet::new(5, [record(3, 1), record(0, 1), record(2, 1)]);

        assert_eq!(
            set.records().map(|r| r.index).collect::<Vec<_>>(),
            vec![0, 2, 3]
        );
    }
}
