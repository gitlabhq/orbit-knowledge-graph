//! The states that flow between pipeline stages, plus the runner's own loop
//! state. Each payload is a distinct phase, so the compiler refuses to hand
//! an [`ExtractedPage`] to the writer or a [`TransformedPage`] back to the
//! transform: stage ordering is checked at compile time.

use std::time::Duration;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;

use crate::handler::HandlerError;
use crate::modules::sdlc::datalake::ScanStats;
use crate::modules::sdlc::plan::Cursor;

/// `read_*` count the rows/bytes actually returned from the datalake; `scanned_*`
/// ClickHouse's storage-scan cost from the summary; `written_*` the transformed
/// rows/bytes inserted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::modules::sdlc) struct PipelineStats {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub scanned_rows: u64,
    pub scanned_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub duration_ms: u64,
    // Per-phase time can exceed duration_ms: extract and write overlap via tokio::join!.
    pub extract_ms: u64,
    pub transform_ms: u64,
    pub write_ms: u64,
}

impl PipelineStats {
    pub(in crate::modules::sdlc) fn merge(&mut self, other: PipelineStats) {
        self.read_rows += other.read_rows;
        self.read_bytes += other.read_bytes;
        self.scanned_rows += other.scanned_rows;
        self.scanned_bytes += other.scanned_bytes;
        self.written_rows += other.written_rows;
        self.written_bytes += other.written_bytes;
        self.duration_ms = self.duration_ms.max(other.duration_ms);
        self.extract_ms += other.extract_ms;
        self.transform_ms += other.transform_ms;
        self.write_ms += other.write_ms;
    }
}

/// Carried across loop iterations because the next page is read while the
/// current one is written.
pub(in crate::modules::sdlc) struct ExtractedPage {
    pub batches: Vec<RecordBatch>,
    pub scan_stats: ScanStats,
    /// Timed inside the stage; the runner overlaps extract with a drain, so
    /// call-site timing would conflate the two.
    pub extract_elapsed: Duration,
}

impl ExtractedPage {
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn rows(&self) -> u64 {
        self.batches.iter().map(|b| b.num_rows() as u64).sum()
    }

    pub fn bytes(&self) -> u64 {
        self.batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum()
    }

    /// The cursor advances off the last block's last row, not row order, so a
    /// multi-block page resumes from the true high-water mark.
    pub fn last_block(&self) -> &RecordBatch {
        self.batches
            .last()
            .expect("non-empty page has a last block")
    }
}

pub(in crate::modules::sdlc) struct TransformedPage {
    pub batches_by_table: Vec<Vec<RecordBatch>>,
    pub transform_elapsed: Duration,
}

pub(in crate::modules::sdlc) struct StagedWrites {
    pub futures: FuturesUnordered<BoxFuture<'static, Result<(), HandlerError>>>,
    /// `(table_index, rows, bytes)` per non-empty table, reported to the
    /// observer before the futures are drained.
    pub per_table: Vec<(usize, u64, u64)>,
}

/// A pull's replication-time window. `floor` is persisted so a resume can rebuild it.
#[derive(Clone, Copy)]
pub(in crate::modules::sdlc) struct WindowBounds {
    pub target: DateTime<Utc>,
    pub floor: Option<DateTime<Utc>>,
}

/// The only mutable state the runner carries between pages.
pub(in crate::modules::sdlc) struct RunState {
    /// Points at the last row already durably written; empty on the first page.
    pub cursor: Cursor,
    /// The `(floor, target]` window this run is responsible for.
    pub window: WindowBounds,
    pub page_number: u64,
    pub stats: PipelineStats,
}

impl RunState {
    pub fn new(cursor: Cursor, window: WindowBounds) -> Self {
        Self {
            cursor,
            window,
            page_number: 0,
            stats: PipelineStats::default(),
        }
    }
}
