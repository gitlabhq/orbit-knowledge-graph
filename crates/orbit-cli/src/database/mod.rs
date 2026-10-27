mod locks;
pub mod manifest;
mod reader;
pub mod schema;
mod writer;

pub use locks::{
    BRIEF_WAIT, WAIT_TIMED_OUT_EXIT_CODE, WaitTimedOut, index_is_running, lock_database_waiting,
    wait_timeout,
};
use locks::{lock_database, read_lock_database};
pub use reader::GraphReader;
pub use writer::GraphWriter;

pub(crate) fn lock_incremental_state(
    path: &std::path::Path,
    wait: std::time::Duration,
) -> anyhow::Result<std::fs::File> {
    lock_database_waiting(path, wait)
}
