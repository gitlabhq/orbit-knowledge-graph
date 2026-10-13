mod locks;
pub mod manifest;
mod reader;
pub mod schema;
mod writer;

use locks::{lock_database, read_lock_database};
pub use reader::GraphReader;
pub use writer::GraphWriter;

pub(crate) fn lock_incremental_state(path: &std::path::Path) -> anyhow::Result<std::fs::File> {
    lock_database(path)
}
