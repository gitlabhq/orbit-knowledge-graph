pub(super) mod global;
pub(super) mod namespace;

pub(super) fn default_datalake_batch_size() -> u64 {
    1_000_000
}
