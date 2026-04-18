/// Before each recursive call, we check `stacker::remaining_stack()` and bail out when
/// less than this many bytes remain, trading completeness for crash safety.
pub const MINIMUM_STACK_REMAINING: usize = 128 * 1024; // 128 KiB

pub mod analysis;
pub mod graph;
pub mod indexer;
pub mod loading;
pub mod parsing;
pub mod stats;

#[cfg(test)]
mod tests;
