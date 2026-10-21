// Shared validation enforces these limits for both JSON and GQL queries.
// build.rs checks matching JSON schema limits and includes this file, so keep it independent of other crate modules.

pub const MAX_HOPS_CAP: u32 = 3;
pub const MAX_DEPTH_CAP: u32 = 3;
pub const MAX_NODES_CAP: usize = 5;
pub const MAX_RELS_CAP: usize = 5;
pub const MAX_NODE_IDS: usize = 500;
pub const MAX_IN_VALUES: usize = 100;
pub const MAX_FILTERS_PER_NODE: usize = 10;
pub const MAX_FILTERS_PER_REL: usize = 5;
pub const MAX_COLUMNS: usize = 50;
pub const MAX_REL_TYPES: usize = 10;
pub const MAX_FILTER_ENTRIES_PER_PROPERTY: usize = 10;
pub const MAX_PREDICATES: usize = 16;
pub const MAX_LIMIT: u32 = 1000;
pub const MAX_SKIP: u32 = 10_000;
pub const MAX_IDENTIFIER_LEN: usize = 64;
pub const MAX_FILTER_STRING_LEN: usize = 1024;
#[allow(
    dead_code,
    reason = "consumed by the compiler; build.rs has no predicate schema"
)]
pub const MAX_PREDICATE_DEPTH: usize = 32;
#[allow(
    dead_code,
    reason = "consumed by the compiler; build.rs has no predicate schema"
)]
pub const MAX_PREDICATE_LEAVES: usize = 256;
