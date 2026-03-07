pub mod backend;
pub mod ir;
pub mod pass;
pub mod pipeline;

// Re-export core IR types at crate root for ergonomic use.
// Downstream crates can use `llqm::expr`, `llqm::plan`, etc. without
// knowing about the `ir/` nesting.
pub use ir::expr;
pub use ir::plan;
pub use ir::substrait;

// Re-export the ClickHouse backend as `llqm::codegen` for backward compat.
pub use backend::clickhouse as codegen;

// Re-export pipeline entry points.
pub use pipeline::{Backend, Frontend, Pipeline};
