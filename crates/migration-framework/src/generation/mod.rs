//! Ontology-driven migration generation.
//!
//! Diffs the schema the ontology wants (`compiler::generate_graph_tables`)
//! against a baseline schema and produces an **additive** migration: a
//! `CREATE TABLE` for each new entity and `ADD COLUMN`/`ADD INDEX`/
//! `ADD PROJECTION` for each drifted (additively-changed) entity.
//!
//! Non-additive drift — dropped/renamed/retyped columns, sort-key or engine
//! changes — is refused rather than emitted: those require a deliberate,
//! out-of-band schema change, not a generated migration. See ADR 016.

pub mod diff;
pub mod emit;

pub use diff::{BreakingChange, SchemaChange, SchemaDiff, diff_schemas, generate_from_ontology};
pub use emit::{render_down, render_up};
