//! Migration framework for the ontology-driven graph schema.
//!
//! Today this crate holds migration **generation** ([`generation`]): diffing
//! the schema the ontology wants against a baseline and producing an additive
//! migration for new and drifted entities. The migration **runner** — applying
//! migrations and triggering the re-index an applied migration implies — is a
//! sibling concern that lands in this crate later. See ADR 016.

pub mod generation;
