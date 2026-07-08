//! The extract→transform seam: which RecordBatch columns a transform may match on, and
//! where each came from. Only enriched columns are enumerated — source and framework
//! columns are opaque, because the transform stage never matches on them.

use ontology::DenormDirection;

#[derive(Debug)]
pub(in crate::modules::sdlc) struct BatchSchema {
    enriched: Vec<EnrichedColumn>,
}

#[derive(Debug)]
pub(in crate::modules::sdlc) struct EnrichedColumn {
    /// Exact RecordBatch column name the extract emits (e.g. `_e0_state`).
    pub name: String,
    pub node_kind: String,
    pub direction: DenormDirection,
    /// Raw endpoint-node source column (e.g. `state`), before property-name resolution.
    pub node_column: String,
}

impl BatchSchema {
    pub(in crate::modules::sdlc) fn opaque() -> Self {
        Self {
            enriched: Vec::new(),
        }
    }

    pub(in crate::modules::sdlc) fn enriched(columns: Vec<EnrichedColumn>) -> Self {
        Self { enriched: columns }
    }

    pub(in crate::modules::sdlc) fn enriched_columns(&self) -> &[EnrichedColumn] {
        &self.enriched
    }
}
