//! Page-bounded enrichment joins, uniform across node/edge/derived pipelines.

use ontology::{DenormDirection, EdgeMapping, EtlScope, NodeRefKind};

const ENRICHMENT_ALIAS_PREFIX: &str = "_e";

#[derive(Clone)]
pub(in crate::modules::sdlc) struct EnrichmentJoin {
    pub alias: String,
    pub table: String,
    pub key: String,
    pub batch_column: String,
    pub columns: Vec<String>,
    pub has_traversal_path: bool,
    pub column_alias: bool,
    pub node_kind: String,
    pub direction: DenormDirection,
}

impl EnrichmentJoin {
    /// One CTE per enriched `Literal` node ref, path-scoped for namespaced endpoints of a namespaced edge since the `id IN (_batch)` bound alone does not prune the endpoint table's sort key.
    pub(in crate::modules::sdlc) fn from_mapping(
        mapping: &EdgeMapping,
        edge_scope: EtlScope,
    ) -> Vec<EnrichmentJoin> {
        let mut joins = Vec::new();

        for (node_ref, direction) in [
            (&mapping.source, DenormDirection::Source),
            (&mapping.target, DenormDirection::Target),
        ] {
            let Some(source) = &node_ref.enrich_source else {
                continue;
            };
            let NodeRefKind::Literal(kind) = &node_ref.kind else {
                continue;
            };

            joins.push(EnrichmentJoin {
                alias: format!("{ENRICHMENT_ALIAS_PREFIX}{}", joins.len()),
                table: source.table.clone(),
                key: "id".to_string(),
                batch_column: node_ref.field.clone(),
                columns: node_ref.enrich.clone(),
                has_traversal_path: edge_scope == EtlScope::Namespaced && source.namespaced,
                column_alias: true,
                node_kind: kind.clone(),
                direction,
            });
        }

        joins
    }
}
