//! Page-bounded enrichment joins, uniform across node/edge/derived pipelines.

use ontology::{DenormDirection, EdgeMapping, EtlScope, Extract, NodeRefKind, Ontology};

const ENRICHMENT_ALIAS_PREFIX: &str = "_e";

#[derive(Clone)]
pub(in crate::modules::sdlc) struct EnrichmentJoin {
    pub alias: String,
    pub table: String,
    pub key: String,
    pub batch_column: String,
    pub columns: Vec<String>,
    /// Adds a traversal_path predicate inside the CTE.
    pub scope_to_path: bool,
    /// Aliases pulled columns `{alias}_{col}` — an edge's two node refs can share column names.
    pub prefix_output: bool,
    /// Reported through the batch schema so transform matches denormalized properties without re-deriving the join.
    pub node_kind: String,
    pub direction: DenormDirection,
}

impl EnrichmentJoin {
    /// One CTE per enriched `Literal` node ref, path-scoped for namespaced endpoints of a namespaced edge since the `id IN (_batch)` bound alone does not prune the endpoint table's sort key.
    pub(in crate::modules::sdlc) fn from_mapping(
        mapping: &EdgeMapping,
        edge_scope: EtlScope,
        ontology: &Ontology,
    ) -> Vec<EnrichmentJoin> {
        let mut joins = Vec::new();
        let mut idx = 0usize;
        for (node_ref, direction) in [
            (&mapping.source, DenormDirection::Source),
            (&mapping.target, DenormDirection::Target),
        ] {
            let NodeRefKind::Literal(kind) = &node_ref.kind else {
                continue;
            };
            if node_ref.enrich.is_empty() {
                continue;
            }
            let Some(base_table) = node_base_table(ontology, kind) else {
                continue;
            };
            let scope_to_path =
                edge_scope == EtlScope::Namespaced && node_is_namespaced(ontology, kind);
            joins.push(EnrichmentJoin {
                alias: format!("{ENRICHMENT_ALIAS_PREFIX}{idx}"),
                table: base_table.to_string(),
                key: "id".to_string(),
                batch_column: node_ref.field.clone(),
                columns: node_ref.enrich.clone(),
                scope_to_path,
                prefix_output: true,
                node_kind: kind.clone(),
                direction,
            });
            idx += 1;
        }
        joins
    }
}

fn node_is_namespaced(ontology: &Ontology, node_kind: &str) -> bool {
    ontology
        .get_node(node_kind)
        .and_then(|node| node.pipelines.first())
        .is_some_and(|pipeline| pipeline.scope == EtlScope::Namespaced)
}

fn node_base_table<'a>(ontology: &'a Ontology, node_kind: &str) -> Option<&'a str> {
    ontology
        .get_node(node_kind)?
        .pipelines
        .first()
        .and_then(|p| {
            let Extract::ClickHouse(extract) = &p.extract;
            extract.tables.first()
        })
        .map(String::as_str)
}
