use ontology::{FieldSource, Ontology, VirtualSource};

use crate::input::{ColumnSelection, Input, QueryType};
use crate::passes::codegen::{HydrationPlan, HydrationTemplate, VirtualColumnRequest};

/// Build the hydration context for a compiled query.
///
/// - Aggregation: no hydration (results are aggregate values, not entity rows).
/// - Search: no hydration (base query already carries node columns).
/// - Traversal (edge-centric): static hydration — entity types are known at
///   compile time, so we build one template per entity type with pre-resolved
///   destination table, column-backed columns, and virtual column requests.
/// - Traversal (join-based fallback): no hydration — base query already joins
///   node tables and carries their columns.
/// - PathFinding/Neighbors: dynamic hydration — entity types are discovered at
///   runtime from edge data, so the server builds hydration queries on the fly.
pub fn generate_hydration_plan(input: &Input, ontology: &Ontology) -> HydrationPlan {
    match input.query_type {
        QueryType::Aggregation | QueryType::Hydration => HydrationPlan::None,
        QueryType::PathFinding | QueryType::Neighbors => HydrationPlan::Dynamic,
        QueryType::Search => HydrationPlan::None,
        QueryType::Traversal => HydrationPlan::Static(build_static_templates(input, ontology)),
    }
}

fn build_static_templates(input: &Input, ontology: &Ontology) -> Vec<HydrationTemplate> {
    input
        .nodes
        .iter()
        .filter_map(|node| {
            let entity = node.entity.as_ref()?;
            let ont_node = ontology.get_node(entity)?;

            let requested: Vec<String> = match &node.columns {
                Some(ColumnSelection::List(cols)) => cols.clone(),
                _ => ont_node.default_columns.clone(),
            };

            let mut columns = Vec::new();
            let mut virtual_columns = Vec::new();

            for col_name in &requested {
                match ont_node.fields.iter().find(|f| &f.name == col_name) {
                    Some(field) => match &field.source {
                        FieldSource::Column(_) => columns.push(col_name.clone()),
                        FieldSource::Virtual(VirtualSource { service, lookup }) => {
                            virtual_columns.push(VirtualColumnRequest {
                                column_name: col_name.clone(),
                                service: service.clone(),
                                lookup: lookup.clone(),
                            });
                        }
                    },
                    // Column not in ontology — pass through as a CH column.
                    // Validation catches unknown columns earlier in the pipeline.
                    None => columns.push(col_name.clone()),
                }
            }

            if columns.is_empty() && virtual_columns.is_empty() {
                return None;
            }

            Some(HydrationTemplate {
                entity_type: entity.clone(),
                node_alias: node.id.clone(),
                destination_table: ont_node.destination_table.clone(),
                columns,
                virtual_columns,
            })
        })
        .collect()
}
