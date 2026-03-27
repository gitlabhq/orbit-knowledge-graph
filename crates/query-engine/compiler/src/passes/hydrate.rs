use ontology::{FieldSource, Ontology, VirtualSource};

use crate::input::{ColumnSelection, DynamicColumnMode, Input, QueryType};
use crate::passes::codegen::{
    DynamicEntityColumns, HydrationPlan, HydrationTemplate, VirtualColumnRequest,
};

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
///   runtime. Column specs are pre-resolved for all ontology entity types so
///   the server just does a lookup, no ontology re-queries.
pub fn generate_hydration_plan(input: &Input, ontology: &Ontology) -> HydrationPlan {
    match input.query_type {
        QueryType::Aggregation | QueryType::Hydration => HydrationPlan::None,
        QueryType::PathFinding | QueryType::Neighbors => {
            HydrationPlan::Dynamic(build_dynamic_specs(input, ontology))
        }
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

            let (columns, virtual_columns) = split_columns(&requested, ont_node);

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

/// Pre-resolve column specs for every ontology entity type based on the
/// query's `dynamic_columns` mode. The server matches discovered entity
/// types against this list at runtime.
fn build_dynamic_specs(input: &Input, ontology: &Ontology) -> Vec<DynamicEntityColumns> {
    ontology
        .node_names()
        .filter_map(|name| {
            let node = ontology.get_node(name)?;

            let requested: Vec<String> = match input.options.dynamic_columns {
                DynamicColumnMode::All => node
                    .fields
                    .iter()
                    .filter(|f| !f.is_virtual() && f.name != "_version" && f.name != "_deleted")
                    .map(|f| f.name.clone())
                    .collect(),
                DynamicColumnMode::Default => node.default_columns.clone(),
            };

            if requested.is_empty() {
                return None;
            }

            let (columns, virtual_columns) = split_columns(&requested, node);

            if columns.is_empty() && virtual_columns.is_empty() {
                return None;
            }

            Some(DynamicEntityColumns {
                entity_type: name.to_string(),
                destination_table: node.destination_table.clone(),
                columns,
                virtual_columns,
            })
        })
        .collect()
}

/// Partition requested column names into CH-backed and virtual based on
/// the ontology field definitions.
fn split_columns(
    requested: &[String],
    node: &ontology::NodeEntity,
) -> (Vec<String>, Vec<VirtualColumnRequest>) {
    let mut columns = Vec::new();
    let mut virtual_columns = Vec::new();

    for col_name in requested {
        match node.fields.iter().find(|f| &f.name == col_name) {
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
            None => columns.push(col_name.clone()),
        }
    }

    (columns, virtual_columns)
}
