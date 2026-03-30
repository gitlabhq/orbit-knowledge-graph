//! Hydration plan: decides how the server fetches entity properties after
//! the base query returns IDs.

use ontology::{FieldSource, Ontology, VirtualSource};

use crate::input::{ColumnSelection, DynamicColumnMode, Input, QueryType};

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum HydrationPlan {
    /// No hydration needed (e.g., Aggregation).
    None,
    /// Entity types known at compile time (Traversal).
    /// One template per input node, with IDs to be filled at runtime.
    Static(Vec<HydrationTemplate>),
    /// Entity types discovered at runtime (PathFinding, Neighbors).
    /// Column specs are pre-resolved for every ontology entity type so
    /// the server just looks up the matching spec — no ontology queries.
    Dynamic(Vec<DynamicEntityColumns>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct HydrationTemplate {
    pub entity_type: String,
    /// Alias from the base query (e.g. "u", "p"). Used to correlate hydration
    /// results back to the base query's `_gkg_{alias}_pk` (or `_gkg_{alias}_id`
    /// when PK == auth ID) column.
    pub node_alias: String,
    /// ClickHouse table to query (resolved from ontology at compile time).
    pub destination_table: String,
    /// Column-backed columns to fetch from ClickHouse. Resolved at compile time
    /// from the user's explicit column selection or the ontology's default_columns,
    /// with virtual columns filtered out.
    pub columns: Vec<String>,
    /// Virtual columns that need to be resolved from remote services after
    /// ClickHouse hydration completes.
    pub virtual_columns: Vec<VirtualColumnRequest>,
}

/// Pre-resolved column spec for an entity type in dynamic hydration.
/// Built at compile time for every entity type in the ontology so the
/// server avoids runtime ontology lookups.
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicEntityColumns {
    pub entity_type: String,
    pub destination_table: String,
    /// Column-backed columns to fetch from ClickHouse.
    pub columns: Vec<String>,
    /// Virtual columns that need remote resolution.
    pub virtual_columns: Vec<VirtualColumnRequest>,
}

/// A column that must be resolved from a remote service rather than ClickHouse.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualColumnRequest {
    /// The column name as the user sees it (e.g. "content").
    pub column_name: String,
    /// Logical service name (e.g. "gitaly").
    pub service: String,
    /// Logical operation name within the service (e.g. "blob_content").
    pub lookup: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan generation
// ─────────────────────────────────────────────────────────────────────────────

/// Build the hydration plan for a compiled query.
///
/// - Aggregation: no hydration (results are aggregate values, not entity rows).
/// - Search: no hydration (base query already carries node columns).
/// - Traversal (edge-centric): static hydration — entity types are known at
///   compile time, so we build one template per entity type with pre-resolved
///   destination table, column-backed columns, and virtual column requests.
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

            // Normalize expands All and None into List before this pass runs.
            let Some(ColumnSelection::List(requested)) = &node.columns else {
                return None;
            };

            let (columns, virtual_columns) = split_columns(requested, ont_node);

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
                FieldSource::DatabaseColumn(_) => columns.push(col_name.clone()),
                FieldSource::Virtual(VirtualSource {
                    service,
                    lookup,
                    disabled,
                }) => {
                    if !disabled {
                        virtual_columns.push(VirtualColumnRequest {
                            column_name: col_name.clone(),
                            service: service.clone(),
                            lookup: lookup.clone(),
                        });
                    }
                }
            },
            None => columns.push(col_name.clone()),
        }
    }

    (columns, virtual_columns)
}
