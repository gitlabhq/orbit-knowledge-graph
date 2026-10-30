use std::collections::{BTreeMap, BTreeSet};

use ontology::{DataType, Ontology, StorageColumn, StorageIndex, StorageProjection};

use super::*;
use crate::DataModelError;

impl crate::Relational<ClickHouse> {
    pub fn derive(ontology: &Ontology) -> Result<Self, DataModelError> {
        let mut tables = Vec::new();
        for node in ontology.nodes() {
            tables.push(Table {
                name: node.destination_table.clone(),
                columns: columns(node.storage.columns.iter()),
                column_types: node
                    .fields
                    .iter()
                    .filter_map(|field| {
                        field
                            .column_name()
                            .map(|_| (field.name.clone(), field.data_type))
                    })
                    .chain(system_types())
                    .collect(),
                property_columns: vec![],
                sort_key: node.sort_key.clone(),
                primary_key: node.storage.primary_key.clone(),
                options: TableOptions {
                    indexes: node.storage.indexes.iter().flat_map(index).collect(),
                    projections: node.storage.projections.iter().map(projection).collect(),
                    engine: Engine::replacing(node.storage.version_only_engine),
                    ttl: None,
                    settings: settings(
                        Some(1024),
                        !node.storage.projections.is_empty(),
                        &node.storage.settings,
                    ),
                },
            });
        }
        for name in ontology.edge_tables() {
            let edge = ontology.edge_table_config(name).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "edge table",
                    name: name.into(),
                }
            })?;
            tables.push(Table {
                name: name.into(),
                columns: columns(
                    edge.storage
                        .columns
                        .iter()
                        .chain(&edge.storage.property_columns)
                        .chain(&edge.storage.denormalized_columns),
                ),
                column_types: edge
                    .columns
                    .iter()
                    .chain(&edge.property_columns)
                    .map(|column| (column.name.trim_matches('`').to_string(), column.data_type))
                    .chain(system_types())
                    .collect(),
                property_columns: edge
                    .property_columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                sort_key: edge.sort_key.clone(),
                primary_key: edge.storage.primary_key.clone(),
                options: TableOptions {
                    indexes: edge
                        .storage
                        .indexes
                        .iter()
                        .chain(&edge.storage.denormalized_indexes)
                        .flat_map(index)
                        .collect(),
                    projections: edge.storage.projections.iter().map(projection).collect(),
                    engine: Engine::replacing(false),
                    ttl: None,
                    settings: settings(
                        Some(edge.storage.index_granularity.unwrap_or(1024)),
                        !edge.storage.projections.is_empty(),
                        &edge.storage.settings,
                    ),
                },
            });
        }
        let mut joins = Vec::new();
        let mut copies = Vec::new();
        let mut dependencies = BTreeMap::new();
        for declaration in ontology.denormalized_joins() {
            let anchor = declaration
                .tables
                .iter()
                .position(|table| table.has_traversal_path)
                .ok_or_else(|| {
                    DataModelError::Invalid(format!(
                        "join '{}' has no scoped anchor",
                        declaration.table
                    ))
                })?;
            let mut sources = Vec::new();
            let mut join_columns = Vec::new();
            let mut column_types = BTreeMap::new();
            let mut indexes = Vec::new();
            let mut explicit_settings = BTreeMap::new();
            for (occurrence, source) in declaration.tables.iter().enumerate() {
                let layout = source_table(&tables, &source.table)?;
                let mut bindings = BTreeMap::new();
                for column in &layout.columns {
                    if !ontology::denormalized::copies(&column.name) {
                        continue;
                    }
                    let name = declaration.column_for(occurrence, &column.name);
                    bindings.insert(column.name.clone(), name.clone());
                    join_columns.push(Column {
                        name,
                        ..column.clone()
                    });
                }
                column_types.extend(layout.column_types.iter().filter_map(|(name, data_type)| {
                    bindings
                        .get(name)
                        .map(|column| (column.clone(), *data_type))
                }));
                indexes.extend(layout.options.indexes.iter().filter_map(|index| {
                    bindings.get(&index.column).map(|column| Index {
                        name: match index.name.strip_prefix("idx_") {
                            Some(name) => format!("idx_t{occurrence}_{name}"),
                            None => format!("t{occurrence}_{}", index.name),
                        },
                        column: column.clone(),
                        ..index.clone()
                    })
                }));
                explicit_settings.extend(
                    layout
                        .options
                        .settings
                        .iter()
                        .filter(|(key, _)| {
                            !matches!(
                                key.as_str(),
                                "index_granularity" | "deduplicate_merge_projection_mode"
                            )
                        })
                        .cloned(),
                );
                sources.push(JoinSource {
                    table: source.table.clone(),
                    columns: bindings,
                    join: source
                        .join
                        .as_ref()
                        .map(|join| (join.prev_column.clone(), join.this_column.clone())),
                    filters: source.filter.clone(),
                    path_column: source.has_traversal_path.then(|| {
                        declaration.column_for(occurrence, ontology::TRAVERSAL_PATH_COLUMN)
                    }),
                });
            }
            let path = sources
                .get(anchor)
                .and_then(|source| source.path_column.as_ref())
                .ok_or_else(|| {
                    DataModelError::Invalid(format!(
                        "join '{}' has no scoped anchor",
                        declaration.table
                    ))
                })?;
            if let Some(position) = join_columns.iter().position(|column| &column.name == path) {
                let column = join_columns.remove(position);
                join_columns.insert(0, column);
            }
            join_columns.extend(system_columns(None));
            column_types.extend(system_types());
            tables.push(Table {
                name: declaration.table.clone(),
                columns: join_columns,
                column_types,
                property_columns: vec![],
                sort_key: declaration.sort_key(),
                primary_key: None,
                options: TableOptions {
                    indexes,
                    projections: vec![],
                    engine: Engine::replacing(false),
                    ttl: None,
                    settings: settings(Some(1024), false, &explicit_settings),
                },
            });
            joins.push(MaterializedJoin {
                table: declaration.table.clone(),
                sources,
                nodes: declaration
                    .hops
                    .iter()
                    .enumerate()
                    .flat_map(|(slot, hop)| {
                        let source = (slot == 0).then(|| JoinNode {
                            entity: hop.source_kind.clone(),
                            source_occurrence: hop.source_table,
                            identity_column: declaration
                                .column_for(hop.source_table, ontology::DEFAULT_PRIMARY_KEY),
                        });
                        source.into_iter().chain(std::iter::once(JoinNode {
                            entity: hop.target_kind.clone(),
                            source_occurrence: hop.target_table,
                            identity_column: declaration
                                .column_for(hop.target_table, ontology::DEFAULT_PRIMARY_KEY),
                        }))
                    })
                    .collect(),
                relationships: declaration
                    .hops
                    .iter()
                    .enumerate()
                    .map(|(slot, hop)| JoinRelationship {
                        kind: hop.relationship_kind.clone(),
                        source_slot: slot,
                        target_slot: slot + 1,
                        edge_occurrence: hop.edge_table,
                    })
                    .collect(),
            });
            dependencies.insert(
                declaration.table.clone(),
                declaration
                    .tables
                    .iter()
                    .map(|source| source.table.clone())
                    .collect(),
            );
        }
        for declaration in ontology.reordered_tables() {
            let source = source_table(&tables, &declaration.source)?;
            tables.push(Table {
                name: declaration.name.clone(),
                sort_key: declaration.sort_key.clone(),
                primary_key: None,
                options: TableOptions {
                    projections: vec![],
                    ..source.options.clone()
                },
                ..source.clone()
            });
            dependencies.insert(
                declaration.name.clone(),
                BTreeSet::from([declaration.source.clone()]),
            );
            copies.push(ReorderedCopy {
                table: declaration.name.clone(),
                source: declaration.source.clone(),
            });
        }
        let edge_routes = ontology
            .edges()
            .map(|edge| EdgeRoute {
                relationship: edge.relationship_kind.clone(),
                source: edge.source_kind.clone(),
                target: edge.target_kind.clone(),
                table: edge.destination_table.clone(),
                foreign_key: edge.fk_column.clone(),
            })
            .collect();
        let relationship_tables = ontology
            .edge_names()
            .map(|kind| {
                (
                    kind.to_string(),
                    ontology
                        .edge_tables_for_relationship(kind)
                        .into_iter()
                        .map(String::from)
                        .collect(),
                )
            })
            .collect();
        let writers = ontology.table_writers();
        Ok(Self {
            tables,
            metadata: Metadata {
                copies,
                dependencies,
                entities: crate::implementations::entity_facts(ontology, false),
                default_edge_table: ontology.edge_table().to_string(),
                denormalized: ontology.denormalized_properties().to_vec(),
                traversal_path_lookups: ontology.traversal_path_lookups().to_vec(),
                auxiliary_tables: ontology
                    .auxiliary_tables()
                    .iter()
                    .map(auxiliary::table)
                    .collect(),
                dictionaries: ontology
                    .auxiliary_dictionaries()
                    .iter()
                    .map(auxiliary::dictionary)
                    .collect(),
                views: ontology
                    .materialized_views()
                    .iter()
                    .map(auxiliary::view)
                    .collect(),
                refreshable_views: ontology
                    .refreshable_materialized_views()
                    .iter()
                    .map(|view| RefreshableView {
                        name: view.name.clone(),
                        versioned: view.versioned,
                        select_query: view.select_query.clone(),
                        append_to: view.append_to.clone(),
                        refresh: view.refresh.clone(),
                    })
                    .collect(),
                graph_tables: ontology
                    .nodes()
                    .map(|node| GraphTable {
                        name: node.destination_table.clone(),
                        global: node.global,
                        has_traversal_path: node.has_traversal_path,
                    })
                    .chain(ontology.edge_tables().into_iter().map(|name| {
                        GraphTable {
                            name: name.into(),
                            global: false,
                            has_traversal_path: ontology
                                .edge_table_config(name)
                                .is_some_and(ontology::EdgeTableConfig::has_traversal_path),
                        }
                    }))
                    .collect(),
                joins,
                writers,
                edge_routes,
                relationship_tables,
            },
        })
    }
}

fn source_table<'a>(tables: &'a [Table], name: &str) -> Result<&'a Table, DataModelError> {
    tables
        .iter()
        .find(|table| table.name == name)
        .ok_or_else(|| DataModelError::UnknownReference {
            kind: "storage source",
            name: name.into(),
        })
}

fn columns<'a>(source: impl Iterator<Item = &'a StorageColumn>) -> Vec<Column> {
    source.map(column).chain(system_columns(None)).collect()
}

fn column(source: &StorageColumn) -> Column {
    Column {
        name: source.name.clone(),
        storage_type: source.ch_type.clone(),
        default: source.default.clone(),
        options: ColumnOptions {
            codecs: source.codec.clone().unwrap_or_default(),
        },
    }
}

fn system_types() -> impl Iterator<Item = (String, DataType)> {
    [
        (ontology::VERSION_COLUMN.into(), DataType::DateTime),
        (ontology::DELETED_COLUMN.into(), DataType::Bool),
    ]
    .into_iter()
}

fn index(source: &StorageIndex) -> Vec<Index> {
    let index = Index {
        name: source.name.clone(),
        column: source.column.clone(),
        lowercase: false,
        index_type: source.index_type.clone(),
        granularity: source.granularity,
    };
    if source.index_type != ontology::constants::TEXT_INDEX_TYPE {
        return vec![index];
    }
    let text = Index {
        lowercase: true,
        index_type: "text(tokenizer = splitByNonAlpha)".into(),
        ..index.clone()
    };
    let Some(ngram) = source.ngram else {
        return vec![text];
    };
    vec![
        text,
        Index {
            name: format!("{}_ngram", index.name),
            lowercase: true,
            index_type: format!("ngrambf_v1(3, {}, {}, 0)", ngram.size, ngram.hashes),
            ..index
        },
    ]
}

pub(super) fn projection(source: &StorageProjection) -> Projection {
    match source {
        StorageProjection::Reorder { name, order_by } => Projection::Reorder {
            name: name.clone(),
            order_by: order_by.clone(),
        },
        StorageProjection::Lightweight { name, order_by } => Projection::Lightweight {
            name: name.clone(),
            order_by: order_by.clone(),
        },
        StorageProjection::Aggregate {
            name,
            select,
            group_by,
        } => Projection::Aggregate {
            name: name.clone(),
            select: select.clone(),
            group_by: group_by.clone(),
        },
    }
}

pub(super) fn settings(
    granularity: Option<u32>,
    projections: bool,
    explicit: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let mut settings: Vec<_> = granularity
        .into_iter()
        .map(|value| ("index_granularity".into(), value.to_string()))
        .collect();
    if projections {
        settings.push((
            "deduplicate_merge_projection_mode".into(),
            "'rebuild'".into(),
        ));
    }
    settings.extend(
        [
            "allow_experimental_replacing_merge_with_cleanup",
            "enable_block_number_column",
            "enable_block_offset_column",
        ]
        .map(|name| (name.into(), "1".into())),
    );
    for (name, value) in explicit {
        if let Some((_, previous)) = settings.iter_mut().find(|(key, _)| key == name) {
            *previous = value.clone();
        } else {
            settings.push((name.clone(), value.clone()));
        }
    }
    settings
}
