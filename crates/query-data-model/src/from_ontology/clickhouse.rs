use std::collections::{BTreeMap, BTreeSet, HashSet};

use indexmap::IndexMap;
use ontology::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};
use ontology::denormalized::DenormalizedJoin;
use ontology::{
    DataType, Ontology, StorageColumn, StorageIndex, StorageProjection, TraversalPathKind,
};

use super::{PLATFORM, attribute_columns, options};
use crate::DataModelError;
use crate::implementations::clickhouse::schema::{default_index_name, storage_type};
use crate::implementations::clickhouse::spec::{
    AggregateProjection, ClickHouseOptions, ClickHouseSource, ColumnSpec, CopySpec,
    DenormalizedSpec, DictionarySpec, EdgeTable, IndexDetail, IndexSpec, IndexSpecs, JoinNodeSpec,
    JoinOnSpec, JoinRelationshipSpec, JoinSourceSpec, JoinSpec, LifetimeSpec, OrderedProjection,
    PathLookupSpec, ProjectionSpec, RefreshableViewSpec, Settings, TableSpec, ViewSpec,
};
use crate::implementations::relational::RelationalRoute;
use crate::spec::{EngineSpec, RouteSpec, SourceSpec, StorageFragment, format_variant, snake_case};

const INDEX_GRANULARITY: &str = "index_granularity";
const DEFAULT_GRANULARITY: &str = "1024";
const ENGINE_FLAGS: [&str; 3] = [
    "allow_experimental_replacing_merge_with_cleanup",
    "enable_block_number_column",
    "enable_block_offset_column",
];

pub(super) fn graph_store(ontology: &Ontology) -> Result<StorageFragment, DataModelError> {
    let writers = ontology.table_writers();
    let backend = ClickHouseOptions {
        version_column: Some(ontology::VERSION_COLUMN.to_string()),
        deleted_column: Some(ontology::DELETED_COLUMN.to_string()),
        engine: Some(replacing(false)),
        sort_key: ontology.default_entity_sort_key().to_vec(),
        columns: system_columns(None),
        settings: std::iter::once((INDEX_GRANULARITY.to_string(), DEFAULT_GRANULARITY.into()))
            .chain(engine_flags())
            .collect(),
        edge_tables: edge_tables(ontology, &writers),
        denormalized: denormalized(ontology),
        copies: ontology
            .reordered_tables()
            .iter()
            .map(|copy| {
                let spec = CopySpec {
                    source: copy.source.clone(),
                    sort_key: copy.sort_key.clone(),
                };
                (copy.name.clone(), spec)
            })
            .collect(),
        joins: ontology
            .denormalized_joins()
            .iter()
            .map(|join| (join.table.clone(), join_spec(ontology, join)))
            .collect(),
        tables: auxiliary_tables(ontology),
        dictionaries: dictionaries(ontology),
        views: views(ontology),
        refreshable_views: ontology
            .refreshable_materialized_views()
            .iter()
            .map(|view| {
                let spec = RefreshableViewSpec {
                    versioned: view.versioned,
                    append_to: view.append_to.clone(),
                    refresh: view.refresh.clone(),
                    select: view.select_query.clone(),
                };
                (view.name.clone(), spec)
            })
            .collect(),
    };
    let relationships = routes(ontology, &backend)?;
    let entities = ontology
        .nodes()
        .map(|node| Ok((node.name.clone(), graph_source(ontology, node, &backend)?)))
        .collect::<Result<_, DataModelError>>()?;
    Ok(StorageFragment {
        package: PLATFORM.to_string(),
        store: "graph".to_string(),
        engine: Some(EngineSpec {
            kind: "clickhouse".to_string(),
            connection: Some("main".to_string()),
            trusted: false,
            table: Some(format!("{}{{entity}}", ontology.table_prefix())),
            path_column: Some(TRAVERSAL_PATH_COLUMN.to_string()),
            backend: options(backend)?,
        }),
        entities,
        relationships,
    })
}

fn replacing(version_only: bool) -> String {
    if version_only {
        format!("ReplacingMergeTree({})", ontology::VERSION_COLUMN)
    } else {
        format!(
            "ReplacingMergeTree({}, {})",
            ontology::VERSION_COLUMN,
            ontology::DELETED_COLUMN
        )
    }
}

fn engine_flags() -> impl Iterator<Item = (String, String)> {
    ENGINE_FLAGS
        .into_iter()
        .map(|name| (name.to_string(), "1".to_string()))
}

fn projection_mode(projections: &[StorageProjection]) -> Option<(String, String)> {
    (!projections.is_empty()).then(|| {
        (
            "deduplicate_merge_projection_mode".to_string(),
            "'rebuild'".to_string(),
        )
    })
}

fn system_columns(version_type: Option<&str>) -> IndexMap<String, ColumnSpec> {
    let version = if version_type == Some("uint64") {
        ColumnSpec {
            storage_type: Some("UInt64".into()),
            ..ColumnSpec::default()
        }
    } else {
        ColumnSpec {
            storage_type: Some("DateTime64(6, 'UTC')".into()),
            default: Some("now64(6)".into()),
            codec: vec!["Delta(8)".into(), "ZSTD(1)".into()],
        }
    };
    let deleted = ColumnSpec {
        storage_type: Some("Bool".into()),
        default: Some("false".into()),
        codec: Vec::new(),
    };
    IndexMap::from([
        (ontology::VERSION_COLUMN.to_string(), version),
        (ontology::DELETED_COLUMN.to_string(), deleted),
    ])
}

fn column_name(column: &StorageColumn) -> String {
    column.name.trim_matches('`').to_string()
}

fn column_spec(column: &StorageColumn, inferred: Option<&str>) -> ColumnSpec {
    ColumnSpec {
        storage_type: (inferred != Some(column.ch_type.as_str())).then(|| column.ch_type.clone()),
        default: column.default.clone(),
        codec: column.codec.clone().unwrap_or_default(),
    }
}

fn indexes<'a>(indexes: impl Iterator<Item = &'a StorageIndex>) -> IndexMap<String, IndexSpecs> {
    let mut grouped = IndexMap::<String, Vec<IndexSpec>>::new();
    for index in indexes {
        let column = index.column.trim_matches('`');
        let position = grouped.get(column).map_or(0, Vec::len);
        let default_name = default_index_name(column, position, &index.index_type);
        let spec = if index.name == default_name && index.granularity == 1 {
            IndexSpec::Kind(index.index_type.clone())
        } else {
            IndexSpec::Detail(IndexDetail {
                kind: index.index_type.clone(),
                granularity: index.granularity,
                name: (index.name != default_name).then(|| index.name.clone()),
            })
        };
        grouped.entry(column.to_string()).or_default().push(spec);
    }
    grouped
        .into_iter()
        .map(|(column, mut specs)| {
            let specs = match specs.len() {
                1 => IndexSpecs::One(specs.remove(0)),
                _ => IndexSpecs::Many(specs),
            };
            (column, specs)
        })
        .collect()
}

fn projections(projections: &[StorageProjection]) -> IndexMap<String, ProjectionSpec> {
    projections
        .iter()
        .map(|projection| match projection {
            StorageProjection::Reorder { name, order_by } => (
                name.clone(),
                ProjectionSpec::Ordered(OrderedProjection {
                    order_by: order_by.clone(),
                    lightweight: false,
                }),
            ),
            StorageProjection::Lightweight { name, order_by } => (
                name.clone(),
                ProjectionSpec::Ordered(OrderedProjection {
                    order_by: order_by.clone(),
                    lightweight: true,
                }),
            ),
            StorageProjection::Aggregate {
                name,
                select,
                group_by,
            } => (
                name.clone(),
                ProjectionSpec::Aggregate(AggregateProjection {
                    select: select.clone(),
                    group_by: group_by.clone(),
                }),
            ),
        })
        .collect()
}

fn table_settings(
    explicit: &BTreeMap<String, String>,
    projections: &[StorageProjection],
) -> Settings {
    explicit
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .chain(projection_mode(projections))
        .collect()
}

fn edge_tables(
    ontology: &Ontology,
    writers: &BTreeMap<String, BTreeSet<String>>,
) -> IndexMap<String, EdgeTable> {
    ontology
        .edge_tables()
        .into_iter()
        .filter_map(|name| {
            let config = ontology.edge_table_config(name)?;
            let storage = &config.storage;
            let granularity = storage
                .index_granularity
                .map(|value| value.to_string())
                .filter(|value| value != DEFAULT_GRANULARITY)
                .map(|value| (INDEX_GRANULARITY.to_string(), value));
            let table = EdgeTable {
                default: name == ontology.edge_table(),
                domains: config.domains.iter().cloned().collect(),
                sort_key: config.sort_key.clone(),
                primary_key: storage.primary_key.clone(),
                engine: None,
                columns: storage
                    .columns
                    .iter()
                    .chain(&storage.denormalized_columns)
                    .map(|column| (column_name(column), column_spec(column, None)))
                    .collect(),
                indexes: indexes(storage.indexes.iter().chain(&storage.denormalized_indexes)),
                projections: projections(&storage.projections),
                settings: granularity
                    .into_iter()
                    .chain(table_settings(&storage.settings, &storage.projections))
                    .collect(),
                writers: writers.get(name).into_iter().flatten().cloned().collect(),
            };
            Some((name.to_string(), table))
        })
        .collect()
}

fn denormalized(ontology: &Ontology) -> Vec<DenormalizedSpec> {
    ontology
        .denormalized_properties()
        .iter()
        .map(|property| DenormalizedSpec {
            entity: property.node_kind.clone(),
            property: property.property_name.clone(),
            relationship: property.relationship_kind.clone(),
            side: match property.direction {
                ontology::DenormDirection::Source => "source",
                ontology::DenormDirection::Target => "target",
            }
            .to_string(),
            column: property.edge_column.clone(),
            tag: property.tag_key.clone(),
            nullable: property.nullable,
        })
        .collect()
}

fn routes(
    ontology: &Ontology,
    backend: &ClickHouseOptions,
) -> Result<IndexMap<String, RouteSpec>, DataModelError> {
    let routing = backend.relational();
    let domain_of = |entity: &str| ontology.get_node(entity).map(|node| node.domain.as_str());
    let holds = |kind: &str, column: &str| {
        ontology
            .get_node(kind)
            .is_some_and(|node| node.fields.iter().any(|field| field.name == column))
    };
    let mut relationships = IndexMap::new();
    for name in ontology.edge_names() {
        let edges: Vec<_> = ontology
            .get_edge(name)
            .unwrap_or_default()
            .iter()
            .filter(|edge| !edge.source_kind.is_empty() && !edge.target_kind.is_empty())
            .collect();
        let bindings: Vec<IndexMap<String, String>> = edges
            .iter()
            .map(|edge| {
                attribute_columns(ontology, &edge.destination_table)
                    .map(|column| (column.name.clone(), column.name.clone()))
                    .collect()
            })
            .collect();
        let shared = match bindings.split_first() {
            Some((first, rest)) if rest.iter().all(|columns| columns == first) => first.clone(),
            _ => IndexMap::new(),
        };
        let mut variants = IndexMap::new();
        for (edge, columns) in edges.into_iter().zip(bindings) {
            let expected =
                routing.edge_table_for(domain_of(&edge.source_kind), domain_of(&edge.target_kind));
            let table = (Some(edge.destination_table.as_str()) != expected)
                .then(|| edge.destination_table.clone());
            let route = RouteSpec {
                fk: edge.fk_column.clone().filter(|column| {
                    holds(&edge.source_kind, column) || holds(&edge.target_kind, column)
                }),
                columns: if shared.is_empty() {
                    columns
                } else {
                    IndexMap::new()
                },
                backend: options(RelationalRoute { table })?,
                variants: IndexMap::new(),
            };
            if route != RouteSpec::default() {
                variants.insert(format_variant(&edge.source_kind, &edge.target_kind), route);
            }
        }
        if !variants.is_empty() || !shared.is_empty() {
            let route = RouteSpec {
                columns: shared,
                variants,
                ..RouteSpec::default()
            };
            relationships.insert(name.to_string(), route);
        }
    }
    Ok(relationships)
}

fn graph_source(
    ontology: &Ontology,
    node: &ontology::NodeEntity,
    store: &ClickHouseOptions,
) -> Result<SourceSpec, DataModelError> {
    let pattern = format!("{}{}", ontology.table_prefix(), snake_case(&node.name));
    let epoch_nulls = epoch_nulls(node);
    let storage = &node.storage;
    let columns = storage
        .columns
        .iter()
        .map(|column| {
            let name = column_name(column);
            let inferred = node
                .fields
                .iter()
                .find(|field| field.name == name)
                .map(|field| {
                    storage_type(
                        data_type(field.data_type),
                        field.nullable && !epoch_nulls.contains(&name),
                    )
                });
            let spec = column_spec(column, inferred.as_deref());
            (name, spec)
        })
        .collect();
    let backend = ClickHouseSource {
        sort_key: if node.sort_key == store.sort_key {
            Vec::new()
        } else {
            node.sort_key.clone()
        },
        primary_key: storage.primary_key.clone(),
        engine: storage.version_only_engine.then(|| replacing(true)),
        columns,
        indexes: indexes(storage.indexes.iter()),
        projections: projections(&storage.projections),
        settings: table_settings(&storage.settings, &storage.projections),
        high_fan_out: node
            .fields
            .iter()
            .filter(|field| field.high_fan_out)
            .map(|field| field.name.clone())
            .collect(),
        epoch_nulls,
        path_lookups: ontology
            .traversal_path_lookups()
            .iter()
            .filter(|lookup| lookup.entity == node.name)
            .map(|lookup| {
                let kind = match lookup.kind {
                    TraversalPathKind::Id => "id",
                    TraversalPathKind::FullPath => "full_path",
                };
                let spec = PathLookupSpec {
                    table: lookup.source_table.clone(),
                    key: lookup.key_column.clone(),
                };
                (kind.to_string(), spec)
            })
            .collect(),
    };
    Ok(SourceSpec {
        table: (node.destination_table != pattern).then(|| node.destination_table.clone()),
        path_column: (node.global || !node.has_traversal_path).then_some(None),
        backend: options(backend)?,
        ..SourceSpec::default()
    })
}

fn epoch_nulls(node: &ontology::NodeEntity) -> Vec<String> {
    let non_nullable: HashSet<String> = node
        .storage
        .columns
        .iter()
        .filter(|column| !column.ch_type.starts_with("Nullable("))
        .map(column_name)
        .collect();
    node.fields
        .iter()
        .filter(|field| {
            field.nullable
                && matches!(field.data_type, DataType::DateTime | DataType::Date)
                && non_nullable.contains(&field.name)
        })
        .map(|field| field.name.clone())
        .collect()
}

fn physical_columns(ontology: &Ontology, table: &str) -> Vec<String> {
    if let Some(node) = ontology
        .nodes()
        .find(|node| node.destination_table == table)
    {
        return node.storage.columns.iter().map(column_name).collect();
    }
    ontology
        .edge_table_config(table)
        .map(|config| {
            let storage = &config.storage;
            storage
                .columns
                .iter()
                .chain(&storage.denormalized_columns)
                .map(column_name)
                .collect()
        })
        .unwrap_or_default()
}

fn join_spec(ontology: &Ontology, join: &DenormalizedJoin) -> JoinSpec {
    let sources = join
        .tables
        .iter()
        .enumerate()
        .map(|(occurrence, table)| JoinSourceSpec {
            table: table.table.clone(),
            path_column: table
                .has_traversal_path
                .then(|| join.column_for(occurrence, TRAVERSAL_PATH_COLUMN)),
            columns: physical_columns(ontology, &table.table)
                .into_iter()
                .filter(|column| ontology::denormalized::copies(column))
                .map(|column| {
                    let renamed = join.column_for(occurrence, &column);
                    (column, renamed)
                })
                .collect(),
            on: table.join.as_ref().map(|on| JoinOnSpec {
                previous: on.prev_column.clone(),
                column: on.this_column.clone(),
            }),
            filter: table.filter.iter().cloned().collect(),
        })
        .collect();
    let identity = |occurrence: usize| join.column_for(occurrence, DEFAULT_PRIMARY_KEY);
    let nodes = join
        .hops
        .iter()
        .enumerate()
        .flat_map(|(slot, hop)| {
            let source = (slot == 0).then(|| JoinNodeSpec {
                entity: hop.source_kind.clone(),
                source: hop.source_table,
                key: identity(hop.source_table),
            });
            source.into_iter().chain(std::iter::once(JoinNodeSpec {
                entity: hop.target_kind.clone(),
                source: hop.target_table,
                key: identity(hop.target_table),
            }))
        })
        .collect();
    let relationships = join
        .hops
        .iter()
        .enumerate()
        .map(|(slot, hop)| JoinRelationshipSpec {
            relationship: hop.relationship_kind.clone(),
            source: slot,
            target: slot + 1,
            edge: hop.edge_table,
        })
        .collect();
    JoinSpec {
        sort_key: join.sort_key(),
        sources,
        nodes,
        relationships,
    }
}

fn data_type(data_type: DataType) -> crate::DataType {
    match data_type {
        DataType::String => crate::DataType::String,
        DataType::Int => crate::DataType::Int,
        DataType::Float => crate::DataType::Float,
        DataType::Bool => crate::DataType::Bool,
        DataType::Date => crate::DataType::Date,
        DataType::DateTime => crate::DataType::DateTime,
        DataType::Enum => crate::DataType::Enum,
        DataType::Uuid => crate::DataType::Uuid,
    }
}

fn auxiliary_type(data_type: DataType, nullable: bool) -> String {
    let base = match data_type {
        DataType::Int => "Int64",
        DataType::Bool => "Bool",
        DataType::DateTime => "DateTime64(6, 'UTC')",
        DataType::Date => "Date32",
        _ => "String",
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base.to_string()
    }
}

fn auxiliary_column(column: &ontology::AuxiliaryColumn) -> (String, ColumnSpec) {
    let spec = ColumnSpec {
        storage_type: Some(auxiliary_type(column.data_type, column.nullable)),
        default: column.default.clone(),
        codec: column.codec.clone().unwrap_or_default(),
    };
    (column.name.clone(), spec)
}

fn auxiliary_tables(ontology: &Ontology) -> IndexMap<String, TableSpec> {
    ontology
        .auxiliary_tables()
        .iter()
        .map(|table| {
            let mut columns: IndexMap<_, _> = table.columns.iter().map(auxiliary_column).collect();
            if table.include_system_columns {
                columns.extend(system_columns(table.version_type.as_deref()));
            }
            let spec = TableSpec {
                versioned: table.versioned,
                engine: table
                    .engine
                    .clone()
                    .unwrap_or_else(|| replacing(table.version_only_engine)),
                columns,
                sort_key: table.order_by.clone(),
                primary_key: None,
                indexes: IndexMap::new(),
                projections: projections(&table.projections),
                settings: projection_mode(&table.projections)
                    .into_iter()
                    .chain(engine_flags())
                    .collect(),
                ttl: table.ttl.clone(),
            };
            (table.name.clone(), spec)
        })
        .collect()
}

fn dictionaries(ontology: &Ontology) -> IndexMap<String, DictionarySpec> {
    ontology
        .auxiliary_dictionaries()
        .iter()
        .map(|dictionary| {
            let key_type = auxiliary_type(dictionary.key_type.unwrap_or(DataType::Int), false);
            let spec = DictionarySpec {
                source: dictionary.source_table.clone(),
                key: dictionary.key.clone(),
                attributes: std::iter::once((dictionary.key.clone(), key_type))
                    .chain(dictionary.attributes.iter().map(|attribute| {
                        let (name, column) = auxiliary_column(attribute);
                        (name, column.storage_type.unwrap_or_default())
                    }))
                    .collect(),
                layout: dictionary.layout.kind.clone(),
                size_in_cells: dictionary.layout.size_in_cells,
                lifetime: LifetimeSpec {
                    min: dictionary.lifetime.min,
                    max: dictionary.lifetime.max,
                },
            };
            (dictionary.name.clone(), spec)
        })
        .collect()
}

fn views(ontology: &Ontology) -> IndexMap<String, ViewSpec> {
    ontology
        .materialized_views()
        .iter()
        .map(|view| {
            let engine = view.engine.as_ref().map(|name| {
                if view.engine_args.is_empty() {
                    name.clone()
                } else {
                    format!("{name}({})", view.engine_args.join(", "))
                }
            });
            let spec = ViewSpec {
                versioned: view.versioned,
                to: view.to_table.clone(),
                select: view.select_query.clone(),
                engine,
                order_by: view.order_by.clone(),
                populate: view.populate,
            };
            (view.name.clone(), spec)
        })
        .collect()
}
