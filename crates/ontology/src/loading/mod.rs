mod derived;
mod edge;
mod node;
mod schema;

use rust_embed::Embed;
use serde::Deserialize;
use std::path::Path;

use crate::entities::{DomainInfo, EdgeColumn};
use crate::{Ontology, OntologyError};

use derived::DerivedYaml;
pub(crate) use edge::EdgeYaml;
pub(crate) use node::NodeYaml;
use schema::SchemaYaml;

pub(crate) const ONTOLOGY_SCHEMA_FILE: &str = "schema.yaml";

#[derive(Embed)]
#[folder = "$ONTOLOGY_DIR"]
#[include = "**/*.yaml"]
#[include = "**/*.sql.j2"]
struct EmbeddedOntology;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EtlSettings {
    pub watermark: String,
    pub deleted: String,
    pub order_by: Vec<String>,
}

struct PipelineNodeMetadata {
    property_names: std::collections::HashSet<String>,
    enrichment_property_columns: Vec<(String, String)>,
    extract_lookup_source: Option<crate::etl::ClickHouseExtractLookupSource>,
}

pub(crate) trait ReadOntologyFile {
    fn read(&self, path: &str) -> Result<String, OntologyError>;
}

pub(crate) struct DirReader<'a>(pub &'a Path);

impl ReadOntologyFile for DirReader<'_> {
    fn read(&self, path: &str) -> Result<String, OntologyError> {
        let full_path = self.0.join(path);
        std::fs::read_to_string(&full_path).map_err(|e| OntologyError::Io {
            path: full_path.to_string_lossy().to_string(),
            source: e,
        })
    }
}

struct EmbeddedReader;

impl ReadOntologyFile for EmbeddedReader {
    fn read(&self, path: &str) -> Result<String, OntologyError> {
        let file = EmbeddedOntology::get(path).ok_or_else(|| OntologyError::Io {
            path: path.to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("embedded file not found: {}", path),
            ),
        })?;

        String::from_utf8(file.data.to_vec()).map_err(|e| OntologyError::Io {
            path: path.to_string(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
        })
    }
}

/// Every embedded ontology source file as `(relative_path, contents)`, sorted by path.
pub(crate) fn embedded_files() -> Vec<(String, String)> {
    let mut files: Vec<(String, String)> = EmbeddedOntology::iter()
        .filter_map(|path| {
            let file = EmbeddedOntology::get(&path)?;
            let text = String::from_utf8(file.data.to_vec()).ok()?;
            Some((path.to_string(), text))
        })
        .collect();
    files.sort();
    files
}

pub(crate) fn parse_yaml<T: for<'de> Deserialize<'de>>(
    content: &str,
    path: &str,
) -> Result<T, OntologyError> {
    serde_yaml::from_str(content).map_err(|e| OntologyError::Yaml {
        path: path.to_string(),
        source: e,
    })
}

pub(super) fn load_from_dir(dir: &Path) -> Result<Ontology, OntologyError> {
    load_with(&DirReader(dir))
}

pub(super) fn load_embedded() -> Result<Ontology, OntologyError> {
    load_with(&EmbeddedReader)
}

pub(crate) fn load_with(reader: &impl ReadOntologyFile) -> Result<Ontology, OntologyError> {
    let schema_content = reader.read(ONTOLOGY_SCHEMA_FILE)?;
    let schema: SchemaYaml = parse_yaml(&schema_content, ONTOLOGY_SCHEMA_FILE)?;

    let denormalization_entries = schema.settings.denormalization.clone();

    let mut ontology = Ontology::new();
    ontology.schema_version = schema.schema_version.unwrap_or_default();
    ontology.table_prefix = schema.settings.table_prefix.clone();
    ontology.default_edge_table = schema.settings.default_edge_table;
    ontology.default_entity_sort_key = schema.settings.default_entity_sort_key;

    ontology.edge_table_configs = schema
        .settings
        .edge_tables
        .into_iter()
        .map(|(name, cfg)| {
            let storage = cfg.storage.map(|s| crate::entities::EdgeTableStorage {
                index_granularity: s.index_granularity,
                primary_key: s.primary_key,
                columns: s
                    .columns
                    .into_iter()
                    .map(|col| crate::entities::StorageColumn {
                        name: col.name,
                        ch_type: col.ch_type,
                        default: col.default,
                        codec: col.codec,
                    })
                    .collect(),
                indexes: s
                    .indexes
                    .into_iter()
                    .map(node::convert_storage_index)
                    .collect(),
                projections: s
                    .projections
                    .into_iter()
                    .map(|p| node::convert_storage_projection(p, &cfg.sort_key))
                    .collect(),
                denormalized_columns: vec![],
                denormalized_indexes: vec![],
                settings: s.settings,
            });
            let config = crate::EdgeTableConfig {
                sort_key: cfg.sort_key,
                columns: cfg
                    .columns
                    .into_iter()
                    .map(|c| crate::entities::EdgeColumn {
                        name: c.name,
                        data_type: c.data_type,
                    })
                    .collect(),
                storage: storage.unwrap_or_default(),
            };
            (name, config)
        })
        .collect();

    let etl_settings = EtlSettings {
        watermark: schema.settings.etl.default_watermark,
        deleted: schema.settings.etl.default_deleted,
        order_by: schema.settings.etl.default_etl_order_by,
    };
    ontology.etl_settings = etl_settings.clone();
    ontology.internal_column_prefix = schema.settings.internal_column_prefix;

    // Edge table names become SQL identifiers, so restrict to lowercase ASCII and underscores.
    for table_name in ontology.edge_table_configs.keys() {
        if !table_name.starts_with(&ontology.table_prefix) {
            return Err(OntologyError::Validation(format!(
                "edge table '{}' does not start with table_prefix '{}'",
                table_name, ontology.table_prefix
            )));
        }
        if !table_name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '_')
        {
            return Err(OntologyError::Validation(format!(
                "edge table '{}' contains invalid characters (only a-z and _ allowed)",
                table_name
            )));
        }
    }
    if !ontology
        .edge_table_configs
        .contains_key(&ontology.default_edge_table)
    {
        return Err(OntologyError::Validation(format!(
            "default_edge_table '{}' is not defined in edge_tables",
            ontology.default_edge_table
        )));
    }

    let actual_names: Vec<&str> = ontology
        .edge_columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    let expected: &[&str] = crate::constants::EDGE_RESERVED_COLUMNS;
    if actual_names != expected {
        return Err(OntologyError::Validation(format!(
            "default edge table columns {:?} do not match EDGE_RESERVED_COLUMNS {:?}",
            actual_names, expected
        )));
    }

    for (domain_name, domain) in &schema.domains {
        let mut node_names = Vec::new();

        for (node_name, node_path) in &domain.nodes {
            if ontology.nodes.contains_key(node_name) {
                return Err(OntologyError::Validation(format!(
                    "duplicate node definition: '{}'",
                    node_name
                )));
            }

            let content = reader.read(node_path)?;
            let node_def: NodeYaml = parse_yaml(&content, node_path)?;

            let entity = node_def.into_entity(
                node_name.clone(),
                node_path,
                &ontology.default_entity_sort_key,
                &etl_settings,
                &ontology.internal_column_prefix,
                reader,
            )?;

            if !entity.destination_table.starts_with(&ontology.table_prefix) {
                return Err(OntologyError::Validation(format!(
                    "node '{}' has destination_table '{}' which does not start with \
                     table_prefix '{}'",
                    node_name, entity.destination_table, ontology.table_prefix
                )));
            }

            ontology.nodes.insert(node_name.clone(), entity);
            node_names.push(node_name.clone());
        }

        for (derived_name, derived_path) in &domain.derived {
            if ontology.derived_entities.contains_key(derived_name) {
                return Err(OntologyError::Validation(format!(
                    "duplicate derived entity definition: '{derived_name}'"
                )));
            }

            let content = reader.read(derived_path)?;
            let derived_def: DerivedYaml = parse_yaml(&content, derived_path)?;
            let derived = derived_def.into_derived(
                derived_name.clone(),
                derived_path,
                &etl_settings,
                reader,
            )?;
            ontology
                .derived_entities
                .insert(derived_name.clone(), derived);
        }

        node_names.sort();
        ontology.domains.insert(
            domain_name.clone(),
            DomainInfo {
                name: domain_name.clone(),
                description: domain.description.clone().unwrap_or_default(),
                node_names,
            },
        );
    }

    for (edge_name, edge_path) in &schema.edges {
        let content = reader.read(edge_path)?;
        let edge_def: EdgeYaml = parse_yaml(&content, edge_path)?;

        let entities = edge_def.to_entities(edge_name.clone(), ontology.edge_table());

        for entity in &entities {
            if !ontology.nodes.contains_key(&entity.source_kind) {
                return Err(OntologyError::Validation(format!(
                    "edge '{}' references unknown source node '{}'",
                    edge_name, entity.source_kind
                )));
            }
            if !ontology.nodes.contains_key(&entity.target_kind) {
                return Err(OntologyError::Validation(format!(
                    "edge '{}' references unknown target node '{}'",
                    edge_name, entity.target_kind
                )));
            }
            if !ontology
                .edge_table_configs
                .contains_key(&entity.destination_table)
            {
                return Err(OntologyError::Validation(format!(
                    "edge '{}' references unknown edge table '{}'",
                    edge_name, entity.destination_table
                )));
            }
        }

        if ontology.edges.contains_key(edge_name) {
            return Err(OntologyError::Validation(format!(
                "duplicate edge definition: '{edge_name}'"
            )));
        }
        ontology.edges.insert(edge_name.clone(), entities);

        if let Some(desc) = &edge_def.description {
            ontology
                .edge_descriptions
                .insert(edge_name.clone(), desc.clone());
        }

        let (pipelines, reindex_sources) =
            edge_def.into_pipelines(edge_name, edge_path, &etl_settings, reader)?;
        if !pipelines.is_empty() {
            ontology.edge_pipelines.insert(edge_name.clone(), pipelines);
        }
        if !reindex_sources.is_empty() {
            ontology
                .edge_reindex_sources
                .insert(edge_name.clone(), reindex_sources);
        }
    }

    let pipeline_node_metadata = get_pipeline_node_metadata(&ontology);
    resolve_extract_lookups(&mut ontology, &pipeline_node_metadata)?;
    resolve_transform_property_inputs(&mut ontology, &pipeline_node_metadata)?;

    let has_denorm = !denormalization_entries.is_empty();

    for entry in &denormalization_entries {
        let node = ontology.nodes.get(&entry.node).ok_or_else(|| {
            OntologyError::Validation(format!("denormalization: unknown node '{}'", entry.node))
        })?;
        let field = node
            .fields
            .iter()
            .find(|f| f.name == entry.property)
            .ok_or_else(|| {
                OntologyError::Validation(format!(
                    "denormalization: unknown property '{}' on node '{}'",
                    entry.property, entry.node
                ))
            })?;
        let enum_values = field.enum_values.clone();
        let field_column = field
            .column_name()
            .map(str::to_string)
            .unwrap_or_else(|| entry.property.clone());

        let tag_key = entry
            .column_alias
            .as_deref()
            .unwrap_or(&entry.property)
            .to_string();

        for (edge_name, variants) in &ontology.edges {
            for variant in variants {
                let mut directions = Vec::new();
                if variant.source_kind == entry.node {
                    directions.push(crate::entities::DenormDirection::Source);
                }
                if variant.target_kind == entry.node {
                    directions.push(crate::entities::DenormDirection::Target);
                }
                for direction in directions {
                    // Declare the denorm only on edges whose ETL materializes
                    // the column, so the compiler never pushes a tag onto an
                    // edge with empty tags.
                    if !ontology.edge_projects_column(
                        edge_name,
                        &entry.node,
                        direction.clone(),
                        &field_column,
                    ) {
                        continue;
                    }
                    let edge_column = match direction {
                        crate::entities::DenormDirection::Source => "source_tags",
                        crate::entities::DenormDirection::Target => "target_tags",
                    }
                    .to_string();

                    let already_exists = ontology.denormalized_properties.iter().any(|dp| {
                        dp.relationship_kind == *edge_name
                            && dp.node_kind == entry.node
                            && dp.property_name == entry.property
                            && dp.direction == direction
                    });
                    if !already_exists {
                        ontology.denormalized_properties.push(
                            crate::entities::DenormalizedProperty {
                                relationship_kind: edge_name.clone(),
                                node_kind: entry.node.clone(),
                                property_name: entry.property.clone(),
                                direction,
                                edge_column,
                                tag_key: tag_key.clone(),
                                enum_values: enum_values.clone(),
                            },
                        );
                    }
                }
            }
        }
    }

    let auto_columns: Vec<crate::entities::StorageColumn> = if has_denorm {
        vec![
            crate::entities::StorageColumn {
                name: "source_tags".to_string(),
                ch_type: "Array(LowCardinality(String))".to_string(),
                default: None,
                codec: Some(vec!["LZ4".to_string()]),
            },
            crate::entities::StorageColumn {
                name: "target_tags".to_string(),
                ch_type: "Array(LowCardinality(String))".to_string(),
                default: None,
                codec: Some(vec!["LZ4".to_string()]),
            },
        ]
    } else {
        vec![]
    };

    let auto_indexes: Vec<crate::entities::StorageIndex> = if has_denorm {
        vec![
            crate::entities::StorageIndex {
                name: "source_tags_idx".to_string(),
                column: "source_tags".to_string(),
                index_type: "text(tokenizer = 'array')".to_string(),
                granularity: 64,
            },
            crate::entities::StorageIndex {
                name: "target_tags_idx".to_string(),
                column: "target_tags".to_string(),
                index_type: "text(tokenizer = 'array')".to_string(),
                granularity: 64,
            },
        ]
    } else {
        vec![]
    };

    for config in ontology.edge_table_configs.values_mut() {
        config.storage.denormalized_columns = auto_columns.clone();
        config.storage.denormalized_indexes = auto_indexes.clone();
    }

    if let Some(local_db) = schema.settings.local_db {
        for entry in local_db.entities {
            let node = ontology.nodes.get(&entry.name).ok_or_else(|| {
                OntologyError::Validation(format!(
                    "local_db.entities: unknown entity '{}'",
                    entry.name
                ))
            })?;

            let field_names: std::collections::HashSet<&str> =
                node.fields.iter().map(|f| f.name.as_str()).collect();
            for prop in &entry.exclude_properties {
                if !field_names.contains(prop.as_str()) {
                    return Err(OntologyError::Validation(format!(
                        "local_db.entities: exclude_properties entry '{}' \
                         is not a declared property of '{}'",
                        prop, entry.name
                    )));
                }
            }

            ontology
                .local_entities
                .insert(entry.name, entry.exclude_properties);
        }

        if let Some(edge_table) = local_db.edge_table {
            let mut seen = std::collections::HashSet::new();
            for col in &edge_table.columns {
                if !seen.insert(&col.name) {
                    return Err(OntologyError::Validation(format!(
                        "local_db.edge_table: duplicate column name '{}'",
                        col.name
                    )));
                }
            }

            ontology.local_edge_table_name = Some(edge_table.name);
            ontology.local_edge_columns = edge_table
                .columns
                .into_iter()
                .map(|c| EdgeColumn {
                    name: c.name,
                    data_type: c.data_type,
                })
                .collect();
        }
    }

    ontology.auxiliary_tables = schema
        .settings
        .auxiliary_tables
        .into_iter()
        .map(|t| {
            let projections = t
                .projections
                .into_iter()
                .map(|p| node::convert_storage_projection(p, &t.order_by))
                .collect();
            crate::entities::AuxiliaryTable {
                name: t.name,
                versioned: t.versioned,
                columns: t
                    .columns
                    .into_iter()
                    .map(|c| crate::entities::AuxiliaryColumn {
                        name: c.name,
                        data_type: c.data_type,
                        nullable: c.nullable,
                        codec: c.codec,
                        default: c.default,
                    })
                    .collect(),
                order_by: t.order_by,
                version_only_engine: t.version_only_engine,
                version_type: t.version_type,
                projections,
                include_system_columns: t.include_system_columns,
                engine: t.engine,
                ttl: t.ttl,
            }
        })
        .collect();

    let all_table_names: std::collections::HashSet<String> = ontology
        .auxiliary_tables
        .iter()
        .map(|t| t.name.clone())
        .chain(ontology.nodes.values().map(|n| n.destination_table.clone()))
        .chain(ontology.edge_table_configs.keys().cloned())
        .collect();

    ontology.materialized_views = schema
        .settings
        .materialized_views
        .into_iter()
        .map(|mv| {
            match (&mv.to_table, &mv.engine) {
                (None, None) => {
                    return Err(OntologyError::Validation(format!(
                        "materialized_view '{}': must set either `to_table` or `engine`",
                        mv.name
                    )));
                }
                (Some(_), Some(_)) => {
                    return Err(OntologyError::Validation(format!(
                        "materialized_view '{}': `to_table` and `engine` are mutually exclusive",
                        mv.name
                    )));
                }
                _ => {}
            }
            if let Some(ref to_table) = mv.to_table
                && !all_table_names.contains(to_table)
            {
                return Err(OntologyError::Validation(format!(
                    "materialized_view '{}': to_table '{}' is not an ontology-tracked table; \
                     it would be orphaned during schema version cleanup",
                    mv.name, to_table
                )));
            }
            Ok(crate::entities::MaterializedViewDefinition {
                name: mv.name,
                to_table: mv.to_table,
                select_query: mv.select_query,
                engine: mv.engine,
                engine_args: mv.engine_args,
                order_by: mv.order_by,
                populate: mv.populate,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    ontology.refreshable_materialized_views = schema
        .settings
        .refreshable_materialized_views
        .into_iter()
        .map(|view| {
            let select_path = Path::new(&view.select_file);
            if select_path.is_absolute()
                || select_path
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir))
            {
                return Err(OntologyError::Validation(format!(
                    "refreshable_materialized_view '{}': select_file '{}' must stay within the ontology directory",
                    view.name, view.select_file
                )));
            }
            if !all_table_names.contains(&view.append_to) {
                return Err(OntologyError::Validation(format!(
                    "refreshable_materialized_view '{}': append_to '{}' is not an ontology-tracked table",
                    view.name, view.append_to
                )));
            }
            let select_query = reader.read(&view.select_file)?;
            if select_query.trim().is_empty() {
                return Err(OntologyError::Validation(format!(
                    "refreshable_materialized_view '{}': select_file '{}' is empty",
                    view.name, view.select_file
                )));
            }
            Ok(crate::entities::RefreshableMaterializedViewDefinition {
                name: view.name,
                versioned: view.versioned,
                select_query,
                append_to: view.append_to,
                refresh: view.refresh,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    ontology.auxiliary_dictionaries = schema
        .settings
        .auxiliary_dictionaries
        .into_iter()
        .map(|d| crate::entities::AuxiliaryDictionary {
            name: d.name,
            source_table: d.source_table,
            key: d.key,
            key_type: d.key_type,
            attributes: d
                .attributes
                .into_iter()
                .map(|c| crate::entities::AuxiliaryColumn {
                    name: c.name,
                    data_type: c.data_type,
                    nullable: c.nullable,
                    codec: c.codec,
                    default: c.default,
                })
                .collect(),
            layout: crate::entities::DictionaryLayout {
                kind: d.layout.kind,
                size_in_cells: d.layout.size_in_cells,
            },
            lifetime: crate::entities::DictionaryLifetime {
                min: d.lifetime.min,
                max: d.lifetime.max,
            },
        })
        .collect();

    ontology.statistics = schema
        .settings
        .statistics
        .map(|s| -> Result<_, OntologyError> {
            for entry in &s.exclude {
                let node = ontology.nodes.get(&entry.node).ok_or_else(|| {
                    OntologyError::Validation(format!(
                        "statistics.exclude: unknown node '{}'",
                        entry.node
                    ))
                })?;
                let field_names: std::collections::HashSet<&str> =
                    node.fields.iter().map(|f| f.name.as_str()).collect();
                for col in &entry.columns {
                    if !field_names.contains(col.as_str()) {
                        return Err(OntologyError::Validation(format!(
                            "statistics.exclude: unknown column '{}' on node '{}'",
                            col, entry.node
                        )));
                    }
                }
            }
            Ok(crate::entities::StatisticsConfig {
                stats_table: s.stats_table,
                histogram_table: s.histogram_table,
                token_table: s.token_table,
                dictionary: s.dictionary,
                lifetime_min: s.lifetime.min,
                lifetime_max: s.lifetime.max,
                histogram_buckets: s.histogram_buckets,
                top_k_tokens: s.top_k_tokens,
                partition_key: s.partition_key,
                exclude: s
                    .exclude
                    .into_iter()
                    .map(|e| crate::entities::StatisticsExclude {
                        node: e.node,
                        columns: e.columns,
                    })
                    .collect(),
            })
        })
        .transpose()?;

    ontology.partition = schema
        .settings
        .partition
        .map(|p| -> Result<_, OntologyError> {
            let hb = p.strategy.hash_bucket.ok_or_else(|| {
                OntologyError::Validation("partition.strategy must set a strategy block".into())
            })?;
            let mut partitioned_tables: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            for entity in &p.include_entities {
                let node = ontology.nodes.get(entity).ok_or_else(|| {
                    OntologyError::Validation(format!(
                        "partition.include_entities: '{entity}' is not a node entity"
                    ))
                })?;
                if node.global {
                    return Err(OntologyError::Validation(format!(
                        "partition.include_entities: '{entity}' is a global entity with no \
                         traversal_path and cannot be partitioned"
                    )));
                }
                partitioned_tables.insert(node.destination_table.clone());
            }
            for table in &p.include_edge_tables {
                if !ontology.edge_table_configs.contains_key(table) {
                    return Err(OntologyError::Validation(format!(
                        "partition.include_edge_tables: '{table}' is not an edge table"
                    )));
                }
                partitioned_tables.insert(table.clone());
            }
            Ok(crate::entities::PartitionConfig {
                strategy: crate::entities::PartitionStrategy::HashBucket {
                    buckets: hb.buckets,
                    column: hb.column,
                },
                partitioned_tables,
            })
        })
        .transpose()?;

    ontology.traversal_path_lookups = ontology
        .nodes
        .values()
        .flat_map(|node| {
            node.fields.iter().filter_map(move |field| {
                field.traversal_path_lookup.as_ref().map(|spec| {
                    crate::entities::TraversalPathLookup {
                        entity: node.name.clone(),
                        kind: spec.kind,
                        dictionary: spec.dictionary.clone(),
                        source_table: spec.source_table.clone(),
                        key_column: spec.key_column.clone(),
                    }
                })
            })
        })
        .collect();

    ontology.gc_preserve_patterns = schema.settings.gc_preserve_patterns;

    validate_storage_columns(&ontology)?;
    validate_auxiliary_dictionaries(&ontology)?;
    validate_traversal_path_lookups(&ontology)?;
    validate_edge_scope_annotations(&ontology)?;
    validate_derived_emits_registered(&ontology)?;
    validate_etl_edges_match_variants(&ontology)?;
    validate_unique_pipeline_names(&ontology)?;

    Ok(ontology)
}

fn get_pipeline_node_metadata(
    ontology: &crate::Ontology,
) -> std::collections::BTreeMap<String, PipelineNodeMetadata> {
    ontology
        .nodes
        .iter()
        .map(|(name, node)| {
            let enrichment_property_columns = node
                .enrichment_props
                .iter()
                .filter_map(|property_name| {
                    let field = node
                        .fields
                        .iter()
                        .find(|field| field.name == *property_name)?;
                    Some((property_name.clone(), field.column_name()?.to_string()))
                })
                .collect();
            let extract_lookup_source = node.pipelines.first().and_then(|pipeline| {
                let crate::etl::Extract::ClickHouse(extract) = &pipeline.extract;
                extract
                    .tables
                    .first()
                    .map(|table| crate::etl::ClickHouseExtractLookupSource {
                        table: table.clone(),
                        namespaced: !node.global,
                    })
            });
            (
                name.clone(),
                PipelineNodeMetadata {
                    property_names: node.fields.iter().map(|field| field.name.clone()).collect(),
                    enrichment_property_columns,
                    extract_lookup_source,
                },
            )
        })
        .collect()
}

fn get_all_pipelines_mut(
    ontology: &mut crate::Ontology,
) -> impl Iterator<Item = &mut crate::etl::Pipeline> {
    ontology
        .nodes
        .values_mut()
        .flat_map(|node| node.pipelines.iter_mut())
        .chain(ontology.edge_pipelines.values_mut().flatten())
        .chain(
            ontology
                .derived_entities
                .values_mut()
                .flat_map(|derived| derived.pipelines.iter_mut()),
        )
}

fn resolve_extract_lookups(
    ontology: &mut crate::Ontology,
    node_metadata: &std::collections::BTreeMap<String, PipelineNodeMetadata>,
) -> Result<(), OntologyError> {
    for pipeline in get_all_pipelines_mut(ontology) {
        let crate::etl::Extract::ClickHouse(extract) = &mut pipeline.extract;
        let lookup_node_kind_counts = extract
            .lookups
            .iter()
            .filter(|lookup| lookup.output_fields.is_empty())
            .fold(std::collections::HashMap::new(), |mut counts, lookup| {
                *counts.entry(lookup.node_kind.clone()).or_insert(0) += 1;
                counts
            });
        for lookup in &mut extract.lookups {
            let metadata = node_metadata.get(&lookup.node_kind).ok_or_else(|| {
                OntologyError::Validation(format!(
                    "pipeline '{}': extract.lookups node '{}' declares no extract table",
                    pipeline.name, lookup.node_kind
                ))
            })?;
            let extract_lookup_source =
                metadata.extract_lookup_source.as_ref().ok_or_else(|| {
                    OntologyError::Validation(format!(
                        "pipeline '{}': extract.lookups node '{}' declares no extract table",
                        pipeline.name, lookup.node_kind
                    ))
                })?;
            if lookup.output_fields.is_empty() {
                if metadata.enrichment_property_columns.is_empty() {
                    return Err(OntologyError::Validation(format!(
                        "pipeline '{}': node '{}' declares no enrichment_props to enrich from",
                        pipeline.name, lookup.node_kind
                    )));
                }
                let prefix = get_enrichment_field_prefix_for_reference(
                    &lookup.node_kind,
                    &lookup.batch_id_column,
                    lookup_node_kind_counts[&lookup.node_kind],
                );
                for (_, source_column) in &metadata.enrichment_property_columns {
                    lookup
                        .output_fields
                        .insert(source_column.clone(), format!("{prefix}{source_column}"));
                }
            }
            lookup.resolved_source = Some(extract_lookup_source.clone());
        }
        validate_unique_extract_lookup_output_fields(pipeline)?;
    }
    Ok(())
}

fn resolve_transform_property_inputs(
    ontology: &mut crate::Ontology,
    node_metadata: &std::collections::BTreeMap<String, PipelineNodeMetadata>,
) -> Result<(), OntologyError> {
    for pipeline in get_all_pipelines_mut(ontology) {
        let crate::etl::Transform::DataFusion { edges } = &mut pipeline.transform else {
            continue;
        };
        let endpoint_node_kind_counts = edges
            .iter()
            .flat_map(|edge| [&edge.source, &edge.target])
            .filter(|endpoint| endpoint.enrich)
            .filter_map(|endpoint| match &endpoint.kind {
                crate::etl::NodeRefKind::Literal(node_kind) => Some(node_kind.clone()),
                crate::etl::NodeRefKind::Derived { .. } => None,
            })
            .fold(std::collections::HashMap::new(), |mut counts, node_kind| {
                *counts.entry(node_kind).or_insert(0) += 1;
                counts
            });
        for endpoint in edges
            .iter_mut()
            .flat_map(|edge| [&mut edge.source, &mut edge.target])
        {
            let crate::etl::NodeRefKind::Literal(node_kind) = &endpoint.kind else {
                if endpoint.enrich {
                    return Err(OntologyError::Validation(format!(
                        "pipeline '{}': transform endpoint '{}' sets enrich but resolves to a derived node kind",
                        pipeline.name, endpoint.field
                    )));
                }
                if !endpoint.property_inputs.is_empty() {
                    return Err(OntologyError::Validation(format!(
                        "pipeline '{}': derived transform endpoint '{}' cannot declare properties",
                        pipeline.name, endpoint.field
                    )));
                }
                continue;
            };
            let node_kind = node_kind.clone();
            let metadata = node_metadata.get(&node_kind).ok_or_else(|| {
                OntologyError::Validation(format!(
                    "pipeline '{}': transform endpoint '{}' references unknown node '{node_kind}'",
                    pipeline.name, endpoint.field
                ))
            })?;
            if endpoint.enrich {
                if metadata.enrichment_property_columns.is_empty() {
                    return Err(OntologyError::Validation(format!(
                        "pipeline '{}': node '{node_kind}' declares no enrichment_props to enrich from",
                        pipeline.name
                    )));
                }
                let prefix = get_enrichment_field_prefix_for_reference(
                    &node_kind,
                    &endpoint.field,
                    endpoint_node_kind_counts[&node_kind],
                );
                for (property, source_column) in &metadata.enrichment_property_columns {
                    endpoint
                        .property_inputs
                        .insert(property.clone(), format!("{prefix}{source_column}"));
                }
            }
            for property_name in endpoint.property_inputs.keys() {
                if !metadata.property_names.contains(property_name) {
                    return Err(OntologyError::Validation(format!(
                        "pipeline '{}': transform endpoint '{}' references unknown property '{property_name}' on node '{node_kind}'",
                        pipeline.name, endpoint.field
                    )));
                }
            }
        }
    }
    Ok(())
}

fn get_enrichment_field_prefix_for_reference(
    node_kind: &str,
    identity_field: &str,
    node_kind_reference_count: usize,
) -> String {
    if node_kind_reference_count > 1 {
        return format!(
            "{}_",
            identity_field.strip_suffix("_id").unwrap_or(identity_field)
        );
    }

    let mut field_prefix = String::with_capacity(node_kind.len() + 4);
    for (character_index, character) in node_kind.char_indices() {
        if character.is_ascii_uppercase() && character_index != 0 {
            field_prefix.push('_');
        }
        field_prefix.push(character.to_ascii_lowercase());
    }
    field_prefix.push('_');
    field_prefix
}

fn validate_unique_extract_lookup_output_fields(
    pipeline: &crate::etl::Pipeline,
) -> Result<(), OntologyError> {
    let crate::etl::Extract::ClickHouse(extract) = &pipeline.extract;
    let mut output_fields = std::collections::HashSet::new();
    for output_field in extract
        .lookups
        .iter()
        .flat_map(|lookup| lookup.output_fields.values())
    {
        if !output_fields.insert(output_field) {
            return Err(OntologyError::Validation(format!(
                "pipeline '{}': extract.lookups output field '{output_field}' is declared more than once",
                pipeline.name
            )));
        }
    }
    Ok(())
}

fn validate_unique_pipeline_names(ontology: &crate::Ontology) -> Result<(), OntologyError> {
    let mut seen = std::collections::HashSet::new();
    let names = ontology
        .nodes()
        .flat_map(|node| node.pipelines.iter())
        .chain(ontology.edge_etl_configs().map(|(_, pipeline)| pipeline))
        .chain(
            ontology
                .derived_entities()
                .flat_map(|derived| derived.pipelines.iter()),
        )
        .map(|pipeline| pipeline.name.as_str());
    for name in names {
        if !seen.insert(name) {
            return Err(OntologyError::Validation(format!(
                "duplicate pipeline name '{name}': pipeline names are indexer handler names \
                 and checkpoint keys and must be globally unique"
            )));
        }
    }
    Ok(())
}

fn validate_derived_emits_registered(ontology: &crate::Ontology) -> Result<(), OntologyError> {
    for derived in ontology.derived_entities() {
        for edge in &derived.emits {
            if !ontology.has_edge(edge) {
                return Err(OntologyError::Validation(format!(
                    "derived entity '{}' emits '{edge}' but it is not registered in the \
                     edges: map of schema.yaml",
                    derived.name
                )));
            }
        }
    }
    Ok(())
}

fn validate_etl_edges_match_variants(ontology: &crate::Ontology) -> Result<(), OntologyError> {
    let variant_exists = |kind: &str, from: &str, to: &str| {
        ontology.get_edge(kind).is_some_and(|variants| {
            variants
                .iter()
                .any(|v| v.source_kind == from && v.target_kind == to)
        })
    };
    let check = |kind: &str, from: &str, to: &str, declared_at: &str| {
        if variant_exists(kind, from, to) {
            Ok(())
        } else {
            Err(OntologyError::Validation(format!(
                "{declared_at} materializes {from} -[{kind}]-> {to}, but '{kind}' declares no \
                 matching variant; add `from_node: {from}` / `to_node: {to}` to the edge YAML"
            )))
        }
    };

    let ref_kinds = |r: &crate::etl::NodeRef| -> Vec<String> {
        match &r.kind {
            crate::etl::NodeRefKind::Literal(t) => vec![t.clone()],
            crate::etl::NodeRefKind::Derived { mapping, .. } => mapping.values().cloned().collect(),
        }
    };

    let pipelines = ontology
        .nodes()
        .flat_map(|node| node.pipelines.iter().map(move |p| (node.name.as_str(), p)))
        .chain(ontology.edge_etl_configs())
        .chain(ontology.derived_entities().flat_map(|derived| {
            derived
                .pipelines
                .iter()
                .map(move |p| (derived.name.as_str(), p))
        }));

    for (owner, pipeline) in pipelines {
        for mapping in pipeline.transform.edges() {
            for from in ref_kinds(&mapping.source) {
                for to in ref_kinds(&mapping.target) {
                    check(
                        &mapping.label,
                        &from,
                        &to,
                        &format!("pipeline '{}' on '{owner}'", pipeline.name),
                    )?;
                }
            }
        }
    }

    Ok(())
}

fn validate_traversal_path_lookups(ontology: &crate::Ontology) -> Result<(), OntologyError> {
    for lookup in ontology.traversal_path_lookups() {
        let node = ontology
            .nodes()
            .find(|n| n.destination_table == lookup.source_table)
            .ok_or_else(|| {
                OntologyError::Validation(format!(
                    "traversal_path_lookup on '{}': source_table '{}' is not a known node table",
                    lookup.entity, lookup.source_table
                ))
            })?;

        if !node
            .storage
            .columns
            .iter()
            .any(|c| c.name.trim_matches('`') == lookup.key_column)
        {
            return Err(OntologyError::Validation(format!(
                "traversal_path_lookup on '{}': key_column '{}' is not a storage column on '{}'",
                lookup.entity, lookup.key_column, lookup.source_table
            )));
        }

        if let Some(dict) = &lookup.dictionary
            && !ontology
                .auxiliary_dictionaries()
                .iter()
                .any(|d| &d.name == dict)
        {
            return Err(OntologyError::Validation(format!(
                "traversal_path_lookup on '{}': dictionary '{}' is not a declared auxiliary dictionary",
                lookup.entity, dict
            )));
        }
    }

    Ok(())
}

fn validate_auxiliary_dictionaries(ontology: &crate::Ontology) -> Result<(), OntologyError> {
    for dict in ontology.auxiliary_dictionaries() {
        let node = ontology
            .nodes()
            .find(|n| n.destination_table == dict.source_table)
            .ok_or_else(|| {
                OntologyError::Validation(format!(
                    "dictionary '{}': source_table '{}' is not a known node table",
                    dict.name, dict.source_table
                ))
            })?;

        if !node
            .storage
            .columns
            .iter()
            .any(|c| c.name.trim_matches('`') == dict.key)
        {
            return Err(OntologyError::Validation(format!(
                "dictionary '{}': key '{}' is not a storage column on '{}'",
                dict.name, dict.key, dict.source_table
            )));
        }
    }

    Ok(())
}

/// Every node's storage columns must correspond 1:1 with its non-virtual
/// properties, catching drift between the logical and physical schema.
fn validate_storage_columns(ontology: &crate::Ontology) -> Result<(), OntologyError> {
    for node in ontology.nodes() {
        if node.storage.columns.is_empty() {
            continue;
        }

        let property_names: Vec<&str> = node
            .fields
            .iter()
            .filter(|f| !f.is_virtual())
            .map(|f| f.name.as_str())
            .collect();

        let storage_names: Vec<String> = node
            .storage
            .columns
            .iter()
            .map(|c| c.name.trim_matches('`').to_string())
            .collect();

        for storage_col in &storage_names {
            if !property_names.contains(&storage_col.as_str()) {
                return Err(OntologyError::Validation(format!(
                    "{}: storage column '{}' has no matching property",
                    node.name, storage_col
                )));
            }
        }

        for prop in &property_names {
            if !storage_names.iter().any(|s| s == prop) {
                return Err(OntologyError::Validation(format!(
                    "{}: property '{}' has no matching storage column",
                    node.name, prop
                )));
            }
        }
    }

    Ok(())
}

/// `namespace_anchor` variants must have an FK column and must target a
/// namespace anchor (an entity with a `traversal_path_lookup`). Also enforces
/// that the same FK column name always maps to the same anchor entity.
fn validate_edge_scope_annotations(ontology: &crate::Ontology) -> Result<(), OntologyError> {
    use crate::entities::EdgeVariantScope;
    use std::collections::HashMap;

    let mut fk_to_anchor: HashMap<&str, &str> = HashMap::new();

    for edge in ontology.edges() {
        if edge.scope == Some(EdgeVariantScope::NamespaceAnchor) {
            if edge.fk_column.is_none() {
                return Err(OntologyError::Validation(format!(
                    "{} ({}→{}): scope 'namespace_anchor' requires fk_column",
                    edge.relationship_kind, edge.source_kind, edge.target_kind
                )));
            }
            if !ontology.is_anchor(&edge.target_kind) {
                return Err(OntologyError::Validation(format!(
                    "{} ({}→{}): scope 'namespace_anchor' requires target '{}' \
                     to be a namespace anchor (have a traversal_path_lookup)",
                    edge.relationship_kind, edge.source_kind, edge.target_kind, edge.target_kind
                )));
            }
            if let Some(fk) = edge.fk_column.as_deref() {
                if let Some(&existing) = fk_to_anchor.get(fk) {
                    if existing != edge.target_kind.as_str() {
                        return Err(OntologyError::Validation(format!(
                            "FK column '{}' maps to both '{}' and '{}' as namespace_anchor targets",
                            fk, existing, edge.target_kind
                        )));
                    }
                } else {
                    fk_to_anchor.insert(fk, &edge.target_kind);
                }
            }
        }

        if edge.scope == Some(EdgeVariantScope::SameNamespace) {
            for endpoint in [edge.source_kind.as_str(), edge.target_kind.as_str()] {
                if !ontology.is_path_scopable(endpoint) {
                    return Err(OntologyError::Validation(format!(
                        "{} ({}→{}): scope 'same_namespace' requires both endpoints \
                         to be path-scopable, but '{}' is not. Use 'prune_to_source' or \
                         'prune_to_target' so the prefix scopes the edge without propagating.",
                        edge.relationship_kind, edge.source_kind, edge.target_kind, endpoint
                    )));
                }
            }
        }

        let pruned = match edge.scope {
            Some(EdgeVariantScope::PruneToSource) => {
                Some(("prune_to_source", edge.source_kind.as_str()))
            }
            Some(EdgeVariantScope::PruneToTarget) => {
                Some(("prune_to_target", edge.target_kind.as_str()))
            }
            _ => None,
        };
        if let Some((label, named)) = pruned
            && !ontology.is_path_scopable(named)
        {
            return Err(OntologyError::Validation(format!(
                "{} ({}→{}): scope '{}' requires the named endpoint '{}' to be path-scopable",
                edge.relationship_kind, edge.source_kind, edge.target_kind, label, named
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::{DerivedEntity, EdgeEntity, Field};
    use crate::etl::{
        ClickHouseExtractLookup, ClickHouseExtractLookupSource, EdgeMapping, EtlScope, Extract,
        NodeRef, NodeRefKind, Pipeline, Transform,
    };
    use indexmap::IndexMap;
    use std::collections::BTreeMap;

    fn system_note(emits: &[&str]) -> DerivedEntity {
        DerivedEntity {
            name: "SystemNote".to_string(),
            emits: emits.iter().map(|s| s.to_string()).collect(),
            pipelines: vec![Pipeline {
                name: "SystemNote".to_string(),
                scope: EtlScope::Namespaced,
                extract: Extract::ClickHouse(crate::etl::ClickHouseExtract {
                    tables: vec!["siphon_notes".to_string()],
                    order_by: vec![],
                    watermark: "w".to_string(),
                    deleted: "d".to_string(),
                    query: crate::etl::ExtractQuery::Generated { filter: None },
                    lookups: vec![],
                }),
                transform: Transform::Rust("system_notes".to_string()),
            }],
            reindex_on: Vec::new(),
        }
    }

    #[test]
    fn unregistered_derived_emit_is_rejected() {
        let mut ontology = crate::Ontology::new().with_edges(["MENTIONS"]);
        ontology.derived_entities.insert(
            "SystemNote".to_string(),
            system_note(&["MENTIONS", "GHOST_EDGE"]),
        );

        let err = validate_derived_emits_registered(&ontology).unwrap_err();
        assert!(
            err.to_string().contains("GHOST_EDGE") && err.to_string().contains("SystemNote"),
            "got: {err}"
        );
    }

    #[test]
    fn duplicate_pipeline_name_across_entities_is_rejected() {
        let mut ontology = crate::Ontology::new();
        ontology
            .derived_entities
            .insert("SystemNote".to_string(), system_note(&[]));
        ontology
            .derived_entities
            .insert("SystemNoteCopy".to_string(), system_note(&[]));

        let err = validate_unique_pipeline_names(&ontology).unwrap_err();
        assert!(err.to_string().contains("SystemNote"), "got: {err}");
    }

    #[test]
    fn registered_derived_emits_pass() {
        let mut ontology = crate::Ontology::new().with_edges(["MENTIONS", "GHOST_EDGE"]);
        ontology.derived_entities.insert(
            "SystemNote".to_string(),
            system_note(&["MENTIONS", "GHOST_EDGE"]),
        );

        assert!(validate_derived_emits_registered(&ontology).is_ok());
    }

    fn variant(from: &str, to: &str) -> EdgeEntity {
        EdgeEntity {
            source_kind: from.to_string(),
            target_kind: to.to_string(),
            ..Default::default()
        }
    }

    fn literal_ref(field: &str, kind: &str) -> NodeRef {
        NodeRef {
            field: field.to_string(),
            kind: NodeRefKind::Literal(kind.to_string()),
            property_inputs: IndexMap::new(),
            enrich: false,
        }
    }

    fn edge_mapping(label: &str, source: NodeRef, target: NodeRef) -> EdgeMapping {
        EdgeMapping {
            label: label.to_string(),
            source,
            target,
            array_field: None,
            mutable: false,
        }
    }

    fn pipeline_with_edges(name: &str, edges: Vec<EdgeMapping>) -> Pipeline {
        Pipeline {
            name: name.to_string(),
            scope: EtlScope::Namespaced,
            extract: Extract::ClickHouse(crate::etl::ClickHouseExtract {
                tables: vec!["siphon_x".to_string()],
                order_by: vec![],
                watermark: "w".to_string(),
                deleted: "d".to_string(),
                query: crate::etl::ExtractQuery::Generated { filter: None },
                lookups: vec![],
            }),
            transform: Transform::DataFusion { edges },
        }
    }

    fn add_extract_lookup(pipeline: &mut Pipeline, node_kind: &str) {
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.lookups.push(ClickHouseExtractLookup {
            node_kind: node_kind.to_string(),
            batch_id_column: "author_id".to_string(),
            output_fields: IndexMap::from([("state".to_string(), "user_state".to_string())]),
            resolved_source: None,
        });
    }

    fn resolve_extract_lookups_for_test(
        ontology: &mut crate::Ontology,
    ) -> Result<(), OntologyError> {
        let node_metadata = get_pipeline_node_metadata(ontology);
        resolve_extract_lookups(ontology, &node_metadata)
    }

    fn resolve_transform_property_inputs_for_test(
        ontology: &mut crate::Ontology,
    ) -> Result<(), OntologyError> {
        let node_metadata = get_pipeline_node_metadata(ontology);
        resolve_transform_property_inputs(ontology, &node_metadata)
    }

    #[test]
    fn enrichment_contract_rejects_collision_with_explicit_lookup_field() {
        let mut ontology = crate::Ontology::new()
            .with_nodes(["Note", "User"])
            .with_fields("User", [("state", crate::DataType::String)]);
        ontology.nodes.get_mut("User").unwrap().enrichment_props = vec!["state".to_string()];

        let mut pipeline = pipeline_with_edges("note_pipeline", vec![]);
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.lookups.push(ClickHouseExtractLookup {
            node_kind: "User".to_string(),
            batch_id_column: "reviewer_id".to_string(),
            output_fields: IndexMap::from([("state".to_string(), "user_state".to_string())]),
            resolved_source: None,
        });
        extract.lookups.push(ClickHouseExtractLookup {
            node_kind: "User".to_string(),
            batch_id_column: "author_id".to_string(),
            output_fields: IndexMap::new(),
            resolved_source: None,
        });
        ontology.nodes.get_mut("Note").unwrap().pipelines = vec![pipeline];
        ontology.nodes.get_mut("User").unwrap().pipelines =
            vec![pipeline_with_edges("user_pipeline", vec![])];

        let error = resolve_extract_lookups_for_test(&mut ontology)
            .expect_err("expanded lookup fields should be unique");
        assert!(
            error
                .to_string()
                .contains("output field 'user_state' is declared more than once"),
            "got: {error}"
        );
    }

    #[test]
    fn extract_and_transform_enrichment_contracts_expand_independently() {
        let mut ontology = crate::Ontology::new()
            .with_nodes(["Note", "User"])
            .with_fields("User", [("state", crate::DataType::String)]);
        ontology.nodes.get_mut("User").unwrap().enrichment_props = vec!["state".to_string()];

        let mut extract_pipeline = pipeline_with_edges("extract_pipeline", vec![]);
        let Extract::ClickHouse(extract) = &mut extract_pipeline.extract;
        extract.lookups.push(ClickHouseExtractLookup {
            node_kind: "User".to_string(),
            batch_id_column: "author_id".to_string(),
            output_fields: IndexMap::new(),
            resolved_source: None,
        });

        let mut enriched_source = literal_ref("author_id", "User");
        enriched_source.enrich = true;
        let transform_pipeline = pipeline_with_edges(
            "transform_pipeline",
            vec![edge_mapping(
                "AUTHORED",
                enriched_source,
                literal_ref("id", "Note"),
            )],
        );
        ontology.nodes.get_mut("Note").unwrap().pipelines =
            vec![extract_pipeline, transform_pipeline];
        ontology.nodes.get_mut("User").unwrap().pipelines =
            vec![pipeline_with_edges("user_pipeline", vec![])];

        resolve_extract_lookups_for_test(&mut ontology).expect("extract contract should expand");
        resolve_transform_property_inputs_for_test(&mut ontology)
            .expect("transform contract should expand");

        let extract_pipeline = &ontology.nodes["Note"].pipelines[0];
        let Extract::ClickHouse(extract) = &extract_pipeline.extract;
        assert_eq!(
            extract.lookups[0].output_fields,
            IndexMap::from([("state".to_string(), "user_state".to_string())])
        );

        let transform_pipeline = &ontology.nodes["Note"].pipelines[1];
        assert_eq!(
            transform_pipeline.transform.edges()[0]
                .source
                .property_inputs,
            IndexMap::from([("state".to_string(), "user_state".to_string())])
        );
    }

    fn node_with_edges(
        node: &str,
        edges: Vec<EdgeMapping>,
        edge_variants: (&str, Vec<EdgeEntity>),
    ) -> crate::Ontology {
        let mut ontology = crate::Ontology::new().with_nodes([node]);
        ontology
            .edges
            .insert(edge_variants.0.to_string(), edge_variants.1);
        ontology.nodes.get_mut(node).unwrap().pipelines =
            vec![pipeline_with_edges(&format!("{node}_pipeline"), edges)];
        ontology
    }

    #[test]
    fn extract_lookup_resolves_to_node_extract_table() {
        let mut ontology = crate::Ontology::new().with_nodes(["Note", "User"]);
        let mut note_pipeline = pipeline_with_edges(
            "note_pipeline",
            vec![edge_mapping(
                "AUTHORED",
                literal_ref("author_id", "User"),
                literal_ref("id", "Note"),
            )],
        );
        add_extract_lookup(&mut note_pipeline, "User");
        ontology.nodes.get_mut("Note").unwrap().pipelines = vec![note_pipeline];
        let user = ontology.nodes.get_mut("User").unwrap();
        user.pipelines = vec![pipeline_with_edges("user_pipeline", vec![])];
        user.global = true;

        resolve_extract_lookups_for_test(&mut ontology).unwrap();

        let Extract::ClickHouse(extract) = &ontology.nodes["Note"].pipelines[0].extract;
        assert_eq!(
            extract.lookups[0].resolved_source,
            Some(ClickHouseExtractLookupSource {
                table: "siphon_x".to_string(),
                namespaced: false,
            })
        );
    }

    #[test]
    fn extract_lookup_without_node_extract_table_is_rejected() {
        let mut ontology = crate::Ontology::new().with_nodes(["Note", "User"]);
        let mut note_pipeline = pipeline_with_edges(
            "note_pipeline",
            vec![edge_mapping(
                "AUTHORED",
                literal_ref("author_id", "User"),
                literal_ref("id", "Note"),
            )],
        );
        add_extract_lookup(&mut note_pipeline, "User");
        ontology.nodes.get_mut("Note").unwrap().pipelines = vec![note_pipeline];

        let msg = resolve_extract_lookups_for_test(&mut ontology)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("declares no extract table"), "got: {msg}");
    }

    #[test]
    fn transform_property_input_requires_known_node_property() {
        let mut source = literal_ref("author_id", "User");
        source
            .property_inputs
            .insert("missing".to_string(), "user_missing".to_string());
        let mut ontology = crate::Ontology::new().with_nodes(["Note", "User"]);
        ontology.nodes.get_mut("User").unwrap().fields = vec![Field {
            name: "state".to_string(),
            ..Default::default()
        }];
        ontology.nodes.get_mut("Note").unwrap().pipelines = vec![pipeline_with_edges(
            "note_pipeline",
            vec![edge_mapping("AUTHORED", source, literal_ref("id", "Note"))],
        )];

        let message = resolve_transform_property_inputs_for_test(&mut ontology)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("unknown property 'missing' on node 'User'"),
            "got: {message}"
        );
    }

    #[test]
    fn transform_property_input_requires_known_literal_node() {
        let mut ontology = crate::Ontology::new().with_nodes(["Note"]);
        ontology.nodes.get_mut("Note").unwrap().pipelines = vec![pipeline_with_edges(
            "note_pipeline",
            vec![edge_mapping(
                "AUTHORED",
                literal_ref("author_id", "MissingUser"),
                literal_ref("id", "Note"),
            )],
        )];

        let message = resolve_transform_property_inputs_for_test(&mut ontology)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("references unknown node 'MissingUser'"),
            "got: {message}"
        );
    }

    #[test]
    fn derived_transform_endpoint_rejects_property_inputs() {
        let derived_source = NodeRef {
            field: "noteable_id".to_string(),
            kind: NodeRefKind::Derived {
                column: "noteable_type".to_string(),
                mapping: BTreeMap::from([("Issue".to_string(), "Issue".to_string())]),
            },
            property_inputs: IndexMap::from([("state".to_string(), "noteable_state".to_string())]),
            enrich: false,
        };
        let mut ontology = crate::Ontology::new().with_nodes(["Note"]);
        ontology.nodes.get_mut("Note").unwrap().pipelines = vec![pipeline_with_edges(
            "note_pipeline",
            vec![edge_mapping(
                "BELONGS_TO",
                literal_ref("id", "Note"),
                derived_source,
            )],
        )];

        let message = resolve_transform_property_inputs_for_test(&mut ontology)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("derived transform endpoint 'noteable_id' cannot declare properties"),
            "got: {message}"
        );
    }

    #[test]
    fn node_edge_without_variant_is_rejected() {
        let ontology = node_with_edges(
            "Dependency",
            vec![edge_mapping(
                "IN_PROJECT",
                literal_ref("id", "Dependency"),
                literal_ref("project_id", "Project"),
            )],
            ("IN_PROJECT", vec![variant("Project", "Dependency")]),
        );

        let msg = validate_etl_edges_match_variants(&ontology)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("on 'Dependency'") && msg.contains("Dependency -[IN_PROJECT]-> Project"),
            "got: {msg}"
        );
    }

    #[test]
    fn node_edge_requires_variant_in_declared_direction() {
        let ontology = node_with_edges(
            "Job",
            vec![edge_mapping(
                "AUTO_CANCELED_BY",
                literal_ref("auto_canceled_by_id", "Pipeline"),
                literal_ref("id", "Job"),
            )],
            ("AUTO_CANCELED_BY", vec![variant("Job", "Pipeline")]),
        );

        let msg = validate_etl_edges_match_variants(&ontology)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("Pipeline -[AUTO_CANCELED_BY]-> Job"),
            "got: {msg}"
        );
    }

    #[test]
    fn polymorphic_edge_requires_variant_per_target() {
        let mapped = || {
            edge_mapping(
                "BELONGS_TO",
                literal_ref("id", "Note"),
                NodeRef {
                    field: "noteable_id".to_string(),
                    property_inputs: IndexMap::new(),
                    enrich: false,
                    kind: NodeRefKind::Derived {
                        column: "noteable_type".to_string(),
                        mapping: BTreeMap::from([
                            ("Issue".to_string(), "Issue".to_string()),
                            ("MergeRequest".to_string(), "MergeRequest".to_string()),
                        ]),
                    },
                },
            )
        };

        let ontology = node_with_edges(
            "Note",
            vec![mapped()],
            ("BELONGS_TO", vec![variant("Note", "Issue")]),
        );
        let msg = validate_etl_edges_match_variants(&ontology)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("Note -[BELONGS_TO]-> MergeRequest"),
            "got: {msg}"
        );

        let ontology = node_with_edges(
            "Note",
            vec![mapped()],
            (
                "BELONGS_TO",
                vec![variant("Note", "Issue"), variant("Note", "MergeRequest")],
            ),
        );
        validate_etl_edges_match_variants(&ontology).expect("all mapped targets have variants");
    }

    #[test]
    fn edge_pipeline_endpoints_without_variant_are_rejected() {
        let mut ontology = crate::Ontology::new();
        ontology
            .edges
            .insert("CONTAINS".to_string(), vec![variant("Project", "Issue")]);
        ontology.edge_pipelines.insert(
            "CONTAINS".to_string(),
            vec![pipeline_with_edges(
                "CONTAINS_siphon_project_links",
                vec![edge_mapping(
                    "CONTAINS",
                    literal_ref("project_id", "Project"),
                    literal_ref("target_id", "MergeRequest"),
                )],
            )],
        );

        let msg = validate_etl_edges_match_variants(&ontology)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("CONTAINS_siphon_project_links")
                && msg.contains("Project -[CONTAINS]-> MergeRequest"),
            "got: {msg}"
        );
    }

    #[test]
    fn etl_edges_with_matching_variants_pass() {
        let mut ontology = node_with_edges(
            "Job",
            vec![edge_mapping(
                "AUTO_CANCELED_BY",
                literal_ref("auto_canceled_by_id", "Pipeline"),
                literal_ref("id", "Job"),
            )],
            ("AUTO_CANCELED_BY", vec![variant("Pipeline", "Job")]),
        );
        ontology.edges.insert(
            "IN_PROJECT".to_string(),
            vec![variant("Dependency", "Project")],
        );
        ontology.edge_pipelines.insert(
            "IN_PROJECT".to_string(),
            vec![pipeline_with_edges(
                "IN_PROJECT_siphon_project_dependencies",
                vec![edge_mapping(
                    "IN_PROJECT",
                    literal_ref("dependency_id", "Dependency"),
                    literal_ref("project_id", "Project"),
                )],
            )],
        );

        validate_etl_edges_match_variants(&ontology).expect("matching variants should load");
    }
}
