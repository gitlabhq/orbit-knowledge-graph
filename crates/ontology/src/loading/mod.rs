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

#[derive(Embed)]
#[folder = "$ONTOLOGY_DIR"]
struct EmbeddedOntology;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EtlSettings {
    pub watermark: String,
    pub deleted: String,
    pub order_by: Vec<String>,
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
    let schema_content = reader.read("schema.yaml")?;
    let schema: SchemaYaml = parse_yaml(&schema_content, "schema.yaml")?;

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
                &ontology.default_entity_sort_key,
                &etl_settings,
                &ontology.internal_column_prefix,
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
            let derived = derived_def.into_derived(derived_name.clone(), &etl_settings)?;
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

        let etl_configs = edge_def.into_etl_configs(edge_name, &etl_settings)?;
        if !etl_configs.is_empty() {
            ontology
                .edge_etl_configs
                .insert(edge_name.clone(), etl_configs);
        }
    }

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
                    if !ontology.edge_projects_column(edge_name, direction.clone(), &field_column) {
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

    Ok(ontology)
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

    for node in ontology.nodes() {
        let Some(etl) = &node.etl else { continue };
        for (column, mapping) in etl.edge_mappings() {
            let kind = &mapping.relationship_kind;
            let targets: Vec<&str> = match &mapping.target {
                crate::etl::EdgeTarget::Literal(target) => vec![target.as_str()],
                crate::etl::EdgeTarget::Column { type_mapping, .. } => {
                    type_mapping.values().map(String::as_str).collect()
                }
            };
            for target in targets {
                let (from, to) = match mapping.direction {
                    crate::etl::EdgeDirection::Outgoing => (node.name.as_str(), target),
                    crate::etl::EdgeDirection::Incoming => (target, node.name.as_str()),
                };
                check(
                    kind,
                    from,
                    to,
                    &format!("node '{}': etl edge on column '{column}'", node.name),
                )?;
            }
        }
    }

    let endpoint_types = |endpoint: &crate::entities::EdgeEndpoint| -> Vec<String> {
        match &endpoint.node_type {
            crate::entities::EdgeEndpointType::Literal(t) => vec![t.clone()],
            crate::entities::EdgeEndpointType::Column { type_mapping, .. } => {
                type_mapping.values().cloned().collect()
            }
        }
    };
    for (kind, configs) in &ontology.edge_etl_configs {
        for config in configs {
            for from in endpoint_types(&config.from) {
                for to in endpoint_types(&config.to) {
                    check(
                        kind,
                        &from,
                        &to,
                        &format!("edge '{kind}': etl from source '{}'", config.source),
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

/// Every node's storage columns must correspond 1:1 with its non-virtual properties.
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::entities::{
        DerivedEntity, EdgeEndpoint, EdgeEndpointType, EdgeEntity, EdgeSourceEtlConfig,
    };
    use crate::etl::{EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig, EtlScope};

    fn system_note(emits: &[&str]) -> DerivedEntity {
        DerivedEntity {
            name: "SystemNote".to_string(),
            emits: emits.iter().map(|s| s.to_string()).collect(),
            transform: "system_notes".to_string(),
            etl: EtlConfig::Table {
                scope: EtlScope::Namespaced,
                source: "siphon_notes".to_string(),
                watermark: "w".to_string(),
                deleted: "d".to_string(),
                order_by: vec![],
                reindex_on: Vec::new(),
                edges: BTreeMap::new(),
            },
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

    fn fk_mapping(kind: &str, target: EdgeTarget, direction: EdgeDirection) -> EdgeMapping {
        EdgeMapping {
            target,
            relationship_kind: kind.to_string(),
            direction,
            delimiter: None,
            array_field: None,
            array: false,
            mutable: false,
        }
    }

    fn ontology_with_node_edge(
        node: &str,
        fk_column: &str,
        mapping: EdgeMapping,
        edge_variants: (&str, Vec<EdgeEntity>),
    ) -> crate::Ontology {
        let mut ontology = crate::Ontology::new().with_nodes([node]);
        ontology
            .edges
            .insert(edge_variants.0.to_string(), edge_variants.1);
        ontology.nodes.get_mut(node).unwrap().etl = Some(EtlConfig::Table {
            scope: EtlScope::Namespaced,
            source: format!("siphon_{}", node.to_lowercase()),
            watermark: "w".to_string(),
            deleted: "d".to_string(),
            order_by: vec![],
            reindex_on: vec![],
            edges: BTreeMap::from([(fk_column.to_string(), vec![mapping])]),
        });
        ontology
    }

    fn edge_etl_config(source: &str, from: (&str, &str), to: (&str, &str)) -> EdgeSourceEtlConfig {
        let endpoint = |(id, node_type): (&str, &str)| EdgeEndpoint {
            id_column: id.to_string(),
            node_type: EdgeEndpointType::Literal(node_type.to_string()),
            enrich: vec![],
        };
        EdgeSourceEtlConfig {
            scope: EtlScope::Namespaced,
            source: source.to_string(),
            watermark: "w".to_string(),
            deleted: "d".to_string(),
            order_by: vec![],
            filter: None,
            reindex_on: vec![],
            from: endpoint(from),
            to: endpoint(to),
        }
    }

    #[test]
    fn node_etl_edge_without_variant_is_rejected() {
        let ontology = ontology_with_node_edge(
            "Dependency",
            "project_id",
            fk_mapping(
                "IN_PROJECT",
                EdgeTarget::Literal("Project".to_string()),
                EdgeDirection::Outgoing,
            ),
            ("IN_PROJECT", vec![variant("Project", "Dependency")]),
        );

        let msg = validate_etl_edges_match_variants(&ontology)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("node 'Dependency'")
                && msg.contains("column 'project_id'")
                && msg.contains("Dependency -[IN_PROJECT]-> Project"),
            "got: {msg}"
        );
    }

    #[test]
    fn incoming_node_etl_edge_requires_flipped_variant() {
        // `direction: incoming` materializes Pipeline -> Job, so the un-flipped
        // Job -> Pipeline variant must not satisfy the validation.
        let ontology = ontology_with_node_edge(
            "Job",
            "auto_canceled_by_id",
            fk_mapping(
                "AUTO_CANCELED_BY",
                EdgeTarget::Literal("Pipeline".to_string()),
                EdgeDirection::Incoming,
            ),
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
    fn type_mapped_etl_edge_requires_variant_per_target() {
        let mapped = || {
            fk_mapping(
                "BELONGS_TO",
                EdgeTarget::Column {
                    column: "noteable_type".to_string(),
                    type_mapping: BTreeMap::from([
                        ("Issue".to_string(), "Issue".to_string()),
                        ("MergeRequest".to_string(), "MergeRequest".to_string()),
                    ]),
                },
                EdgeDirection::Outgoing,
            )
        };

        let ontology = ontology_with_node_edge(
            "Note",
            "noteable_id",
            mapped(),
            ("BELONGS_TO", vec![variant("Note", "Issue")]),
        );
        let msg = validate_etl_edges_match_variants(&ontology)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("Note -[BELONGS_TO]-> MergeRequest"),
            "got: {msg}"
        );

        let ontology = ontology_with_node_edge(
            "Note",
            "noteable_id",
            mapped(),
            (
                "BELONGS_TO",
                vec![variant("Note", "Issue"), variant("Note", "MergeRequest")],
            ),
        );
        validate_etl_edges_match_variants(&ontology).expect("all mapped targets have variants");
    }

    #[test]
    fn edge_etl_endpoints_without_variant_are_rejected() {
        let mut ontology = crate::Ontology::new();
        ontology
            .edges
            .insert("CONTAINS".to_string(), vec![variant("Project", "Issue")]);
        ontology.edge_etl_configs.insert(
            "CONTAINS".to_string(),
            vec![edge_etl_config(
                "siphon_project_links",
                ("project_id", "Project"),
                ("target_id", "MergeRequest"),
            )],
        );

        let msg = validate_etl_edges_match_variants(&ontology)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("edge 'CONTAINS'")
                && msg.contains("siphon_project_links")
                && msg.contains("Project -[CONTAINS]-> MergeRequest"),
            "got: {msg}"
        );
    }

    #[test]
    fn etl_edges_with_matching_variants_pass() {
        let mut ontology = ontology_with_node_edge(
            "Job",
            "auto_canceled_by_id",
            fk_mapping(
                "AUTO_CANCELED_BY",
                EdgeTarget::Literal("Pipeline".to_string()),
                EdgeDirection::Incoming,
            ),
            ("AUTO_CANCELED_BY", vec![variant("Pipeline", "Job")]),
        );
        ontology.edges.insert(
            "IN_PROJECT".to_string(),
            vec![variant("Dependency", "Project")],
        );
        ontology.edge_etl_configs.insert(
            "IN_PROJECT".to_string(),
            vec![edge_etl_config(
                "siphon_project_dependencies",
                ("dependency_id", "Dependency"),
                ("project_id", "Project"),
            )],
        );

        validate_etl_edges_match_variants(&ontology).expect("matching variants should load");
    }
}
