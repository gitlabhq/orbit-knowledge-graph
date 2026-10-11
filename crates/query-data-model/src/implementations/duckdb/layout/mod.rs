use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct DuckDb;

impl crate::RelationalBackend for DuckDb {
    type StorageType = ontology::DataType;
    type TableOptions = ();
    type ColumnOptions = ();
    type Metadata = Metadata;
}

pub type Table = crate::layout::relational::Table<DuckDb>;
pub type Column = crate::layout::relational::Column<DuckDb>;

#[derive(Debug)]
pub struct Metadata {
    pub(crate) entities: Vec<crate::implementations::EntityFacts>,
    pub edge_table: String,
    pub entity_tables: HashMap<String, String>,
}

impl crate::Relational<DuckDb> {
    pub fn derive(ontology: &ontology::Ontology) -> Self {
        let edge_table = ontology
            .local_edge_table_name()
            .unwrap_or_else(|| ontology.edge_table())
            .to_string();
        let edge = Table {
            name: edge_table.clone(),
            columns: ontology
                .local_edge_columns()
                .iter()
                .map(|column| Column {
                    name: column.name.clone(),
                    storage_type: column.data_type,
                    default: None,
                    options: (),
                })
                .collect(),
            column_types: ontology
                .local_edge_columns()
                .iter()
                .map(|column| (column.name.clone(), column.data_type))
                .collect(),
            sort_key: ontology
                .sort_key_for_table(&edge_table)
                .unwrap_or_else(|| ontology.edge_sort_key())
                .to_vec(),
            primary_key: None,
            options: (),
        };
        let mut tables = vec![edge];
        let mut entity_tables = HashMap::new();
        let local_entities = ontology.local_entity_names();
        let names: Vec<_> = if local_entities.is_empty() {
            ontology.node_names().collect()
        } else {
            local_entities
        };
        for entity in names {
            let Some(node) = ontology.get_node(entity) else {
                continue;
            };
            let fields = ontology
                .local_entity_fields(entity)
                .unwrap_or_else(|| node.fields.iter().collect());
            entity_tables.insert(entity.to_string(), node.destination_table.clone());
            tables.push(Table {
                name: node.destination_table.clone(),
                columns: fields
                    .iter()
                    .map(|field| Column {
                        name: field.name.clone(),
                        storage_type: field.data_type,
                        default: None,
                        options: (),
                    })
                    .collect(),
                column_types: fields
                    .iter()
                    .map(|field| (field.name.clone(), field.data_type))
                    .collect(),
                sort_key: node.sort_key.clone(),
                primary_key: None,
                options: (),
            });
        }
        Self {
            tables,
            metadata: Metadata {
                entities: crate::implementations::entity_facts(ontology, true),
                edge_table,
                entity_tables,
            },
        }
    }

    pub fn edge(&self) -> &Table {
        self.table(&self.metadata.edge_table)
            .expect("derived local edge table")
    }

    pub fn entity_table(&self, entity: &str) -> Option<&Table> {
        self.table(self.metadata.entity_tables.get(entity)?)
    }
}
