use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: HashSet<String>,
    pub column_types: HashMap<String, ontology::DataType>,
    pub sort_key: Vec<String>,
}

#[derive(Debug)]
pub struct StorageCatalog {
    pub edge: Table,
    pub nodes: HashMap<String, Table>,
}

impl StorageCatalog {
    pub fn derive(ontology: &ontology::Ontology) -> Self {
        let name = ontology
            .local_edge_table_name()
            .unwrap_or_else(|| ontology.edge_table())
            .to_string();
        let edge = Table {
            columns: ontology
                .local_edge_columns()
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            column_types: ontology
                .local_edge_columns()
                .iter()
                .map(|column| (column.name.clone(), column.data_type))
                .collect(),
            sort_key: ontology
                .sort_key_for_table(&name)
                .unwrap_or_else(|| ontology.edge_sort_key())
                .to_vec(),
            name,
        };
        let local_entities = ontology.local_entity_names();
        let names: Vec<_> = if local_entities.is_empty() {
            ontology.node_names().collect()
        } else {
            local_entities
        };
        let nodes = names
            .into_iter()
            .filter_map(|entity| {
                let node = ontology.get_node(entity)?;
                let fields = ontology
                    .local_entity_fields(entity)
                    .unwrap_or_else(|| node.fields.iter().collect());
                Some((
                    entity.to_string(),
                    Table {
                        name: node.destination_table.clone(),
                        columns: fields.iter().map(|field| field.name.clone()).collect(),
                        column_types: fields
                            .iter()
                            .map(|field| (field.name.clone(), field.data_type))
                            .collect(),
                        sort_key: node.sort_key.clone(),
                    },
                ))
            })
            .collect();
        Self { edge, nodes }
    }
}
