use super::{RowSemantics, StorageType, StoredColumn, TableLayout};
use ontology::{DataType, EdgeColumn, NodeEntity};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalType {
    Int64,
    UInt64,
    Bool,
    String,
    Date,
    Timestamp,
    Nullable(Box<Self>),
    Array(Box<Self>),
}

impl LocalType {
    fn from_storage(value: &str) -> Self {
        let value = value.trim();
        for wrapper in ["Nullable", "LowCardinality", "Array"] {
            if let Some(inner) = value
                .strip_prefix(wrapper)
                .and_then(|value| value.strip_prefix('('))
                .and_then(|value| value.strip_suffix(')'))
            {
                let inner = Self::from_storage(inner);
                return match wrapper {
                    "Nullable" => Self::Nullable(Box::new(inner)),
                    "Array" => Self::Array(Box::new(inner)),
                    _ => inner,
                };
            }
        }
        match value {
            "Int64" => Self::Int64,
            "UInt64" => Self::UInt64,
            "Bool" => Self::Bool,
            "Date32" => Self::Date,
            value if value.starts_with("DateTime64(") => Self::Timestamp,
            _ => Self::String,
        }
    }
}

impl TableLayout {
    pub fn local_node(
        node: &NodeEntity,
        excluded: &[String],
    ) -> Result<Self, crate::DataModelError> {
        let columns = node
            .storage
            .columns
            .iter()
            .filter(|column| !excluded.contains(&column.name))
            .map(|column| StoredColumn {
                name: column.name.clone(),
                data_type: StorageType::DuckDb(LocalType::from_storage(&column.ch_type)),
                codec: None,
                default: column
                    .default
                    .as_ref()
                    .filter(|value| literal_default(value))
                    .cloned(),
                query_type: node
                    .fields
                    .iter()
                    .find(|field| field.name == column.name && field.column_name().is_some())
                    .map(|field| field.data_type),
            })
            .collect();
        let sort_key = node
            .sort_key
            .iter()
            .filter(|key| !excluded.contains(key))
            .cloned()
            .collect::<Vec<_>>();
        Self::new(
            &node.destination_table,
            columns,
            &sort_key,
            RowSemantics::Current,
        )
    }

    pub fn local_edge(name: &str, columns: &[EdgeColumn]) -> Result<Self, crate::DataModelError> {
        let sort_key = columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let columns = columns
            .iter()
            .map(|column| StoredColumn {
                name: column.name.clone(),
                data_type: StorageType::DuckDb(match column.data_type {
                    DataType::Int => LocalType::Int64,
                    DataType::Bool => LocalType::Bool,
                    DataType::DateTime => LocalType::Timestamp,
                    DataType::Date => LocalType::Date,
                    _ => LocalType::String,
                }),
                codec: None,
                default: None,
                query_type: Some(column.data_type),
            })
            .collect();
        Self::new(name, columns, &sort_key, RowSemantics::Current)
    }
}

pub fn local_tables(
    ontology: &ontology::Ontology,
) -> Result<Vec<TableLayout>, crate::DataModelError> {
    ontology
        .local_entity_names()
        .into_iter()
        .map(|name| {
            TableLayout::local_node(
                ontology.get_node(name).expect("declared local entity"),
                ontology
                    .local_entity_excludes(name)
                    .expect("local exclusions"),
            )
        })
        .chain(
            ontology
                .local_edge_table_name()
                .map(|name| TableLayout::local_edge(name, ontology.local_edge_columns())),
        )
        .collect()
}

fn literal_default(value: &str) -> bool {
    let value = value.trim();
    value.eq_ignore_ascii_case("false")
        || value.eq_ignore_ascii_case("true")
        || (value.starts_with('\'') && value.ends_with('\''))
        || value.parse::<f64>().is_ok()
}
