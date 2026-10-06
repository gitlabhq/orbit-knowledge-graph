use crate::storage::StoredColumn;
use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};
use ontology::{AuxiliaryColumn, DataType, EdgeTableConfig, NodeEntity, StorageColumn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseColumn {
    pub data_type: String,
    pub default: Option<String>,
    pub codec: Option<Vec<String>>,
    pub query_type: Option<DataType>,
}

pub fn auxiliary(column: &AuxiliaryColumn) -> StoredColumn<ClickHouseColumn> {
    let base = match column.data_type {
        DataType::Int => "Int64",
        DataType::Bool => "Bool",
        DataType::DateTime => "DateTime64(6, 'UTC')",
        DataType::Date => "Date32",
        _ => "String",
    };
    StoredColumn::new(
        &column.name,
        ClickHouseColumn {
            data_type: if column.nullable {
                format!("Nullable({base})")
            } else {
                base.into()
            },
            default: column.default.clone(),
            codec: column.codec.clone(),
            query_type: Some(column.data_type),
        },
    )
}

fn stored(column: &StorageColumn, query_type: Option<DataType>) -> StoredColumn<ClickHouseColumn> {
    StoredColumn::new(
        column.name.trim_matches('`'),
        ClickHouseColumn {
            data_type: column.ch_type.clone(),
            default: column.default.clone(),
            codec: column.codec.clone(),
            query_type,
        },
    )
}

pub fn system_columns(version_type: Option<&str>) -> Vec<StoredColumn<ClickHouseColumn>> {
    let version = if version_type == Some("uint64") {
        ClickHouseColumn {
            data_type: "UInt64".into(),
            default: None,
            codec: None,
            query_type: None,
        }
    } else {
        ClickHouseColumn {
            data_type: "DateTime64(6, 'UTC')".into(),
            default: Some("now64(6)".into()),
            codec: Some(vec!["Delta(8)".into(), "ZSTD(1)".into()]),
            query_type: None,
        }
    };
    vec![
        StoredColumn::new(VERSION_COLUMN, version),
        StoredColumn::new(
            DELETED_COLUMN,
            ClickHouseColumn {
                data_type: "Bool".into(),
                default: Some("false".into()),
                codec: None,
                query_type: None,
            },
        ),
    ]
}

pub fn node_columns(node: &NodeEntity) -> Vec<StoredColumn<ClickHouseColumn>> {
    node.storage
        .columns
        .iter()
        .map(|column| {
            stored(
                column,
                node.fields
                    .iter()
                    .find(|field| {
                        field.name == column.name.trim_matches('`') && field.column_name().is_some()
                    })
                    .map(|field| field.data_type),
            )
        })
        .chain(system_columns(None))
        .collect()
}

pub fn edge_columns(config: &EdgeTableConfig) -> Vec<StoredColumn<ClickHouseColumn>> {
    config
        .storage
        .columns
        .iter()
        .chain(&config.storage.denormalized_columns)
        .map(|column| {
            stored(
                column,
                config
                    .columns
                    .iter()
                    .find(|field| field.name.trim_matches('`') == column.name.trim_matches('`'))
                    .map(|field| field.data_type),
            )
        })
        .chain(system_columns(None))
        .collect()
}

pub fn denormalized_columns<'a>(
    join: &ontology::denormalized::DenormalizedJoin,
    source: impl Fn(usize) -> &'a [StoredColumn<ClickHouseColumn>],
) -> Vec<StoredColumn<ClickHouseColumn>> {
    let anchor = join.anchor_table();
    let mut columns: Vec<_> = source(anchor)
        .iter()
        .filter(|column| column.name() == ontology::TRAVERSAL_PATH_COLUMN)
        .cloned()
        .collect();
    for index in 0..join.tables.len() {
        columns.extend(
            source(index)
                .iter()
                .filter(|column| {
                    ontology::denormalized::copies(column.name())
                        && !(index == anchor && column.name() == ontology::TRAVERSAL_PATH_COLUMN)
                })
                .map(|column| {
                    StoredColumn::new(
                        join.column_for(index, column.name()),
                        column.storage().clone(),
                    )
                }),
        );
    }
    columns.extend(system_columns(None));
    columns
}
