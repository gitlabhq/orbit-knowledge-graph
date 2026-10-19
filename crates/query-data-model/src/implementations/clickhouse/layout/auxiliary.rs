use std::collections::BTreeMap;

use super::{
    AuxiliaryTable, Column, ColumnOptions, Dictionary, Engine, MaterializedView, Table,
    TableOptions, derive, system_columns,
};

pub(super) fn table(source: &ontology::AuxiliaryTable) -> AuxiliaryTable {
    let mut columns: Vec<_> = source.columns.iter().map(column).collect();
    let mut column_types: BTreeMap<_, _> = source
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.data_type))
        .collect();
    if source.include_system_columns {
        columns.extend(system_columns(source.version_type.as_deref()));
        column_types.insert(
            ontology::VERSION_COLUMN.into(),
            if source.version_type.as_deref() == Some("uint64") {
                ontology::DataType::Int
            } else {
                ontology::DataType::DateTime
            },
        );
        column_types.insert(ontology::DELETED_COLUMN.into(), ontology::DataType::Bool);
    }
    AuxiliaryTable {
        versioned: source.versioned,
        table: Table {
            name: source.name.clone(),
            columns,
            column_types,
            sort_key: source.order_by.clone(),
            primary_key: None,
            options: TableOptions {
                indexes: vec![],
                projections: source.projections.iter().map(derive::projection).collect(),
                engine: source
                    .engine
                    .as_ref()
                    .map(|name| Engine {
                        name: name.clone(),
                        arguments: vec![],
                    })
                    .unwrap_or_else(|| Engine::replacing(source.version_only_engine)),
                settings: derive::settings(None, !source.projections.is_empty(), &BTreeMap::new()),
                ttl: source.ttl.clone(),
            },
        },
    }
}

pub(super) fn dictionary(source: &ontology::AuxiliaryDictionary) -> Dictionary {
    let mut attributes = vec![Column {
        name: source.key.clone(),
        storage_type: storage_type(
            source.key_type.as_ref().unwrap_or(&ontology::DataType::Int),
            false,
        ),
        default: None,
        options: ColumnOptions::default(),
    }];
    attributes.extend(source.attributes.iter().map(column));
    Dictionary {
        name: source.name.clone(),
        source_table: source.source_table.clone(),
        key: source.key.clone(),
        attributes,
        layout: source.layout.kind.clone(),
        size_in_cells: source.layout.size_in_cells,
        lifetime_min: source.lifetime.min,
        lifetime_max: source.lifetime.max,
    }
}

pub(super) fn view(source: &ontology::MaterializedViewDefinition) -> MaterializedView {
    MaterializedView {
        name: source.name.clone(),
        versioned: source.versioned,
        to_table: source.to_table.clone(),
        select_query: source.select_query.clone(),
        engine: source.engine.as_ref().map(|name| Engine {
            name: name.clone(),
            arguments: source.engine_args.clone(),
        }),
        order_by: source.order_by.clone(),
        populate: source.populate,
    }
}

fn column(source: &ontology::AuxiliaryColumn) -> Column {
    Column {
        name: source.name.clone(),
        storage_type: storage_type(&source.data_type, source.nullable),
        default: source.default.clone(),
        options: ColumnOptions {
            codecs: source.codec.clone().unwrap_or_default(),
        },
    }
}

fn storage_type(data_type: &ontology::DataType, nullable: bool) -> String {
    use ontology::DataType;
    let base = match data_type {
        DataType::String | DataType::Uuid => "String",
        DataType::Int => "Int64",
        DataType::Bool => "Bool",
        DataType::DateTime => "DateTime64(6, 'UTC')",
        DataType::Date => "Date32",
        _ => "String",
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base.into()
    }
}
