use std::collections::HashSet;

use indexmap::IndexMap;

use super::{Column, Engine, Index, Projection, Table, data_type, nullable};
use crate::catalog::Diagnostics;
use crate::implementations::clickhouse::backend::IndexKind;
use crate::implementations::clickhouse::spec::{
    ColumnSpec, IndexSpec, IndexSpecs, ProjectionSpec, Settings, TableSpec,
};

const TEXT_INDEX: &str = "text";

pub(super) fn auxiliary_table(
    package: &str,
    context: &str,
    name: &str,
    spec: &TableSpec,
    diagnostics: &mut Diagnostics,
) -> Option<Table> {
    let engine = parse_engine(package, context, &spec.engine, diagnostics)?;
    let columns = declared_columns(&spec.columns);
    Some(Table {
        indexes: table_indexes(package, context, &spec.indexes, &columns, diagnostics),
        projections: projections(&spec.projections),
        engine: Some(engine),
        sort_key: spec.sort_key.clone(),
        primary_key: spec.primary_key.clone(),
        settings: merge(&[], &spec.settings),
        ttl: spec.ttl.clone(),
        ..Table::new(name, columns)
    })
}

pub(super) fn declared_columns(columns: &IndexMap<String, ColumnSpec>) -> Vec<Column> {
    columns
        .iter()
        .map(|(name, spec)| Column {
            data_type: data_type(&spec.storage_type),
            nullable: nullable(&spec.storage_type),
            storage_type: spec.storage_type.clone(),
            default: spec.default.clone(),
            codecs: spec.codec.clone(),
            ..Column::new(name, None)
        })
        .collect()
}

pub(super) fn table_indexes(
    package: &str,
    context: &str,
    indexes: &IndexMap<String, IndexSpecs>,
    columns: &[Column],
    diagnostics: &mut Diagnostics,
) -> Vec<Index> {
    let has_column = |target: &str| columns.iter().any(|column| column.name == target);
    check_indexes(package, context, indexes, has_column, diagnostics);
    build_indexes(indexes, |target| {
        has_column(target).then(|| target.to_string())
    })
}

pub(super) fn parse_engine(
    package: &str,
    context: &str,
    text: &str,
    diagnostics: &mut Diagnostics,
) -> Option<Engine> {
    diagnostics.require(Engine::parse(text), package, || {
        format!("{context} has malformed engine `{text}`")
    })
}

pub(super) fn merge(base: &[(String, String)], overrides: &Settings) -> Vec<(String, String)> {
    let mut settings = base.to_vec();
    for (name, value) in overrides {
        match settings.iter_mut().find(|(key, _)| key == name) {
            Some((_, previous)) => previous.clone_from(value),
            None => settings.push((name.clone(), value.clone())),
        }
    }
    settings
}

pub fn default_index_name(target: &str, position: usize, kind: &str) -> String {
    let column = target.trim_start_matches('_');
    match position {
        0 => format!("idx_{column}"),
        _ => format!(
            "idx_{column}_{}",
            kind.split('(').next().unwrap_or(kind).trim()
        ),
    }
}

fn named<'a>(
    target: &'a str,
    specs: &'a IndexSpecs,
) -> impl Iterator<Item = (String, &'a IndexSpec)> + 'a {
    specs
        .specs()
        .iter()
        .enumerate()
        .map(move |(position, spec)| {
            let name = spec.name().map_or_else(
                || default_index_name(target, position, spec.kind()),
                str::to_string,
            );
            (name, spec)
        })
}

pub(in crate::implementations::clickhouse) fn check_indexes(
    package: &str,
    context: &str,
    indexes: &IndexMap<String, IndexSpecs>,
    has_column: impl Fn(&str) -> bool,
    diagnostics: &mut Diagnostics,
) {
    let mut names = HashSet::new();
    for (target, specs) in indexes {
        if !has_column(target) {
            diagnostics.error(
                package,
                format!("{context} indexes unknown column {target}"),
            );
        }
        for (name, spec) in named(target, specs) {
            if IndexKind::parse(spec.kind()).is_none() {
                diagnostics.error(
                    package,
                    format!(
                        "{context} {target} has unknown index kind `{}`",
                        spec.kind()
                    ),
                );
            }
            if !names.insert(name.clone()) {
                diagnostics.error(package, format!("{context} declares index {name} twice"));
            }
        }
    }
}

pub(super) fn build_indexes(
    indexes: &IndexMap<String, IndexSpecs>,
    column_of: impl Fn(&str) -> Option<String>,
) -> Vec<Index> {
    let mut built = Vec::new();
    for (target, specs) in indexes {
        let Some(column) = column_of(target) else {
            continue;
        };
        for (name, spec) in named(target, specs) {
            let index = Index {
                name,
                column: column.clone(),
                lowercase: false,
                kind: spec.kind().to_string(),
                granularity: spec.granularity(),
            };
            if index.kind == TEXT_INDEX {
                built.extend(text_indexes(index));
            } else {
                built.push(index);
            }
        }
    }
    built
}

fn text_indexes(index: Index) -> [Index; 2] {
    [
        Index {
            lowercase: true,
            kind: "text(tokenizer = splitByNonAlpha)".into(),
            ..index.clone()
        },
        Index {
            name: format!("{}_ngram", index.name),
            lowercase: true,
            kind: "ngrambf_v1(3, 512, 2, 0)".into(),
            ..index
        },
    ]
}

pub(super) fn projections(specs: &IndexMap<String, ProjectionSpec>) -> Vec<Projection> {
    specs
        .iter()
        .map(|(name, spec)| match spec {
            ProjectionSpec::Ordered(ordered) if ordered.lightweight => Projection::Lightweight {
                name: name.clone(),
                order_by: ordered.order_by.clone(),
            },
            ProjectionSpec::Ordered(ordered) => Projection::Reorder {
                name: name.clone(),
                order_by: ordered.order_by.clone(),
            },
            ProjectionSpec::Aggregate(aggregate) => Projection::Aggregate {
                name: name.clone(),
                select: aggregate.select.clone(),
                group_by: aggregate.group_by.clone(),
            },
        })
        .collect()
}
