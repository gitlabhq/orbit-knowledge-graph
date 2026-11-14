//! Every generic field a storage fragment sets is one its store's backend reads, and every
//! entity of a store that a source writes names its table.

use super::backend::{Field, Registered};
use super::{Diagnostics, registered};
use crate::spec::storage::ColumnStorage;
use crate::spec::{EntityStorage, StorageFragment};

pub(super) fn check(fragment: &StorageFragment, kind: &str, diagnostics: &mut Diagnostics) {
    let Some(backend) = registered(kind) else {
        return;
    };
    let mut set: Vec<(Field, String)> = Vec::new();
    if let Some(engine) = &fragment.engine {
        let on = format!("store {}", fragment.store);
        let fields = [
            (engine.trusted, Field::Trusted),
            (engine.path_column.is_some(), Field::PathColumn),
        ];
        set.extend(written(fields).map(|field| (field, on.clone())));
    }
    for (name, entity) in &fragment.entities {
        set.extend(entity_fields(entity).map(|field| (field, name.clone())));
        for (property, column) in &entity.columns {
            let on = format!("{name}.{property}");
            set.extend(column_fields(column).map(|field| (field, on.clone())));
        }
    }
    for (relationship, variants) in &fragment.relationships {
        for (variant, binding) in variants {
            let on = format!("{relationship}({variant})");
            let fields = [
                (binding.fk.is_some(), Field::Fk),
                (binding.edge_table.is_some(), Field::EdgeTable),
                (binding.index.is_some(), Field::Index),
                (!binding.copies.is_empty(), Field::Copies),
            ];
            set.extend(written(fields).map(|field| (field, on.clone())));
        }
    }
    for (field, on) in set.iter().filter(|(field, _)| !backend.reads(*field)) {
        diagnostics.error(
            &fragment.package,
            format!(
                "store {} sets {} on {on}, which {kind} stores do not read",
                fragment.store,
                field.name()
            ),
        );
    }
    if backend.capabilities().writer {
        tables(fragment, backend, diagnostics);
    }
}

fn entity_fields(entity: &EntityStorage) -> impl Iterator<Item = Field> {
    let access = &entity.access;
    let fields = [
        (entity.complete, Field::Complete),
        (entity.accepts.is_some(), Field::Accepts),
        (entity.table.is_some(), Field::Table),
        (entity.path_column.is_some(), Field::PathColumn),
        (!entity.exclude.is_empty(), Field::Exclude),
        (
            !(access.filter.is_empty() && access.text.is_empty() && access.order.is_empty()),
            Field::Access,
        ),
    ];
    written(fields)
}

fn column_fields(column: &ColumnStorage) -> impl Iterator<Item = Field> {
    let fields = [
        (column.immutable, Field::Immutable),
        (column.column.is_some(), Field::Column),
        (column.selectivity.is_some(), Field::Selectivity),
        (column.stored_as_text, Field::StoredAsText),
    ];
    written(fields)
}

fn written<const N: usize>(fields: [(bool, Field); N]) -> impl Iterator<Item = Field> {
    fields
        .into_iter()
        .filter_map(|(set, field)| set.then_some(field))
}

/// A source writes each entity of a writer store into the table it names.
fn tables(fragment: &StorageFragment, backend: &dyn Registered, diagnostics: &mut Diagnostics) {
    for (name, entity) in &fragment.entities {
        if entity.table.is_none() {
            diagnostics.error(
                &fragment.package,
                format!(
                    "store {} holds {name} in a {} store, which a source writes, but names no table",
                    fragment.store,
                    backend.kind()
                ),
            );
        }
    }
}
