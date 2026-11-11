use std::collections::HashSet;

use indexmap::IndexMap;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::ClickHouse;
use super::spec::{ClickHouseOptions, ClickHouseSource};
use crate::DataModelError;
use crate::catalog::StorageBackend;
use crate::spec::{BackendOptions, Fragment, StorageFragment, decode_options, encode_options};

pub fn versioned(fragments: Vec<Fragment>, prefix: &str) -> Result<Vec<Fragment>, DataModelError> {
    let stores: HashSet<String> = fragments
        .iter()
        .filter_map(|fragment| match fragment {
            Fragment::Storage(storage)
                if storage
                    .engine
                    .as_ref()
                    .is_some_and(|engine| engine.kind == ClickHouse::KIND) =>
            {
                Some(storage.store.clone())
            }
            _ => None,
        })
        .collect();
    fragments
        .into_iter()
        .map(|fragment| match fragment {
            Fragment::Storage(mut storage) if stores.contains(&storage.store) => {
                prefix_store(&mut storage, prefix)?;
                Ok(Fragment::Storage(storage))
            }
            fragment => Ok(fragment),
        })
        .collect()
}

fn prefix_store(storage: &mut StorageFragment, prefix: &str) -> Result<(), DataModelError> {
    let name = |table: &str| format!("{prefix}{table}");
    if let Some(engine) = &mut storage.engine {
        rewrite::<ClickHouseOptions>(&mut engine.backend, |options| {
            options.edge_tables = prefix_keys(std::mem::take(&mut options.edge_tables), prefix);
            options.copies = prefix_keys(std::mem::take(&mut options.copies), prefix);
            for copy in options.copies.values_mut() {
                copy.source = name(&copy.source);
            }
            options.joins = prefix_keys(std::mem::take(&mut options.joins), prefix);
            for source in options
                .joins
                .values_mut()
                .flat_map(|join| &mut join.sources)
            {
                source.table = name(&source.table);
            }
            options.tables = std::mem::take(&mut options.tables)
                .into_iter()
                .map(|(table, spec)| (if spec.versioned { name(&table) } else { table }, spec))
                .collect();
            options.dictionaries = prefix_keys(std::mem::take(&mut options.dictionaries), prefix);
            for dictionary in options.dictionaries.values_mut() {
                dictionary.source = name(&dictionary.source);
            }
        })?;
    }
    for source in storage.entities.values_mut() {
        source.table = source.table.as_deref().map(name);
        rewrite::<ClickHouseSource>(&mut source.backend, |options| {
            for lookup in options.path_lookups.values_mut() {
                lookup.table = name(&lookup.table);
            }
        })?;
    }
    for binding in storage
        .relationships
        .values_mut()
        .flat_map(IndexMap::values_mut)
    {
        binding.edge_table = binding.edge_table.as_deref().map(name);
        if let Some(index) = &mut binding.index {
            index.edge_table = name(&index.edge_table);
        }
    }
    Ok(())
}

fn prefix_keys<V>(map: IndexMap<String, V>, prefix: &str) -> IndexMap<String, V> {
    map.into_iter()
        .map(|(key, value)| (format!("{prefix}{key}"), value))
        .collect()
}

fn rewrite<T: Serialize + DeserializeOwned>(
    options: &mut BackendOptions,
    edit: impl FnOnce(&mut T),
) -> Result<(), DataModelError> {
    if options.is_empty() {
        return Ok(());
    }
    let invalid = |error: serde_json::Error| DataModelError::Invalid(error.to_string());
    let mut typed = decode_options::<T>(options).map_err(invalid)?;
    edit(&mut typed);
    *options = encode_options(typed).map_err(invalid)?;
    Ok(())
}
