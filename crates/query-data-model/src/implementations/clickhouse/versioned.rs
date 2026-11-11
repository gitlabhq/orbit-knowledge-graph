use std::collections::HashSet;

use indexmap::IndexMap;

use super::ClickHouse;
use super::spec::{ClickHouseOptions, ClickHouseSource};
use crate::DataModelError;
use crate::catalog::StorageBackend;
use crate::spec::{Block, Fragment, Fragments, StorageFragment};

/// The fragments with every table of a ClickHouse store named under `prefix`.
pub fn versioned(mut fragments: Fragments, prefix: &str) -> Result<Fragments, DataModelError> {
    let stores: HashSet<String> = fragments
        .iter()
        .filter_map(|fragment| match fragment {
            Fragment::Storage(storage) => storage
                .engine
                .as_ref()
                .filter(|engine| engine.kind == ClickHouse::KIND)
                .map(|_| storage.store.clone()),
            _ => None,
        })
        .collect();
    for fragment in fragments.iter_mut() {
        if let Fragment::Storage(storage) = fragment
            && stores.contains(&storage.store)
        {
            prefix_store(storage, prefix)?;
        }
    }
    fragments.tag(prefix);
    Ok(fragments)
}

fn prefix_store(storage: &mut StorageFragment, prefix: &str) -> Result<(), DataModelError> {
    let name = |table: &str| format!("{prefix}{table}");
    if let Some(engine) = &mut storage.engine {
        let options: &mut ClickHouseOptions = typed(&mut engine.backend, &storage.store)?;
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
    }
    for source in storage.entities.values_mut() {
        source.table = source.table.as_deref().map(name);
        let options: &mut ClickHouseSource = typed(&mut source.backend, &storage.store)?;
        for lookup in options.path_lookups.values_mut() {
            lookup.table = name(&lookup.table);
        }
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

fn typed<'a, T: 'static>(
    block: &'a mut Box<dyn Block>,
    store: &str,
) -> Result<&'a mut T, DataModelError> {
    block.downcast_mut().ok_or_else(|| {
        DataModelError::Invalid(format!("store {store} holds blocks of another backend"))
    })
}

fn prefix_keys<V>(map: IndexMap<String, V>, prefix: &str) -> IndexMap<String, V> {
    map.into_iter()
        .map(|(key, value)| (format!("{prefix}{key}"), value))
        .collect()
}
