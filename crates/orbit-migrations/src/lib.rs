pub mod campaign;
pub mod catalog;
pub mod completion;
pub mod execute;
pub mod fingerprint;
pub mod garbage_collection;
pub mod ledger;
pub mod nats;
pub mod schema;
pub mod scope;
pub mod snapshot_check;
pub mod version;

#[cfg(test)]
pub(crate) mod test_helpers {
    use std::collections::BTreeSet;

    use ontology::migrations::EdgeRows;

    use crate::scope::MigrationScope;

    pub fn entity_set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    pub fn sdlc_scope(names: &[&str]) -> MigrationScope {
        MigrationScope::Sdlc {
            entities: entity_set(names),
            edge_rows: EdgeRows::Changed,
        }
    }
}
