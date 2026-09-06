pub mod campaign;
pub mod completion;
pub mod execute;
pub mod fingerprint;
pub mod garbage_collection;
pub mod ledger;
pub mod nats;
pub mod schema;
pub mod scope;
pub mod version;

#[cfg(test)]
pub(crate) mod test_helpers {
    use std::collections::BTreeSet;

    pub fn entity_set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }
}
