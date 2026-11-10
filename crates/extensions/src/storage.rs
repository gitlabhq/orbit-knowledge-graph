//! Creating the graph tables of extension entities.
//!
//! The tables come from the same ontology-to-DDL path as the core graph tables
//! ([`orbit_migrations::schema::GraphSchema::from_ontology`]), so an extension
//! table has the same shape a core node table would: a `ReplacingMergeTree` over
//! the declared storage columns plus the internal version and deleted columns.
//!
//! They are created outside the core schema version and the migration ledger.
//! The core migration machinery fingerprints only the bundled ontology and the
//! tables it derives, so these never enter the fingerprint or the ledger. Their
//! name carries the package digest, so a new package version is a new table and
//! the extensions engine reindexes into it.

use ontology::Ontology;
use orbit_migrations::schema::GraphSchema;

/// The prefix that marks an extension graph table. It combines the ontology's
/// required `gl_` table prefix with `ext_`.
pub const EXTENSION_TABLE_PREFIX: &str = "gl_ext_";

#[derive(Debug, thiserror::Error)]
#[error("extension storage: {0}")]
pub struct StorageError(String);

/// The `CREATE TABLE` statement for every extension table in the merged model,
/// paired with its table name. `version_prefix` is empty for the unversioned
/// extension tables; pass a `v<N>_` prefix only to match a versioned read path.
pub fn extension_create_statements(
    ontology: &Ontology,
    version_prefix: &str,
) -> Result<Vec<(String, String)>, StorageError> {
    let schema = GraphSchema::from_ontology(ontology).map_err(|e| StorageError(e.to_string()))?;
    Ok(schema
        .tables
        .iter()
        .filter(|table| table.name.starts_with(EXTENSION_TABLE_PREFIX))
        .map(|table| {
            (
                format!("{version_prefix}{}", table.name),
                table.to_create_sql(version_prefix),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loader::load_merged_ontology;
    use crate::package::{Package, short_digest};

    const WIDGETS: &str = include_str!("../tests/fixtures/example.widgets.yaml");

    fn merged() -> Ontology {
        let package = Package::parse(WIDGETS, short_digest(WIDGETS)).expect("fixture parses");
        load_merged_ontology(orbit_versions::VERSIONS.schema, &[package]).expect("model loads")
    }

    #[test]
    fn generates_a_replacing_merge_tree_per_entity() {
        let statements = extension_create_statements(&merged(), "").expect("ddl generated");
        let names: Vec<&str> = statements.iter().map(|(name, _)| name.as_str()).collect();

        assert_eq!(names.len(), 2, "one table per extension entity: {names:?}");
        for (name, ddl) in &statements {
            assert!(name.starts_with(EXTENSION_TABLE_PREFIX), "{name}");
            assert!(ddl.contains("CREATE TABLE IF NOT EXISTS"), "{ddl}");
            assert!(ddl.contains("ReplacingMergeTree"), "{ddl}");
            assert!(ddl.contains(name.as_str()), "ddl names the table");
        }
    }

    #[test]
    fn table_names_carry_the_digest() {
        let digest8: String = short_digest(WIDGETS).chars().take(8).collect();
        let statements = extension_create_statements(&merged(), "").expect("ddl generated");
        assert!(
            statements.iter().all(|(name, _)| name.ends_with(&digest8)),
            "every table ends with the digest {digest8}: {:?}",
            statements.iter().map(|(n, _)| n).collect::<Vec<_>>()
        );
    }

    #[test]
    fn extension_tables_stay_out_of_the_core_fingerprint() {
        let core = Ontology::load_embedded().expect("core ontology loads");
        let fingerprints =
            orbit_migrations::fingerprint::current_fingerprints(&core).expect("fingerprints");
        assert!(
            fingerprints
                .ddl
                .keys()
                .all(|table| !table.starts_with(EXTENSION_TABLE_PREFIX)),
            "no extension table appears in the core migration fingerprint"
        );
    }
}
