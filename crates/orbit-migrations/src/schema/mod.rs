mod sql;
pub(crate) mod translate;
pub mod types;

use ontology::Ontology;

pub use sql::{attach_partitions_sql, clone_table_sql, drop_entity_sql};
pub use translate::render_refreshable_view_select;
pub use types::*;

impl GraphSchema {
    pub fn from_ontology(ontology: &Ontology) -> Self {
        let tables = translate::build_all_tables(ontology);
        let all_table_names = translate::collect_all_table_names(ontology);

        Self {
            views: translate::build_views(ontology, &tables),
            dictionaries: translate::build_dictionaries(ontology),
            refreshable_views: translate::build_refreshable_views(ontology),
            unversioned_definitions: translate::build_unversioned_definitions(
                ontology,
                &all_table_names,
            ),
            tables,
        }
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.tables
            .iter()
            .map(|table| table.name.as_str())
            .collect()
    }
}
