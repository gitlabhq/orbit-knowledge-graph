//! Build-time guard for authored ETL SQL. Watermark/deleted columns are declared
//! once per pipeline and can be overridden per entity, so authored SQL must reach
//! them through `{{watermark_column}}`/`{{deleted_column}}` markers; a hardcoded
//! literal silently drifts the day an override changes the resolved column.

use crate::etl::{Extract, ExtractQuery, Pipeline};
use crate::{Ontology, OntologyError};

pub fn validate_authored_etl_sql(ontology: &Ontology) -> Result<(), OntologyError> {
    for pipeline in all_pipelines(ontology) {
        let Extract::ClickHouse(extract) = &pipeline.extract;
        let ExtractQuery::Sql(raw) = &extract.query else {
            continue;
        };
        for (kind, column) in [
            ("watermark", extract.watermark.as_str()),
            ("deleted", extract.deleted.as_str()),
        ] {
            if raw.contains(column) {
                return Err(OntologyError::Validation(format!(
                    "authored SQL for pipeline '{}' hardcodes {kind} column '{column}'; use {{{{{kind}_column}}}} instead",
                    pipeline.name
                )));
            }
        }
    }
    Ok(())
}

fn all_pipelines(ontology: &Ontology) -> impl Iterator<Item = &Pipeline> {
    ontology
        .nodes()
        .flat_map(|node| node.pipelines.iter())
        .chain(
            ontology
                .derived_entities()
                .flat_map(|derived| derived.pipelines.iter()),
        )
        .chain(ontology.edge_etl_configs().map(|(_, pipeline)| pipeline))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn first_pipeline(ontology: &mut Ontology) -> &mut Pipeline {
        ontology
            .nodes
            .values_mut()
            .flat_map(|node| node.pipelines.iter_mut())
            .next()
            .expect("embedded ontology has a node pipeline")
    }

    #[test]
    fn embedded_ontology_authored_sql_uses_markers() {
        validate_authored_etl_sql(&Ontology::load_embedded().expect("load embedded"))
            .expect("committed authored SQL must use markers");
    }

    #[test]
    fn hardcoded_watermark_column_is_rejected() {
        let mut ontology = Ontology::load_embedded().expect("load embedded");
        let pipeline = first_pipeline(&mut ontology);
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.query =
            ExtractQuery::Sql(format!("SELECT {} AS _version FROM t", extract.watermark));

        let err = validate_authored_etl_sql(&ontology).expect_err("hardcoded watermark rejected");
        assert!(
            err.to_string().contains("hardcodes watermark column"),
            "got: {err}"
        );
    }
}
