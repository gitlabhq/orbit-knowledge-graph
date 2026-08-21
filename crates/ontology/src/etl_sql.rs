use crate::etl::{Extract, ExtractQuery, Pipeline};
use crate::{Ontology, OntologyError};

pub fn validate_authored_etl_sql(ontology: &Ontology) -> Result<(), OntologyError> {
    for pipeline in all_pipelines(ontology) {
        let Extract::ClickHouse(extract) = &pipeline.extract;
        let ExtractQuery::Sql(raw) = &extract.query else {
            continue;
        };
        for (kind, column) in [
            ("version", extract.version.as_str()),
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
        let remaining = raw
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .replace("{{watermark_column}} > {last_watermark:String}", "")
            .replace("{{watermark_column}} <= {watermark:String}", "");
        if remaining.contains("{{watermark_column}}") {
            return Err(OntologyError::Validation(format!(
                "authored SQL for pipeline '{}' uses the watermark column outside a window predicate",
                pipeline.name
            )));
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
    fn hardcoded_version_column_is_rejected() {
        let mut ontology = Ontology::load_embedded().expect("load embedded");
        let pipeline = first_pipeline(&mut ontology);
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.query = ExtractQuery::Sql(format!("SELECT {} AS _version FROM t", extract.version));

        let err = validate_authored_etl_sql(&ontology).expect_err("hardcoded version rejected");
        assert!(
            err.to_string().contains("hardcodes version column"),
            "got: {err}"
        );
    }

    #[test]
    fn watermark_argmax_key_is_rejected() {
        let mut ontology = Ontology::load_embedded().expect("load embedded");
        let pipeline = first_pipeline(&mut ontology);
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.query = ExtractQuery::Sql(
            "SELECT argMax(x, {{watermark_column}}), {{version_column}} AS _version FROM t"
                .to_string(),
        );

        let err = validate_authored_etl_sql(&ontology).expect_err("watermark argMax rejected");
        assert!(
            err.to_string()
                .contains("uses the watermark column outside a window predicate")
        );
    }

    #[test]
    fn multiline_watermark_argmax_key_is_rejected() {
        let mut ontology = Ontology::load_embedded().expect("load embedded");
        let pipeline = first_pipeline(&mut ontology);
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.query = ExtractQuery::Sql(
            "SELECT argMax(x,\n  {{watermark_column}}), {{version_column}} AS _version FROM t"
                .to_string(),
        );

        let err = validate_authored_etl_sql(&ontology).expect_err("watermark argMax rejected");
        assert!(
            err.to_string()
                .contains("uses the watermark column outside a window predicate")
        );
    }

    #[test]
    fn watermark_usage_outside_a_window_is_rejected() {
        let mut ontology = Ontology::load_embedded().expect("load embedded");
        let pipeline = first_pipeline(&mut ontology);
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.query = ExtractQuery::Sql(
            "SELECT {{watermark_column}} AS changed_at, {{version_column}} AS _version FROM t"
                .to_string(),
        );

        let err = validate_authored_etl_sql(&ontology).expect_err("non-window watermark rejected");
        assert!(
            err.to_string()
                .contains("uses the watermark column outside a window predicate")
        );
    }

    #[test]
    fn watermark_window_predicates_are_allowed() {
        let mut ontology = Ontology::load_embedded().expect("load embedded");
        let pipeline = first_pipeline(&mut ontology);
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.query = ExtractQuery::Sql(
            "SELECT {{version_column}} AS _version FROM t WHERE {{watermark_column}} > {last_watermark:String} AND {{watermark_column}} <= {watermark:String}"
                .to_string(),
        );

        validate_authored_etl_sql(&ontology).expect("window watermark allowed");
    }

    #[test]
    fn multiline_watermark_window_predicates_are_allowed() {
        let mut ontology = Ontology::load_embedded().expect("load embedded");
        let pipeline = first_pipeline(&mut ontology);
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.query = ExtractQuery::Sql(
            "SELECT {{version_column}} AS _version FROM t WHERE {{watermark_column}}\n  > {last_watermark:String} AND {{watermark_column}}\n  <= {watermark:String}"
                .to_string(),
        );

        validate_authored_etl_sql(&ontology).expect("window watermark allowed");
    }

    #[test]
    fn valid_window_does_not_hide_other_watermark_usage() {
        let mut ontology = Ontology::load_embedded().expect("load embedded");
        let pipeline = first_pipeline(&mut ontology);
        let Extract::ClickHouse(extract) = &mut pipeline.extract;
        extract.query = ExtractQuery::Sql(
            "SELECT {{watermark_column}} AS changed_at, {{version_column}} AS _version FROM t WHERE {{watermark_column}} > {last_watermark:String} AND {{watermark_column}} <= {watermark:String}"
                .to_string(),
        );

        let err = validate_authored_etl_sql(&ontology).expect_err("non-window watermark rejected");
        assert!(
            err.to_string()
                .contains("uses the watermark column outside a window predicate")
        );
    }
}
