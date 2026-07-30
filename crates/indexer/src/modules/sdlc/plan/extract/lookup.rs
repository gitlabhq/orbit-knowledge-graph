use ontology::{ClickHouseExtract, EtlScope};

const LOOKUP_ALIAS_PREFIX: &str = "_e";
const LOOKUP_SOURCE_ID_COLUMN: &str = "id";

#[derive(Clone)]
pub(in crate::modules::sdlc) struct PointLookupOutputField {
    pub source_column: String,
    pub output_field: String,
}

#[derive(Clone)]
pub(in crate::modules::sdlc) struct PointLookupJoin {
    pub internal_alias: String,
    pub source_table: String,
    pub source_id_column: String,
    pub batch_id_column: String,
    pub output_fields: Vec<PointLookupOutputField>,
    pub has_traversal_path: bool,
}

impl PointLookupJoin {
    pub(in crate::modules::sdlc) fn get_from_extract_declaration(
        extract: &ClickHouseExtract,
        pipeline_scope: EtlScope,
    ) -> Vec<PointLookupJoin> {
        extract
            .lookups
            .iter()
            .enumerate()
            .filter_map(|(index, lookup)| {
                let source = lookup.resolved_source.as_ref()?;
                Some(PointLookupJoin {
                    internal_alias: format!("{LOOKUP_ALIAS_PREFIX}{index}"),
                    source_table: source.table.clone(),
                    source_id_column: LOOKUP_SOURCE_ID_COLUMN.to_string(),
                    batch_id_column: lookup.batch_id_column.clone(),
                    output_fields: lookup
                        .output_fields
                        .iter()
                        .map(|(source_column, output_field)| PointLookupOutputField {
                            source_column: source_column.clone(),
                            output_field: output_field.clone(),
                        })
                        .collect(),
                    has_traversal_path: pipeline_scope == EtlScope::Namespaced && source.namespaced,
                })
            })
            .collect()
    }
}
