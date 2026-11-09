use query_data_model::catalog::sources::{EtlScope, Pipeline};

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
        pipeline: &Pipeline,
    ) -> Vec<PointLookupJoin> {
        pipeline
            .lookups
            .iter()
            .enumerate()
            .map(|(index, lookup)| PointLookupJoin {
                internal_alias: format!("{LOOKUP_ALIAS_PREFIX}{index}"),
                source_table: lookup.table.clone(),
                source_id_column: LOOKUP_SOURCE_ID_COLUMN.to_string(),
                batch_id_column: lookup.id_column.clone(),
                output_fields: lookup
                    .fields
                    .iter()
                    .map(|(source_column, output_field)| PointLookupOutputField {
                        source_column: source_column.clone(),
                        output_field: output_field.clone(),
                    })
                    .collect(),
                has_traversal_path: pipeline.scope == EtlScope::Namespaced && lookup.namespaced,
            })
            .collect()
    }
}
