use super::Schema;

pub fn render_refreshable_view_select(
    template: &str,
    schema: &Schema,
    version: u32,
    version_prefix: &str,
) -> Result<String, query_data_model::sql_template::Error> {
    query_data_model::sql_template::render(
        template,
        query_data_model::sql_template::context! {
            schema => query_data_model::sql_template::context! { version => version },
            graph => query_data_model::sql_template::context! {
                tables => table_contexts(schema, version_prefix)
            },
        },
    )
}

#[derive(serde::Serialize)]
struct TableContext {
    logical_name: String,
    physical_name: String,
    global: bool,
    has_traversal_path: bool,
}

fn table_contexts(schema: &Schema, version_prefix: &str) -> Vec<TableContext> {
    let mut tables: Vec<TableContext> = schema
        .graph_tables()
        .iter()
        .map(|table| TableContext {
            logical_name: table.name.clone(),
            physical_name: format!("{version_prefix}{}", table.name),
            global: table.global,
            has_traversal_path: table.has_path_column,
        })
        .collect();
    tables.sort_by(|left, right| left.logical_name.cmp(&right.logical_name));
    tables
}
