// TODO: Move schema template rendering into a dedicated schema compiler crate with DDL generation.

use minijinja::{Environment, Error, UndefinedBehavior, context};
use ontology::Ontology;
use serde::Serialize;

#[derive(Serialize)]
struct GraphTableTemplateContext {
    logical_name: String,
    physical_name: String,
    global: bool,
    has_traversal_path: bool,
}

pub(crate) fn render_refreshable_materialized_view_select(
    template: &str,
    ontology: &Ontology,
    schema_version: u32,
    table_prefix: &str,
) -> Result<String, Error> {
    let mut environment = Environment::new();
    environment.set_undefined_behavior(UndefinedBehavior::Strict);
    environment.render_str(
        template,
        context! {
            schema => context! { version => schema_version },
            graph => context! { tables => get_graph_table_template_contexts(ontology, table_prefix) },
        },
    )
}

fn get_graph_table_template_contexts(
    ontology: &Ontology,
    table_prefix: &str,
) -> Vec<GraphTableTemplateContext> {
    let mut tables = ontology
        .nodes()
        .map(|node| GraphTableTemplateContext {
            logical_name: node.destination_table.clone(),
            physical_name: format!("{table_prefix}{}", node.destination_table),
            global: node.global,
            has_traversal_path: node.has_traversal_path,
        })
        .collect::<Vec<_>>();

    tables.extend(ontology.edge_tables().into_iter().map(|table| {
        GraphTableTemplateContext {
            logical_name: table.to_string(),
            physical_name: format!("{table_prefix}{table}"),
            global: false,
            has_traversal_path: ontology
                .edge_table_config(table)
                .is_some_and(ontology::EdgeTableConfig::has_traversal_path),
        }
    }));

    tables.sort_by(|left, right| left.logical_name.cmp(&right.logical_name));
    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_schema_version_and_physical_table_names() {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let rendered = render_refreshable_materialized_view_select(
            "{% for table in graph.tables if table.has_traversal_path %}{{ table.logical_name }}={{ table.physical_name }};{% endfor %}version={{ schema.version }}",
            &ontology,
            7,
            "v7_",
        )
        .expect("template must render");

        assert!(rendered.contains("gl_project=v7_gl_project"));
        assert!(rendered.contains("gl_edge=v7_gl_edge"));
        assert!(rendered.contains("version=7"));
    }
}
