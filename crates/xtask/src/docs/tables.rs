use anyhow::{Result, bail};
use ontology::Ontology;

const MIN_TEXT_INDEXED_ENTITIES: usize = 10;
const MIN_NODES: usize = 20;
const MIN_EDGES: usize = 20;

pub fn text_indexed_properties(ontology: &Ontology) -> Result<String> {
    let rows: Vec<(&str, Vec<&str>)> = ontology
        .nodes()
        .map(|node| {
            (
                node.name.as_str(),
                ontology.text_indexed_columns(&node.name),
            )
        })
        .filter(|(_, columns)| !columns.is_empty())
        .collect();
    require_floor(
        "entities carry a text index",
        rows.len(),
        MIN_TEXT_INDEXED_ENTITIES,
    )?;

    let mut out = table_header(&["Entity", "Text-indexed properties"]);
    for (entity, columns) in rows {
        out.push_str(&format!("| `{entity}` | {} |\n", code_list(columns)));
    }
    Ok(out)
}

pub fn schema_nodes(ontology: &Ontology) -> Result<String> {
    let mut sections = Vec::new();
    let mut node_count = 0;
    for domain in ontology.domains() {
        let mut out = format!("### `{}`\n\n", domain.name);
        if !domain.description.is_empty() {
            out.push_str(&format!("{}\n\n", table_cell(&domain.description)));
        }
        out.push_str(&table_header(&["Node type", "Description", "Properties"]));
        for node in domain
            .node_names
            .iter()
            .filter_map(|n| ontology.get_node(n))
        {
            let properties = node
                .fields
                .iter()
                .filter(|f| !f.admin_only)
                .map(|f| f.name.as_str());
            out.push_str(&format!(
                "| `{}` | {} | {} |\n",
                node.name,
                table_cell(&node.description),
                code_list(properties),
            ));
            node_count += 1;
        }
        sections.push(out);
    }
    require_floor("node types", node_count, MIN_NODES)?;
    Ok(sections.join("\n"))
}

pub fn schema_edges(ontology: &Ontology) -> Result<String> {
    let edges: Vec<&str> = ontology.edge_names().collect();
    require_floor("relationships", edges.len(), MIN_EDGES)?;

    let mut out = table_header(&["Relationship", "Description", "From", "To"]);
    for edge in edges {
        out.push_str(&format!(
            "| `{edge}` | {} | {} | {} |\n",
            table_cell(ontology.get_edge_description(edge).unwrap_or_default()),
            code_list(ontology.get_edge_source_types(edge)),
            code_list(ontology.get_edge_all_target_types(edge)),
        ));
    }
    Ok(out)
}

fn require_floor(what: &str, count: usize, floor: usize) -> Result<()> {
    if count < floor {
        bail!("only {count} {what} (minimum {floor}); did the ontology fail to load?");
    }
    Ok(())
}

fn table_header(columns: &[&str]) -> String {
    let separators: Vec<String> = columns.iter().map(|c| "-".repeat(c.len() + 2)).collect();
    format!("| {} |\n|{}|\n", columns.join(" | "), separators.join("|"))
}

fn table_cell(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace('|', "\\|")
}

fn code_list<S: AsRef<str>>(names: impl IntoIterator<Item = S>) -> String {
    names
        .into_iter()
        .map(|name| format!("`{}`", name.as_ref()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backticked_first_column(table: &str) -> Vec<&str> {
        table
            .lines()
            .filter_map(|l| l.strip_prefix("| `"))
            .filter_map(|l| l.split('`').next())
            .collect()
    }

    #[test]
    fn renderers_are_deterministic_and_sorted() {
        let ontology = Ontology::load_embedded().unwrap();
        for render in [text_indexed_properties, schema_edges] {
            let first = render(&ontology).unwrap();
            assert_eq!(first, render(&ontology).unwrap());
            let names = backticked_first_column(&first);
            assert!(names.windows(2).all(|w| w[0] < w[1]), "rows sorted");
        }
        assert_eq!(
            schema_nodes(&ontology).unwrap(),
            schema_nodes(&ontology).unwrap()
        );
    }

    #[test]
    fn text_indexed_properties_lists_merge_request() {
        let table = text_indexed_properties(&Ontology::load_embedded().unwrap()).unwrap();
        assert!(table.starts_with("| Entity | Text-indexed properties |\n|--------|"));
        assert!(table.contains("| `MergeRequest` |"));
    }

    #[test]
    fn schema_nodes_groups_by_sorted_domain_and_hides_admin_only_fields() {
        let ontology = Ontology::load_embedded().unwrap();
        let rendered = schema_nodes(&ontology).unwrap();
        let headings: Vec<&str> = rendered
            .lines()
            .filter_map(|l| l.strip_prefix("### "))
            .collect();
        assert_eq!(headings.len(), ontology.domains().count());
        assert!(headings.windows(2).all(|w| w[0] < w[1]), "domains sorted");
        assert_eq!(
            backticked_first_column(&rendered).len(),
            ontology.node_count()
        );
        let row = |name: &str| {
            rendered
                .lines()
                .find(|l| l.starts_with(&format!("| `{name}` |")))
                .unwrap()
                .to_string()
        };
        assert!(row("User").contains("`username`"));
        assert!(!row("User").contains("`email`"), "admin_only field listed");
        assert!(row("Project").contains("`traversal_path`"));
    }

    #[test]
    fn schema_edges_lists_every_edge_with_endpoints() {
        let ontology = Ontology::load_embedded().unwrap();
        let table = schema_edges(&ontology).unwrap();
        assert_eq!(
            backticked_first_column(&table).len(),
            ontology.edge_names().count()
        );
        assert!(table.starts_with("| Relationship | Description | From | To |\n"));
        assert!(table.contains("| `AUTHORED` | Authorship relationship between users and entities | `User` | `MergeRequest`, `Note`, `Vulnerability`, `WorkItem` |"));
    }

    #[test]
    fn renderers_bail_below_floor() {
        let empty = Ontology::new();
        assert!(text_indexed_properties(&empty).is_err());
        assert!(schema_nodes(&empty).is_err());
        assert!(schema_edges(&empty).is_err());
    }

    #[test]
    fn table_cell_flattens_whitespace_and_escapes_pipes() {
        assert_eq!(table_cell("a  b\nc | d"), "a b c \\| d");
    }
}
