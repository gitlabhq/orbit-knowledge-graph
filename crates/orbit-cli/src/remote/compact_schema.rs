use serde_json::Value;

pub(crate) const DOMAINS: &[&str] = &["core", "code_review", "plan"];

pub(crate) fn node_names(schema: &Value) -> Vec<String> {
    schema["domains"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|domain| {
            domain["name"]
                .as_str()
                .is_some_and(|name| DOMAINS.contains(&name))
        })
        .flat_map(|domain| domain["node_names"].as_array().into_iter().flatten())
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

pub(crate) fn render(schema: &Value) -> Option<String> {
    let names = node_names(schema);
    if names.is_empty() {
        return None;
    }
    let mut lines = vec![format!(
        "\n- Schema for {}; CALL db.schema('Node') covers the other domains:",
        DOMAINS.join(", ")
    )];
    for name in &names {
        let Some(node) = schema["nodes"]
            .as_array()?
            .iter()
            .find(|node| node["name"] == name.as_str())
        else {
            continue;
        };
        let properties: Vec<String> = node["properties"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(property)
            .collect();
        if properties.is_empty() {
            continue;
        }
        lines.push(format!("  - {name}: {}", properties.join(", ")));
    }
    let edges: Vec<String> = schema["edges"]
        .as_array()?
        .iter()
        .flat_map(|edge| edge_patterns(edge, &names))
        .collect();
    if !edges.is_empty() {
        lines.push(format!("  - Edges: {}", edges.join("; ")));
    }
    Some(lines.join("\n"))
}

fn property(property: &Value) -> Option<String> {
    let name = property["name"].as_str()?;
    let values: Vec<&str> = property["enum_values"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    Some(if values.is_empty() {
        name.to_string()
    } else {
        format!("{name} ({})", values.join("|"))
    })
}

fn edge_patterns(edge: &Value, names: &[String]) -> Vec<String> {
    let Some(kind) = edge["name"].as_str() else {
        return Vec::new();
    };
    let mut targets_by_source: Vec<(&str, Vec<&str>)> = Vec::new();
    for variant in edge["variants"].as_array().into_iter().flatten() {
        let (Some(source), Some(target)) = (
            listed(&variant["source_type"], names),
            listed(&variant["target_type"], names),
        ) else {
            continue;
        };
        match targets_by_source
            .iter_mut()
            .find(|(known, _)| *known == source)
        {
            Some((_, targets)) => targets.push(target),
            None => targets_by_source.push((source, vec![target])),
        }
    }
    let mut sources_by_targets: Vec<(String, Vec<&str>)> = Vec::new();
    for (source, targets) in targets_by_source {
        let targets = targets.join("|");
        match sources_by_targets
            .iter_mut()
            .find(|(known, _)| *known == targets)
        {
            Some((_, sources)) => sources.push(source),
            None => sources_by_targets.push((targets, vec![source])),
        }
    }
    sources_by_targets
        .into_iter()
        .map(|(targets, sources)| format!("({})-[:{kind}]->({targets})", sources.join("|")))
        .collect()
}

fn listed<'a>(side: &'a Value, names: &[String]) -> Option<&'a str> {
    side.as_str()
        .filter(|name| names.iter().any(|known| known == name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> Value {
        json!({
            "domains": [
                {"name": "core", "node_names": ["User", "Project"]},
                {"name": "code_review", "node_names": ["MergeRequest"]},
                {"name": "ci", "node_names": ["Pipeline"]}
            ],
            "nodes": [
                {"name": "User", "properties": [
                    {"name": "id", "enum_values": []},
                    {"name": "username", "enum_values": []}
                ]},
                {"name": "Project", "properties": [{"name": "full_path", "enum_values": []}]},
                {"name": "MergeRequest", "properties": [
                    {"name": "iid", "enum_values": []},
                    {"name": "state", "enum_values": ["opened", "merged"]}
                ]},
                {"name": "Pipeline", "properties": [{"name": "status", "enum_values": []}]}
            ],
            "edges": [
                {"name": "AUTHORED", "variants": [
                    {"source_type": "User", "target_type": "MergeRequest"}
                ]},
                {"name": "IN_PROJECT", "variants": [
                    {"source_type": "MergeRequest", "target_type": "Project"},
                    {"source_type": "Pipeline", "target_type": "Project"}
                ]},
                {"name": "TRIGGERED", "variants": [
                    {"source_type": "MergeRequest", "target_type": "Pipeline"}
                ]}
            ]
        })
    }

    #[test]
    fn node_names_keep_only_the_listed_domains() {
        assert_eq!(node_names(&schema()), ["User", "Project", "MergeRequest"]);
    }

    #[test]
    fn render_lists_properties_with_enum_values_and_edges_between_listed_nodes() {
        let rendered = render(&schema()).unwrap();
        assert!(rendered.starts_with("\n- Schema for core, code_review, plan;"));
        assert!(rendered.contains("  - User: id, username\n"));
        assert!(rendered.contains("  - MergeRequest: iid, state (opened|merged)\n"));
        assert!(rendered.contains("(User)-[:AUTHORED]->(MergeRequest)"));
        assert!(rendered.contains("(MergeRequest)-[:IN_PROJECT]->(Project)"));
        assert!(!rendered.contains("Pipeline"));
    }

    #[test]
    fn render_merges_sources_that_share_targets() {
        let mut schema = schema();
        schema["edges"][1]["variants"][1]["source_type"] = json!("User");
        let rendered = render(&schema).unwrap();
        assert!(rendered.contains("(MergeRequest|User)-[:IN_PROJECT]->(Project)"));
    }

    #[test]
    fn render_skips_a_node_without_properties() {
        let mut schema = schema();
        schema["nodes"][0]["properties"] = json!([]);
        let rendered = render(&schema).unwrap();
        assert!(!rendered.contains("  - User:"));
        assert!(rendered.contains("  - MergeRequest: iid, state (opened|merged)"));
        assert!(render(&json!({"domains": []})).is_none());
    }
}
