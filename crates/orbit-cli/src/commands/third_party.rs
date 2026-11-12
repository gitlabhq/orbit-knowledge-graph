use anyhow::Result;
use orbit_third_party::{Target, ThirdParty};

pub(crate) fn enabled() -> bool {
    crate::commands::setup::spec::nightly_enabled()
}

fn configured() -> Option<ThirdParty> {
    enabled().then(ThirdParty::from_env).flatten()
}

pub(crate) fn providers() -> crate::remote::server_schema::Providers {
    let Some(third_party) = configured() else {
        return Default::default();
    };
    let sources = third_party.config.sources.iter();
    let names =
        |source: &orbit_third_party::config::Source| source.entities.keys().cloned().collect();
    sources
        .map(|source| (source.name.clone(), names(source)))
        .collect()
}

pub(crate) async fn grep(query: &str, paths: &mut Vec<String>) -> Result<Option<Option<String>>> {
    let Some(third_party) = configured() else {
        return Ok(None);
    };
    let (scope, rest) = third_party.scope(query);
    let mut targets = Vec::new();
    paths.retain(|path| match third_party.parse_target(path) {
        Some(target) if !std::path::Path::new(path).exists() => {
            targets.push(target);
            false
        }
        _ => true,
    });
    if scope.is_none() && targets.is_empty() {
        return Ok(None);
    }
    anyhow::ensure!(
        !rest.contains('|'),
        "an OR is not supported in a third-party grep; run one grep per term, such as `slack:message:a` then `slack:message:b`"
    );
    anyhow::ensure!(
        !rest.trim().is_empty() || !targets.is_empty(),
        "add words to search for, such as `{q}:words`, or `.` to list them all",
        q = query.trim()
    );
    anyhow::ensure!(
        paths.is_empty(),
        "a third-party grep takes references such as slack:channel:name, not code paths: {}",
        paths.join(" ")
    );
    let mut scope = scope.unwrap_or_default();
    scope.within = targets;
    let grepped = third_party.grep(&rest, &scope).await;
    for (source, error) in &grepped.failed {
        eprintln!("orbit: {source} search failed: {error}");
    }
    let searched = grepped.found.len();
    match grepped.found.iter().any(|entity| !entity.hits.is_empty()) {
        true => Ok(Some(Some(orbit_third_party::render(&grepped.found)))),
        false if searched == 0 && !grepped.failed.is_empty() => {
            anyhow::bail!("every third-party search failed, so there are no results to show")
        }
        false => {
            eprintln!("orbit: no third-party results match {:?}", rest.trim());
            Ok(Some(None))
        }
    }
}

pub(crate) async fn context(targets: &mut Vec<String>) -> Result<Option<String>> {
    if targets.is_empty() {
        return Ok(None);
    }
    let Some(third_party) = configured() else {
        return Ok(None);
    };
    let found: Vec<Target> = targets
        .extract_if(.., |target| third_party.parse_target(target).is_some())
        .filter_map(|target| third_party.parse_target(&target))
        .collect();
    if found.is_empty() {
        return Ok(None);
    }
    let mut out = Vec::new();
    for target in &found {
        out.push(third_party.context(target).await?);
    }
    Ok(Some(out.join("\n")))
}

pub(crate) async fn query_locally(
    query: &str,
    response_format: Option<crate::remote::ResponseFormat>,
) -> Result<bool> {
    let Some(third_party) = configured() else {
        return Ok(false);
    };
    let (ours, theirs) = third_party.labels_in(query);
    if ours.is_empty() {
        return Ok(false);
    }
    let gitlab = match theirs.is_empty() {
        true => None,
        false => Some(Remote(
            crate::remote::client::OrbitClient::from_env().map_err(|error| {
                anyhow::anyhow!(
                    "this query also names GitLab entities ({}), which needs a GitLab sign-in: {}",
                    theirs.join(", "),
                    error.message
                )
            })?,
        )),
    };
    let format = match response_format {
        None | Some(crate::remote::ResponseFormat::Llm) => crate::sql_format::Format::Table,
        Some(crate::remote::ResponseFormat::Raw) => crate::sql_format::Format::Json,
        Some(crate::remote::ResponseFormat::Gql) => {
            anyhow::bail!("third-party queries print llm (a table) or raw (JSON rows)")
        }
    };
    let batches = third_party
        .query(
            query,
            gitlab
                .as_ref()
                .map(|remote| remote as &dyn orbit_third_party::GitLab),
        )
        .await?;
    crate::sql_format::write(std::io::stdout().lock(), format, &batches)?;
    Ok(true)
}

struct Remote(crate::remote::client::OrbitClient);

fn literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => {
            format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
        }
        serde_json::Value::Array(items) => format!(
            "[{}]",
            items.iter().map(literal).collect::<Vec<_>>().join(", ")
        ),
        other => other.to_string(),
    }
}

fn conditions(node: &orbit_third_party::InputNode) -> Result<Vec<String>> {
    use orbit_third_party::FilterOp::*;
    let mut wheres = Vec::new();
    for (field, filters) in &node.filters {
        for filter in filters {
            let op = match filter.op.unwrap_or(Eq) {
                Eq => "=",
                Ne => "<>",
                Gt => ">",
                Lt => "<",
                Gte => ">=",
                Lte => "<=",
                In => "IN",
                Contains => "CONTAINS",
                StartsWith => "STARTS WITH",
                EndsWith => "ENDS WITH",
                _ => anyhow::bail!(
                    "{}.{field}: this filter can't be sent to GitLab in a mixed query",
                    node.id
                ),
            };
            let value = filter.value.as_ref().map(literal).unwrap_or_default();
            wheres.push(format!("{}.{field} {op} {value}", node.id));
        }
    }
    Ok(wheres)
}

#[orbit_third_party::async_trait]
impl orbit_third_party::GitLab for Remote {
    async fn fetch(&self, part: &orbit_third_party::Part) -> Result<orbit_third_party::Fetched> {
        let mut patterns: Vec<String> = part
            .nodes
            .iter()
            .map(|node| format!("({}:{})", node.id, node.entity.clone().unwrap_or_default()))
            .collect();
        for edge in &part.relationships {
            let kind = edge.types.join("|");
            patterns.push(match edge.direction {
                orbit_third_party::Direction::Incoming => {
                    format!("({})<-[:{kind}]-({})", edge.from, edge.to)
                }
                _ => format!("({})-[:{kind}]->({})", edge.from, edge.to),
            });
        }
        let mut wheres = Vec::new();
        for node in &part.nodes {
            wheres.extend(conditions(node)?);
        }
        if let Some((anchor, mentioned)) = &part.anchor {
            anyhow::ensure!(
                !mentioned.is_empty(),
                "no Slack message in that search links to a GitLab merge request or issue"
            );
            let (paths, iids): (Vec<&str>, Vec<&str>) =
                mentioned.iter().filter_map(|at| at.split_once('#')).unzip();
            let paths: std::collections::BTreeSet<String> =
                paths.iter().map(|path| format!("'{path}'")).collect();
            patterns.push(format!(
                "({anchor})-[:IN_PROJECT]->(anchor_project:Project)"
            ));
            wheres.push(format!(
                "anchor_project.full_path IN [{}]",
                paths.into_iter().collect::<Vec<_>>().join(", ")
            ));
            wheres.push(format!("{anchor}.iid IN [{}]", iids.join(", ")));
        }
        anyhow::ensure!(
            !wheres.is_empty(),
            "the GitLab part of a mixed query needs a filter, or a link from a Slack message"
        );
        let mut returned: Vec<String> = part.nodes.iter().map(|node| node.id.clone()).collect();
        if part.anchor.is_some() {
            returned.push("anchor_project.full_path".into());
        }
        let query = format!(
            "MATCH {} WHERE {} RETURN {} LIMIT 500",
            patterns.join(", "),
            wheres.join(" AND "),
            returned.join(", ")
        );
        let body =
            serde_json::to_vec(&serde_json::json!({"query": query, "response_format": "raw"}))?;
        let response = self
            .0
            .query_raw(body)
            .await
            .map_err(|error| anyhow::anyhow!("GitLab: {}", error.message))?;
        let response: serde_json::Value = serde_json::from_slice(&response)?;
        let result = &response["result"];
        let pagination = &result["pagination"];
        anyhow::ensure!(
            !pagination["truncated"].as_bool().unwrap_or(false)
                && !pagination["has_more"].as_bool().unwrap_or(false),
            "GitLab has more than 500 rows for that, so they would be cut off; narrow it with a filter"
        );
        let nodes = result["nodes"].as_array().cloned().unwrap_or_default();
        let edges = result["edges"].as_array().cloned().unwrap_or_default();
        let path_of = |id: &str| {
            let to = edges
                .iter()
                .find(|edge| edge["from_id"] == id && edge["to"] == "Project")?["to_id"]
                .as_str()?;
            nodes.iter().find(|node| node["id"] == to)?["full_path"]
                .as_str()
                .map(String::from)
        };
        let labels: Vec<&str> = part
            .nodes
            .iter()
            .filter_map(|node| node.entity.as_deref())
            .collect();
        let mut fetched = orbit_third_party::Fetched::default();
        for node in &nodes {
            let (Some(kind), Some(id)) = (node["type"].as_str(), node["id"].as_str()) else {
                continue;
            };
            if !labels.contains(&kind) {
                continue;
            }
            let at = format!("{}#{}", path_of(id).unwrap_or_default(), node["iid"]);
            let values = node
                .as_object()
                .into_iter()
                .flatten()
                .filter(|(field, _)| !matches!(field.as_str(), "type" | "id"))
                .map(|(field, value)| (field.clone(), value.clone()))
                .collect();
            fetched.rows.push((
                kind.to_string(),
                at,
                orbit_third_party::schema::Row {
                    id: id.parse()?,
                    values,
                },
            ));
        }
        for edge in &edges {
            let (Some(from), Some(to), Some(kind)) = (
                edge["from"].as_str(),
                edge["to"].as_str(),
                edge["type"].as_str(),
            ) else {
                continue;
            };
            if !labels.contains(&from) || !labels.contains(&to) {
                continue;
            }
            let id = |field: &str| edge[field].as_str().and_then(|id| id.parse::<i64>().ok());
            if let (Some(from_id), Some(to_id)) = (id("from_id"), id("to_id")) {
                fetched.edges.push(orbit_third_party::schema::Edge {
                    from: (from_id, from.into()),
                    kind: kind.into(),
                    to: (to_id, to.into()),
                });
            }
        }
        Ok(fetched)
    }
}
