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
    match grepped.found.iter().any(|entity| !entity.hits.is_empty()) {
        true => Ok(Some(Some(orbit_third_party::render(&grepped.found)))),
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

#[orbit_third_party::async_trait]
impl orbit_third_party::GitLab for Remote {
    async fn fetch(
        &self,
        label: &str,
        filters: &std::collections::HashMap<String, Vec<orbit_third_party::InputFilter>>,
        mentioned: &[String],
    ) -> Result<Vec<(String, orbit_third_party::schema::Row)>> {
        let literal = |value: &serde_json::Value| match value {
            serde_json::Value::String(text) => {
                format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            other => other.to_string(),
        };
        let mut wheres = Vec::new();
        for (field, filters) in filters {
            for filter in filters {
                use orbit_third_party::FilterOp::*;
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
                        "{label}.{field}: this filter can't be sent to GitLab in a mixed query"
                    ),
                };
                let value = filter.value.as_ref().map(literal).unwrap_or_default();
                wheres.push(format!("n.{field} {op} {value}"));
            }
        }
        let by_url = !mentioned.is_empty() && label == "MergeRequest";
        let query = match by_url {
            true => {
                let (paths, iids): (Vec<&str>, Vec<&str>) =
                    mentioned.iter().filter_map(|at| at.split_once('#')).unzip();
                let paths: std::collections::BTreeSet<String> =
                    paths.iter().map(|path| format!("'{path}'")).collect();
                wheres.push(format!(
                    "p.full_path IN [{}]",
                    paths.into_iter().collect::<Vec<_>>().join(", ")
                ));
                wheres.push(format!("n.iid IN [{}]", iids.join(", ")));
                format!(
                    "MATCH (n:{label})-[:IN_PROJECT]->(p:Project) WHERE {} RETURN n, p.full_path LIMIT 500",
                    wheres.join(" AND ")
                )
            }
            false => {
                anyhow::ensure!(
                    !wheres.is_empty(),
                    "{label} in a mixed query needs a filter, such as {label}.state = 'opened', or a link from a Slack message"
                );
                format!(
                    "MATCH (n:{label}) WHERE {} RETURN n LIMIT 500",
                    wheres.join(" AND ")
                )
            }
        };
        let body =
            serde_json::to_vec(&serde_json::json!({"query": query, "response_format": "raw"}))?;
        let response = self
            .0
            .query_raw(body)
            .await
            .map_err(|error| anyhow::anyhow!("GitLab: {}", error.message))?;
        let response: serde_json::Value = serde_json::from_slice(&response)?;
        let result = &response["result"];
        anyhow::ensure!(
            !result["pagination"]["truncated"].as_bool().unwrap_or(false)
                && !result["pagination"]["has_more"].as_bool().unwrap_or(false),
            "GitLab has more than 500 {label}s for that, so they would be cut off; narrow it with a filter"
        );
        let nodes = result["nodes"].as_array().cloned().unwrap_or_default();
        let path_of = |id: &str| {
            let edges = result["edges"].as_array()?;
            let to = edges
                .iter()
                .find(|edge| edge["from_id"] == id && edge["to"] == "Project")?["to_id"]
                .as_str()?;
            nodes.iter().find(|node| node["id"] == to)?["full_path"]
                .as_str()
                .map(String::from)
        };
        let mut rows = Vec::new();
        for node in nodes.iter().filter(|node| node["type"] == label) {
            let Some(id) = node["id"].as_str() else {
                continue;
            };
            let at = format!("{}#{}", path_of(id).unwrap_or_default(), node["iid"]);
            let values = node
                .as_object()
                .into_iter()
                .flatten()
                .filter(|(field, _)| !matches!(field.as_str(), "type" | "id"))
                .map(|(field, value)| (field.clone(), value.clone()))
                .collect();
            rows.push((
                at,
                orbit_third_party::schema::Row {
                    id: id.parse()?,
                    values,
                },
            ));
        }
        Ok(rows)
    }
}
