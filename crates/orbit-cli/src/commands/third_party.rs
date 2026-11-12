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
    anyhow::ensure!(
        theirs.is_empty(),
        "this query mixes third-party entities ({}) with GitLab ones ({}); query them separately",
        ours.join(", "),
        theirs.join(", ")
    );
    let format = match response_format {
        None | Some(crate::remote::ResponseFormat::Llm) => crate::sql_format::Format::Table,
        Some(crate::remote::ResponseFormat::Raw) => crate::sql_format::Format::Json,
        Some(crate::remote::ResponseFormat::Gql) => {
            anyhow::bail!("third-party queries print llm (a table) or raw (JSON rows)")
        }
    };
    let batches = third_party.query(query).await?;
    crate::sql_format::write(std::io::stdout().lock(), format, &batches)?;
    Ok(true)
}
