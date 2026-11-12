use anyhow::Result;
use orbit_third_party::{Target, ThirdParty};

pub(crate) fn enabled() -> bool {
    crate::commands::setup::spec::nightly_enabled()
}

pub(crate) fn require_nightly() -> Result<()> {
    anyhow::ensure!(
        enabled(),
        "`{} auth` is a nightly feature; set ORBIT_NIGHTLY=1 to try third-party sources",
        crate::commands::setup::spec::launcher()
    );
    Ok(())
}

fn configured() -> Option<ThirdParty> {
    if !enabled() {
        return None;
    }
    let root = crate::workspace::Workspace::default_root().ok()?;
    match ThirdParty::open(&root) {
        Ok(found) => found,
        Err(error) => {
            eprintln!("orbit: skipping third-party sources: {error:#}");
            None
        }
    }
}

pub(crate) fn providers() -> crate::remote::server_schema::Providers {
    let Some(third_party) = configured() else {
        return Default::default();
    };
    third_party
        .config
        .sources
        .iter()
        .map(|source| {
            (
                source.name.clone(),
                source.entities.keys().cloned().collect(),
            )
        })
        .collect()
}

pub(crate) async fn grep(query: &str, paths: &mut Vec<String>) -> Result<Option<Option<String>>> {
    let Some(third_party) = configured() else {
        return Ok(None);
    };
    let (scope, rest) = third_party.scope(query);
    anyhow::ensure!(
        scope.is_none() || !rest.contains('|'),
        "an OR across a third-party scope is not supported; run one grep per scope, such as `slack:message:a` and `grafana:dashboard:b`"
    );
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
        paths.is_empty(),
        "a third-party grep takes references such as slack:channel:name, not code paths: {}",
        paths.join(" ")
    );
    let mut scope = scope.unwrap_or_default();
    scope.within = targets.clone();
    let kinds: Vec<String> = scope
        .picks
        .iter()
        .filter_map(|(source, entity)| Some(format!("{source} {}s", third_party.kind_of(entity)?)))
        .collect();
    let mut nothing = format!(
        "no {} match {:?}",
        match kinds.is_empty() {
            true => "results".to_string(),
            false => kinds.join(", "),
        },
        rest.trim()
    );
    if !targets.is_empty() {
        let named: Vec<String> = targets
            .iter()
            .map(|target| third_party.shown(target))
            .collect();
        nothing.push_str(&format!(" in {}", named.join(", ")));
    }
    let patterns: Vec<String> = match orbit_search::query_alternatives(&rest) {
        Ok(alternatives) => alternatives,
        Err(_) => vec![rest.clone()],
    };
    let grepped = third_party.grep(&patterns, &scope).await;
    for (source, error) in &grepped.failed {
        eprintln!("orbit: {source} search failed: {error}");
    }
    match grepped.found.iter().any(|entity| !entity.hits.is_empty()) {
        true => Ok(Some(Some(orbit_third_party::render(&grepped.found)))),
        false => {
            eprintln!("orbit: {nothing}");
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
    let mut found: Vec<Target> = Vec::new();
    targets.retain(|target| match third_party.parse_target(target) {
        Some(parsed) => {
            found.push(parsed);
            false
        }
        None => true,
    });
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

pub(crate) async fn print_schema(nodes: &[String]) -> Result<bool> {
    let Some(third_party) = configured() else {
        return Ok(false);
    };
    let ours = !nodes.is_empty()
        && nodes
            .iter()
            .all(|node| third_party.config.entity(node).is_some());
    if ours {
        print!("{}", third_party.schema_of(nodes));
        return Ok(true);
    }
    if nodes.is_empty() {
        let result = crate::remote::run_ontology(Vec::new()).await;
        println!("\nThird-party entities (read live; query them on their own):");
        print!("{}", third_party.schema_of(&[]));
        result?;
        return Ok(true);
    }
    Ok(false)
}

pub(crate) async fn connect(name: &str, url: Option<String>, team: Option<String>) -> Result<()> {
    use orbit_third_party::setup::Connectable;
    let source = Connectable::named(name).ok_or_else(|| {
        anyhow::anyhow!(
            "no source named {name}; connectable: {}",
            Connectable::names().join(", ")
        )
    })?;
    let spec = source.spec();
    let env = source
        .token_env()
        .ok_or_else(|| anyhow::anyhow!("{} has no token variable", spec.title))?;
    let token = std::env::var(env)
        .ok()
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "set {env} first: {} Orbit reads it from the environment on every call and never saves it",
                spec.token_help
            )
        })?;
    let root = crate::workspace::Workspace::default_root()?;
    let path = ThirdParty::config_path(&root);
    let url = match (url, spec.asks_url) {
        (Some(url), _) => Some(url),
        (None, true) if std::io::IsTerminal::is_terminal(&std::io::stdin()) => {
            Some(crate::tui::input(
                &format!("{} server URL", spec.title),
                spec.url_help.as_deref().unwrap_or_default(),
                None,
            )?)
        }
        (None, true) => anyhow::bail!("{} needs --url", spec.title),
        (None, false) => None,
    };
    let base = url
        .as_deref()
        .or(source.default_url())
        .ok_or_else(|| anyhow::anyhow!("{} needs a server URL", spec.title))?;
    let verified = source.verify(base, token.trim()).await?;
    let can_ask = std::io::IsTerminal::is_terminal(&std::io::stdin());
    let team = pick_team(spec, &verified.teams, team, can_ask)?;
    let shown = match verified
        .teams
        .iter()
        .find(|(id, _)| Some(id) == team.as_ref())
    {
        Some((_, name)) => name.clone(),
        None => verified.shows.clone(),
    };
    orbit_third_party::setup::write_source(&path, name, url.as_deref(), team.as_deref())?;
    let who = match shown.is_empty() {
        true => String::new(),
        false => format!(" as {shown}"),
    };
    crate::tui::success(format!(
        "Connected {}{who}; every call reads the token from {env}",
        spec.title
    ));
    Ok(())
}

pub(crate) fn source_name_parser() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(orbit_third_party::setup::Connectable::names())
}

pub(crate) fn status() -> Result<()> {
    use orbit_third_party::setup::Connectable;
    let root = crate::workspace::Workspace::default_root()?;
    let path = ThirdParty::config_path(&root);
    let config = orbit_third_party::Config::load(&path)?;
    for source in Connectable::all() {
        let configured = config
            .as_ref()
            .and_then(|config| config.source(source.name));
        let strategy = configured
            .and_then(|found| found.auth.clone())
            .or_else(|| source.definition.auth.clone());
        let token = match &strategy {
            Some(strategy) => format!("{} {}", strategy.name(), strategy.describe()),
            None => "no auth".into(),
        };
        let line = match configured {
            Some(found) => format!(
                "{}  connected  {}  {token}",
                source.name,
                found.base_url.as_deref().unwrap_or_default()
            ),
            None => format!(
                "{}  not connected; run `{} auth {}`",
                source.name,
                crate::commands::setup::spec::launcher(),
                source.name
            ),
        };
        println!("{line}");
    }
    Ok(())
}

fn pick_team(
    spec: &orbit_third_party::config::SetupSpec,
    teams: &[(String, String)],
    requested: Option<String>,
    can_ask: bool,
) -> Result<Option<String>> {
    let names = || {
        teams
            .iter()
            .map(|(id, name)| format!("{id} ({name})"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    if let Some(requested) = requested {
        let found = teams
            .iter()
            .find(|(id, name)| id == &requested || name.eq_ignore_ascii_case(&requested));
        return match (found, teams.is_empty()) {
            (Some((id, _)), _) => Ok(Some(id.clone())),
            (None, true) => Ok(Some(requested)),
            (None, false) => anyhow::bail!(
                "{} has no workspace {requested}; this token can read {}",
                spec.title,
                names()
            ),
        };
    }
    match teams {
        [] => Ok(None),
        [(id, _)] => Ok(Some(id.clone())),
        _ if can_ask => {
            let choices: Vec<crate::tui::Choice> = teams
                .iter()
                .map(|(id, name)| crate::tui::Choice {
                    key: id.clone(),
                    label: name.clone(),
                    hint: id.clone(),
                    section: None,
                })
                .collect();
            Ok(Some(crate::tui::select(
                &format!("Which {} workspace?", spec.title),
                &choices,
                &teams[0].0,
            )?))
        }
        _ => anyhow::bail!(
            "this {} token spans several workspaces; pass --team with one of {}",
            spec.title,
            names()
        ),
    }
}

pub(crate) fn disconnect(name: &str) -> Result<()> {
    let root = crate::workspace::Workspace::default_root()?;
    let path = ThirdParty::config_path(&root);
    match orbit_third_party::setup::remove_source(&path, name)? {
        true => println!("orbit: disconnected {name}"),
        false => println!("orbit: {name} was not connected"),
    }
    Ok(())
}

pub(crate) async fn ask_to_connect() -> Result<()> {
    use orbit_third_party::setup::Connectable;
    let root = crate::workspace::Workspace::default_root()?;
    let path = ThirdParty::config_path(&root);
    let choices: Vec<crate::tui::Choice> = Connectable::all()
        .iter()
        .map(|source| {
            let connected = orbit_third_party::setup::configured(&path, source.name);
            let env = source.token_env().unwrap_or_default();
            crate::tui::Choice {
                key: source.name.to_string(),
                label: source.spec().title.clone(),
                hint: match (connected, std::env::var(env).is_ok()) {
                    (true, _) => "connected".into(),
                    (false, true) => format!("{env} is set"),
                    (false, false) => format!("set {env} first"),
                },
                section: None,
            }
        })
        .collect();
    let chosen = match crate::tui::multiselect(
        "Connect third-party sources? grep, context, and query can then search them",
        &choices,
        &[],
    ) {
        Ok(chosen) => chosen,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::Interrupted) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    for name in chosen {
        if let Err(error) = connect(&name, None, None).await {
            crate::tui::error(format!("{name}: {error:#}"));
        }
    }
    Ok(())
}
