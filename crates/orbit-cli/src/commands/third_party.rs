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
            let entities: Vec<String> = source
                .entities
                .iter()
                .filter(|(_, entity)| entity.fetch.is_some())
                .map(|(name, _)| name.clone())
                .collect();
            (source.name.clone(), entities)
        })
        .filter(|(_, entities)| !entities.is_empty())
        .collect()
}

fn refresh(third_party: &ThirdParty) -> Vec<String> {
    let staleness = match third_party.staleness() {
        Ok(staleness) => staleness,
        Err(error) => {
            eprintln!("orbit: third-party sync failed: {error:#}");
            return Vec::new();
        }
    };
    for (source, error) in &staleness.failed {
        eprintln!("orbit: {source} did not sync: {error}");
    }
    if !staleness.never.is_empty() || !staleness.old.is_empty() {
        spawn_background_sync();
    }
    staleness.never
}

fn syncing() -> bool {
    sync_lock_path().is_some_and(|path| path.exists())
}

fn while_first_sync(first: &[String]) -> Option<String> {
    (!first.is_empty() && syncing()).then(|| {
        format!(
            "{} {} syncing for the first time in the background; try again in a few seconds",
            first.join(", "),
            if first.len() == 1 { "is" } else { "are" }
        )
    })
}

fn explain(error: anyhow::Error, first: &[String]) -> anyhow::Error {
    match while_first_sync(first) {
        Some(why) => error.context(why),
        None => error,
    }
}

const SYNC_LOCK_TTL: std::time::Duration = std::time::Duration::from_secs(10 * 60);

fn sync_lock_path() -> Option<std::path::PathBuf> {
    let root = crate::workspace::Workspace::default_root().ok()?;
    Some(root.join("third-party").join("sync.lock"))
}

fn take_sync_lock() -> Option<std::path::PathBuf> {
    let path = sync_lock_path()?;
    std::fs::create_dir_all(path.parent()?).ok()?;
    let stale = std::fs::metadata(&path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age > SYNC_LOCK_TTL);
    if stale {
        let _ = std::fs::remove_file(&path);
    }
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .ok()?;
    Some(path)
}

fn spawn_background_sync() {
    let Some(lock) = take_sync_lock() else {
        return;
    };
    let Ok(exe) = std::env::current_exe() else {
        let _ = std::fs::remove_file(&lock);
        return;
    };
    let mut command = std::process::Command::new(exe);
    command
        .arg("third-party-sync")
        .arg("--lock")
        .arg(&lock)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    if command.spawn().is_err() {
        let _ = std::fs::remove_file(&lock);
    }
}

pub(crate) async fn background_sync(lock: std::path::PathBuf) -> Result<()> {
    for _ in 0..BACKGROUND_PASSES {
        let Some(third_party) = configured() else {
            break;
        };
        let synced = match third_party.ensure_fresh().await {
            Ok(freshness) => {
                third_party.record_failures(&freshness);
                !freshness.synced.is_empty()
            }
            Err(error) => {
                third_party.record_failure_all(&format!("{error:#}"));
                false
            }
        };
        let _ = std::fs::write(&lock, b"");
        if !synced {
            break;
        }
        let pending = third_party
            .staleness()
            .is_ok_and(|staleness| !staleness.never.is_empty());
        if !pending {
            break;
        }
    }
    let _ = std::fs::remove_file(lock);
    Ok(())
}

const BACKGROUND_PASSES: usize = 3;

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
    let first = refresh(&third_party);
    let mut scope = scope.unwrap_or_default();
    for target in &targets {
        match third_party.resolve_to_key(target) {
            Ok(key) => {
                let id = third_party
                    .resolve(target)
                    .map_err(|error| explain(error, &first))?;
                scope.within.push((target.source.clone(), id));
                scope
                    .within_keys
                    .push((target.source.clone(), target.entity.clone(), key));
            }
            Err(error) => match third_party.live_name(target) {
                Some(name) => {
                    scope
                        .within_names
                        .push((target.source.clone(), target.entity.clone(), name))
                }
                None => return Err(explain(error, &first)),
            },
        }
    }
    let kinds: Vec<String> = scope
        .picks
        .iter()
        .filter_map(|(source, entity)| {
            Some(format!(
                "{source} {}s",
                third_party.kind_of(entity.as_deref()?)?
            ))
        })
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
    let rest = match rest.trim().is_empty() {
        true => ".".to_string(),
        false => rest,
    };
    let patterns: Vec<String> = match orbit_search::query_alternatives(&rest) {
        Ok(alternatives) => alternatives,
        Err(_) => vec![rest.clone()],
    };
    for (source, error) in third_party.search_live(&patterns, &mut scope).await {
        eprintln!("orbit: {source} search failed: {error}");
    }
    match third_party.grep(&patterns, Some(&scope), GREP_LIMIT) {
        Ok(found) if found.iter().any(|entity| !entity.hits.is_empty()) => {
            Ok(Some(Some(orbit_third_party::render(&found))))
        }
        Ok(_) => {
            eprintln!("orbit: {nothing}");
            if let Some(why) = while_first_sync(&first) {
                eprintln!("orbit: {why}");
            }
            Ok(Some(None))
        }
        Err(error) => {
            eprintln!("orbit: third-party search failed: {error:#}");
            if let Some(why) = while_first_sync(&first) {
                eprintln!("orbit: {why}");
            }
            Ok(Some(None))
        }
    }
}

const GREP_LIMIT: usize = 20;

pub(crate) async fn context(targets: &mut Vec<String>) -> Result<Option<String>> {
    if targets.is_empty() {
        return Ok(None);
    }
    let Some(third_party) = configured() else {
        return Ok(None);
    };
    if !targets
        .iter()
        .any(|target| third_party.parse_target(target).is_some())
    {
        return Ok(None);
    }
    let first = refresh(&third_party);
    let mut found: Vec<Target> = Vec::new();
    targets.retain(|target| match third_party.parse_target(target) {
        Some(parsed) => {
            found.push(parsed);
            false
        }
        None => true,
    });
    let mut out = Vec::new();
    for target in &found {
        out.push(
            third_party
                .context_live(target)
                .await
                .map_err(|error| explain(error, &first))?,
        );
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
        "this query mixes third-party entities ({}) with GitLab ones ({}); the third-party graph is local, so query them separately",
        ours.join(", "),
        theirs.join(", ")
    );
    let first = refresh(&third_party);
    let batches = third_party
        .query(query)
        .map_err(|error| explain(error, &first))?;
    let format = match response_format {
        None | Some(crate::remote::ResponseFormat::Llm) => crate::sql_format::Format::Table,
        Some(crate::remote::ResponseFormat::Raw) => crate::sql_format::Format::Json,
        Some(crate::remote::ResponseFormat::Gql) => {
            anyhow::bail!(
                "third-party queries run locally and print llm (a table) or raw (JSON rows)"
            )
        }
    };
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
        let text = third_party.schema_of(nodes);
        anyhow::ensure!(
            !text.is_empty(),
            "{} {} searched and fetched live, not stored, so it has no schema to query",
            nodes.join(", "),
            if nodes.len() == 1 { "is" } else { "are" }
        );
        print!("{text}");
        return Ok(true);
    }
    if nodes.is_empty() {
        let result = crate::remote::run_ontology(Vec::new()).await;
        println!("\nThird-party entities (local; query them on their own):");
        print!("{}", third_party.schema_of(&[]));
        result?;
        return Ok(true);
    }
    Ok(false)
}

pub(crate) enum TokenInput {
    Prompt,
    Given(String),
}

pub(crate) async fn connect(
    name: &str,
    input: TokenInput,
    url: Option<String>,
    team: Option<String>,
) -> Result<()> {
    use orbit_third_party::setup::Connectable;
    let source = Connectable::named(name).ok_or_else(|| {
        anyhow::anyhow!(
            "no source named {name}; connectable: {}",
            Connectable::names().join(", ")
        )
    })?;
    let spec = source.spec();
    let root = crate::workspace::Workspace::default_root()?;
    let path = ThirdParty::config_path(&root);
    let from_env = source
        .token_env()
        .and_then(|env| Some((env.to_string(), std::env::var(env).ok()?)))
        .filter(|(_, token)| !token.trim().is_empty());
    let prompting = matches!(input, TokenInput::Prompt) && from_env.is_none();
    anyhow::ensure!(
        !prompting || std::io::IsTerminal::is_terminal(&std::io::stdin()),
        "stdin is not a terminal; pipe the token with `{} auth {name} --token-stdin`",
        crate::commands::setup::spec::launcher()
    );
    let url = match (url, spec.asks_url) {
        (Some(url), _) => Some(url),
        (None, true) if prompting => Some(crate::tui::input(
            &format!("{} server URL", spec.title),
            spec.url_help.as_deref().unwrap_or_default(),
            None,
        )?),
        (None, true) => anyhow::bail!("{} needs --url", spec.title),
        (None, false) => None,
    };
    let base = url
        .as_deref()
        .or(source.default_url())
        .ok_or_else(|| anyhow::anyhow!("{} needs a server URL", spec.title))?;
    let env = match &input {
        TokenInput::Prompt => from_env.as_ref().map(|(env, _)| env.clone()),
        TokenInput::Given(_) => None,
    };
    let token = match (input, from_env) {
        (TokenInput::Given(token), _) => token,
        (TokenInput::Prompt, Some((_, token))) => token.trim().to_string(),
        (TokenInput::Prompt, None) => {
            crate::tui::card(format!("{} token", spec.title), &spec.token_help)?;
            crate::tui::secret(&format!("Paste the {} token", spec.title))?
        }
    };
    anyhow::ensure!(!token.is_empty(), "no token given");
    if let Some(prefix) = &spec.token_prefix
        && env.is_none()
        && !token.starts_with(prefix.as_str())
    {
        crate::tui::warning(format!(
            "{} tokens usually start with {prefix}; trying it anyway",
            spec.title
        ));
    }
    let verified = source.verify(base, &token).await?;
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
    let saved = orbit_third_party::credentials::store(name, &token);
    if let (Err(error), None) = (&saved, &env) {
        anyhow::bail!("{error:#}");
    }
    orbit_third_party::setup::write_source(&path, name, url.as_deref(), team.as_deref())?;
    let who = match shown.is_empty() {
        true => String::new(),
        false => format!(" as {shown}"),
    };
    let kept = match (&saved, &env) {
        (Ok(()), Some(env)) => format!(
            "saved {env}'s token in the {}; {env} wins over it while it is set",
            orbit_third_party::credentials::STORE_NAME
        ),
        (Ok(()), None) => format!(
            "the token is in the {}",
            orbit_third_party::credentials::STORE_NAME
        ),
        (Err(error), Some(env)) => {
            format!("could not save the token ({error:#}); only shells with {env} set can reach it")
        }
        (Err(_), None) => unreachable!("checked before writing the source"),
    };
    crate::tui::success(format!("Connected {}{who}; {kept}", spec.title));
    orbit_third_party::store::forget(&ThirdParty::store_path_in(&root), name, false)?;
    if let Some(third_party) = ThirdParty::open(&root)? {
        third_party.clear_failure(name);
        spawn_background_sync();
        crate::tui::success(format!(
            "Syncing {} in the background; grep, context, and query can read it in a few seconds",
            spec.title
        ));
    }
    Ok(())
}

pub(crate) fn source_name_parser() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(orbit_third_party::setup::Connectable::names())
}

pub(crate) fn read_token_stdin() -> Result<String> {
    let mut token = String::new();
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut token)?;
    Ok(token.trim().to_string())
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
            Some(strategy) => format!("{} {}", strategy.name(), strategy.describe(source.name)),
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
    if let Ok(Some(third_party)) = ThirdParty::open(&root) {
        third_party.clear_failure(name);
    }
    let in_config = orbit_third_party::setup::remove_source(&path, name)?;
    let in_keychain = orbit_third_party::credentials::remove(name)?;
    orbit_third_party::store::forget(&ThirdParty::store_path_in(&root), name, true)?;
    match in_config || in_keychain {
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
            crate::tui::Choice {
                key: source.name.to_string(),
                label: source.spec().title.clone(),
                hint: match connected {
                    true => "connected; pick to replace the token".into(),
                    false => source.definition.description.clone(),
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
        if let Err(error) = connect(&name, TokenInput::Prompt, None, None).await {
            crate::tui::error(format!("{name}: {error:#}"));
        }
    }
    Ok(())
}
