use anyhow::Result;
use orbit_third_party::{Target, ThirdParty};

fn configured() -> Option<ThirdParty> {
    let root = crate::workspace::Workspace::default_root().ok()?;
    match ThirdParty::open(&root) {
        Ok(found) => found,
        Err(error) => {
            eprintln!("orbit: skipping third-party sources: {error:#}");
            None
        }
    }
}

async fn refresh(third_party: &ThirdParty) {
    match third_party.ensure_fresh().await {
        Ok(freshness) => {
            for (source, error) in freshness.failed {
                eprintln!("orbit: {source} did not sync, using what was stored: {error}");
            }
        }
        Err(error) => eprintln!("orbit: third-party sync failed: {error:#}"),
    }
}

/// Searches third-party data only when the query names a source, such as `slack:words` or
/// `slack:message:words`, or a path is a `source:type:key` reference, such as
/// `slack:channel:orbit-team`; a plain grep stays on local code. References are taken out of
/// `paths`. `Some(None)` means it was scoped to third-party data and nothing matched.
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
        paths.is_empty() || targets.is_empty(),
        "grep searches either code paths or third-party references, not both: {}",
        paths.join(" ")
    );
    refresh(&third_party).await;
    let mut scope = scope.unwrap_or_default();
    for target in &targets {
        scope
            .within
            .push((target.source.clone(), third_party.resolve(target)?));
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
    match third_party.grep(&patterns, Some(&scope), GREP_LIMIT) {
        Ok(found) if found.iter().any(|entity| !entity.hits.is_empty()) => {
            Ok(Some(Some(orbit_third_party::render(&found))))
        }
        Ok(_) => {
            eprintln!("orbit: {nothing}");
            Ok(Some(None))
        }
        Err(error) => {
            eprintln!("orbit: third-party search failed: {error:#}");
            Ok(Some(None))
        }
    }
}

const GREP_LIMIT: usize = 20;

/// Reads `source:type:key` targets, such as `slack:user:U0123ABCD`, removing them from
/// `targets`.
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
    refresh(&third_party).await;
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
        out.push(third_party.context(target)?);
    }
    Ok(Some(out.join("\n")))
}

/// Runs a query locally when every label it matches on is a third-party entity.
pub(crate) async fn query_locally(query: &str) -> Result<bool> {
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
    refresh(&third_party).await;
    let batches = third_party.query(query)?;
    crate::sql_format::write(
        std::io::stdout().lock(),
        crate::sql_format::Format::Table,
        &batches,
    )?;
    Ok(true)
}

/// `orbit schema` with only third-party node names prints those locally; with none, it
/// lists the third-party entities after the GitLab schema.
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
        println!("\nThird-party entities (local; query them on their own):");
        print!("{}", third_party.schema_of(&[]));
        result?;
        return Ok(true);
    }
    Ok(false)
}

/// Connects a built-in source: asks for its URL when it needs one and for a token, checks
/// the token, saves it in the OS keychain, adds the source to the config, and syncs it.
/// `token` skips the prompt, as with `--yes` and an environment variable.
/// Where `orbit auth` gets the token from: piped in, or else the source's variable when set,
/// or else a prompt.
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
    match &env {
        Some(_) => {
            orbit_third_party::credentials::remove(name)?;
        }
        None => orbit_third_party::credentials::store(name, &token)?,
    }
    orbit_third_party::setup::write_source(&path, name, url.as_deref(), team.as_deref())?;
    let who = match shown.is_empty() {
        true => String::new(),
        false => format!(" as {shown}"),
    };
    let kept = match &env {
        Some(env) => format!("each sync reads the token from {env}, nothing is saved"),
        None => "the token is in the OS keychain".into(),
    };
    crate::tui::success(format!("Connected {}{who}; {kept}", spec.title));
    orbit_third_party::store::forget(&ThirdParty::store_path_in(&root), name, false)?;
    let Some(third_party) = ThirdParty::open(&root)? else {
        return Ok(());
    };
    let spinner = crate::tui::spinner(format!("Syncing {}", spec.title));
    let freshness = third_party.ensure_fresh().await?;
    spinner.clear();
    for (failed, error) in &freshness.failed {
        crate::tui::warning(format!("{failed} did not sync: {error}"));
    }
    let synced: Vec<String> = freshness
        .synced
        .iter()
        .filter(|(synced, _, _)| synced == name)
        .map(|(_, entity, count)| format!("{count} {entity}"))
        .collect();
    if !synced.is_empty() {
        crate::tui::success(format!("Synced {}", synced.join(", ")));
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

/// Lists each connectable source, whether it is connected, and where its token comes from.
pub(crate) fn status() -> Result<()> {
    use orbit_third_party::credentials::{TokenFrom, resolve};
    use orbit_third_party::setup::Connectable;
    let root = crate::workspace::Workspace::default_root()?;
    let path = ThirdParty::config_path(&root);
    let config = orbit_third_party::Config::load(&path)?;
    for source in Connectable::all() {
        let configured = config
            .as_ref()
            .and_then(|config| config.source(source.name));
        let auth = configured.and_then(|found| found.auth.as_ref());
        let env = auth
            .and_then(|auth| auth.token_env.as_deref())
            .or(source.token_env());
        let token = match resolve(source.name, env).map(|(_, from)| from) {
            Some(TokenFrom::Env) => format!("token from {}", env.unwrap_or_default()),
            Some(TokenFrom::Keychain) => "token in the OS keychain".into(),
            None => "no token".into(),
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

/// The team a multi-team token should read: `--team` when given, the only one when there is
/// one, otherwise a prompt. `None` when the token needs no team.
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

/// Removes the source from the config and its token from the keychain.
pub(crate) fn disconnect(name: &str) -> Result<()> {
    let root = crate::workspace::Workspace::default_root()?;
    let path = ThirdParty::config_path(&root);
    let in_config = orbit_third_party::setup::remove_source(&path, name)?;
    let in_keychain = orbit_third_party::credentials::remove(name)?;
    orbit_third_party::store::forget(&ThirdParty::store_path_in(&root), name, true)?;
    match in_config || in_keychain {
        true => println!("orbit: disconnected {name}"),
        false => println!("orbit: {name} was not connected"),
    }
    Ok(())
}

/// The `orbit setup` step: offers every connectable source that is not connected yet.
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
    let chosen = crate::tui::multiselect(
        "Connect third-party sources? grep, context, and query can then search them",
        &choices,
        &[],
    )?;
    for name in chosen {
        if let Err(error) = connect(&name, TokenInput::Prompt, None, None).await {
            crate::tui::error(format!("{name}: {error:#}"));
        }
    }
    Ok(())
}
