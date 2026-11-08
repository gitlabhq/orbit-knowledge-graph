use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cynic::{GraphQlErrorPathSegment, QueryBuilder};
use orbit_cli_graphql::queries::session_start::{
    ScopeIndexing, SessionStart, SessionStartVariables,
};
use serde::{Deserialize, Serialize};

use super::client::{OrbitClient, block_on_short};
use super::error::RemoteError;

const TTL: Duration = Duration::from_secs(15 * 60);
const SCHEMA_WAIT: Duration = Duration::from_secs(3);

pub(crate) type Providers = BTreeMap<String, Vec<String>>;

#[derive(Default)]
pub(crate) struct Session {
    pub(crate) reachable: bool,
    pub(crate) providers: Providers,
}

#[derive(Serialize, Deserialize)]
struct Cached {
    identity: String,
    fetched_at: u64,
    providers: Providers,
}

pub(crate) fn session_start(dir: &Path) -> Session {
    let Ok(client) = OrbitClient::from_env_in(dir) else {
        return Session::default();
    };
    let scope = client.get_host().ok().and_then(|host| {
        super::origin::project(dir)
            .filter(|(origin_host, _)| *origin_host == host)
            .map(|(_, path)| path)
    });
    let identity = ontology::migrations::sha256_hex(&client.identity());
    let cached = read_cache(&identity);
    let fresh = cached
        .as_ref()
        .is_some_and(|cached| now().saturating_sub(cached.fetched_at) < TTL.as_secs());
    let Some(fetched) = block_on_short(ask(client, scope, !fresh)) else {
        return Session::default();
    };
    let cached = cached.map(|cached| cached.providers);
    let providers = match fresh {
        true => cached.unwrap_or_default(),
        false => {
            let providers = fetched.or(cached).unwrap_or_default();
            write_cache(identity, &providers);
            providers
        }
    };
    Session {
        reachable: true,
        providers,
    }
}

async fn ask(
    client: OrbitClient,
    scope: Option<String>,
    with_schema: bool,
) -> Result<Option<Providers>, RemoteError> {
    match answer(&client, &scope, with_schema).await? {
        Some(data) if reachable(&data) => Ok(providers(&data)),
        Some(_) => Err(super::status::build_unavailable_error()),
        None => {
            match scope {
                Some(path) => client.check_graph_status(path).await?,
                None => client.check_status().await?,
            }
            Ok(None)
        }
    }
}

async fn answer(
    client: &OrbitClient,
    scope: &Option<String>,
    with_schema: bool,
) -> Result<Option<SessionStart>, RemoteError> {
    if !with_schema {
        return read(client, scope, false).await;
    }
    let combined = read(client, scope, true);
    tokio::pin!(combined);
    match tokio::time::timeout(SCHEMA_WAIT, &mut combined).await {
        Ok(Ok(Some(data))) => return Ok(Some(data)),
        Ok(_) => return read(client, scope, false).await,
        Err(_) => {}
    }
    let plain = read(client, scope, false);
    tokio::pin!(plain);
    tokio::select! {
        Ok(Some(data)) = &mut combined => Ok(Some(data)),
        data = &mut plain => data,
    }
}

async fn read(
    client: &OrbitClient,
    scope: &Option<String>,
    with_schema: bool,
) -> Result<Option<SessionStart>, RemoteError> {
    let full_path = cynic::Id::new(scope.clone().unwrap_or_default());
    let Some(response) = client
        .graphql(&SessionStart::build(SessionStartVariables {
            full_path: &full_path,
            scoped: scope.is_some(),
            with_schema,
        }))
        .await?
    else {
        return Ok(None);
    };
    let errors_only_in_schema = response.errors.iter().flatten().all(|error| {
        error.path.iter().flatten().any(
            |segment| matches!(segment, GraphQlErrorPathSegment::Field(field) if field == "schema"),
        )
    });
    Ok(response.data.filter(|_| errors_only_in_schema))
}

fn reachable(data: &SessionStart) -> bool {
    let indexed = |orbit: &Option<ScopeIndexing>| {
        orbit.as_ref().is_some_and(|orbit| orbit.indexing.is_some())
    };
    match data
        .orbit
        .as_ref()
        .and_then(|orbit| orbit.service_status.as_ref())
    {
        Some(status) => status.is_some(),
        None => {
            data.group
                .iter()
                .flatten()
                .any(|group| indexed(&group.orbit))
                || data
                    .project
                    .iter()
                    .flatten()
                    .any(|project| indexed(&project.orbit))
        }
    }
}

fn providers(data: &SessionStart) -> Option<Providers> {
    let nodes = data
        .orbit
        .as_ref()?
        .schema
        .as_ref()?
        .as_ref()?
        .nodes
        .nodes
        .as_ref()?;
    let mut entities: Vec<String> = Vec::new();
    for name in nodes.iter().flatten().map(|node| node.name.as_str()) {
        if is_name(name) && !entities.iter().any(|kept| kept == name) {
            entities.push(name.to_string());
        }
    }
    (!entities.is_empty()).then(|| BTreeMap::from([("gitlab".to_string(), entities)]))
}

fn is_name(name: &str) -> bool {
    name.len() <= 48
        && name.starts_with(|c: char| c.is_ascii_uppercase())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn cache_path(identity: &str) -> Option<PathBuf> {
    let root = crate::workspace::Workspace::default_root().ok()?;
    Some(
        root.join("var")
            .join(format!("server-schema-{}.json", &identity[..16])),
    )
}

fn read_cache(identity: &str) -> Option<Cached> {
    let text = std::fs::read(cache_path(identity)?).ok()?;
    serde_json::from_slice::<Cached>(&text)
        .ok()
        .filter(|cached| cached.identity == identity)
}

fn write_cache(identity: String, providers: &Providers) {
    let Some(path) = cache_path(&identity) else {
        return;
    };
    let cached = Cached {
        identity,
        fetched_at: now(),
        providers: providers.clone(),
    };
    if let (Some(parent), Ok(body)) = (path.parent(), serde_json::to_vec(&cached)) {
        let _ = std::fs::create_dir_all(parent);
        let _ = std::fs::write(path, body);
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}
