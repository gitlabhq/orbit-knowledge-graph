use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};

const CACHE_VERSION: u32 = 2;
const TTL: Duration = Duration::from_secs(15 * 60);
const RETRY: Duration = Duration::from_secs(5 * 60);
const REFRESH_LEASE: Duration = Duration::from_secs(60);
const FETCH_TIMEOUT: Duration = Duration::from_secs(3);
const MIN_FETCH: Duration = Duration::from_millis(500);
const GITLAB: &str = "gitlab";
pub(crate) const MAX_ENTITIES: usize = 200;
pub(crate) const MAX_ENTITY_CHARS: usize = 3000;
const MAX_NAME_CHARS: usize = 48;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Provider {
    pub(crate) name: String,
    pub(crate) entities: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Entities {
    pub(crate) providers: Vec<Provider>,
}

impl Entities {
    fn is_empty(&self) -> bool {
        self.providers
            .iter()
            .all(|provider| provider.entities.is_empty())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Cached {
    version: u32,
    origin: String,
    fetched_at: u64,
    entities: Entities,
}

pub(crate) fn server_entities(dir: &Path, deadline: Instant) -> Option<Entities> {
    let origin = super::client::instance_in(dir)?;
    let path = cache_path(&origin)?;
    let now = unix_now();
    let cached = read_cache(&path, &origin);
    let usable = cached
        .as_ref()
        .is_some_and(|cached| !cached.entities.is_empty());
    if let Some(cached) = cached.filter(|_| usable) {
        if !fresh(&cached, now) && take_refresh_lease(&path, now) {
            spawn_refresh(dir);
        }
        return non_empty(cached.entities);
    }
    let budget = deadline
        .saturating_duration_since(Instant::now())
        .min(FETCH_TIMEOUT);
    if budget < MIN_FETCH || !take_refresh_lease(&path, now) {
        return None;
    }
    let entities = refresh_cache(dir, &origin, &path, Entities::default(), budget);
    let _ = std::fs::remove_file(lease_path(&path));
    entities
}

pub(crate) fn refresh_now(dir: &Path) {
    let Some(origin) = super::client::instance_in(dir) else {
        return;
    };
    let Some(path) = cache_path(&origin) else {
        return;
    };
    let stale = read_cache(&path, &origin)
        .map(|cached| cached.entities)
        .unwrap_or_default();
    refresh_cache(dir, &origin, &path, stale, FETCH_TIMEOUT);
    let _ = std::fs::remove_file(lease_path(&path));
}

fn refresh_cache(
    dir: &Path,
    origin: &str,
    path: &Path,
    stale: Entities,
    budget: Duration,
) -> Option<Entities> {
    let now = unix_now();
    let fetched = OrbitClient::from_env_in(dir)
        .ok()
        .and_then(|client| fetch_blocking(client, budget));
    let (entities, fetched_at) = match fetched {
        Some(fetched) => (fetched, now),
        None => (
            stale,
            now.saturating_sub(TTL.saturating_sub(RETRY).as_secs()),
        ),
    };
    write_cache(
        path,
        &Cached {
            version: CACHE_VERSION,
            origin: origin.to_string(),
            fetched_at,
            entities: entities.clone(),
        },
    );
    non_empty(entities)
}

fn lease_path(path: &Path) -> PathBuf {
    path.with_extension("refresh")
}

fn take_refresh_lease(path: &Path, now: u64) -> bool {
    let lease = lease_path(path);
    let held = std::fs::read_to_string(&lease)
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok())
        .is_some_and(|at| at <= now && now - at < REFRESH_LEASE.as_secs());
    if held {
        return false;
    }
    if let Some(parent) = lease.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(&lease, now.to_string()).is_ok()
}

fn spawn_refresh(dir: &Path) {
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let mut command = std::process::Command::new(exe);
    command
        .arg("refresh-schema")
        .current_dir(dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    let _ = command.spawn();
}

fn non_empty(entities: Entities) -> Option<Entities> {
    (!entities.is_empty()).then_some(entities)
}

fn fresh(cached: &Cached, now: u64) -> bool {
    cached.fetched_at <= now && now - cached.fetched_at < TTL.as_secs()
}

fn cache_path(origin: &str) -> Option<PathBuf> {
    let root = crate::workspace::Workspace::default_root().ok()?;
    let key = &ontology::migrations::sha256_hex(origin)[..16];
    Some(root.join("var").join(format!("server-schema-{key}.json")))
}

fn read_cache(path: &Path, origin: &str) -> Option<Cached> {
    let text = std::fs::read(path).ok()?;
    serde_json::from_slice::<Cached>(&text)
        .ok()
        .filter(|cached| cached.version == CACHE_VERSION && cached.origin == origin)
        .map(|cached| Cached {
            entities: valid(cached.entities),
            ..cached
        })
}

fn write_cache(path: &Path, cached: &Cached) {
    let Some(parent) = path.parent() else {
        return;
    };
    if std::fs::create_dir_all(parent).is_err() {
        return;
    }
    let Ok(body) = serde_json::to_vec(cached) else {
        return;
    };
    let staging = path.with_extension(format!("{}.tmp", std::process::id()));
    if std::fs::write(&staging, body).is_ok() && std::fs::rename(&staging, path).is_err() {
        let _ = std::fs::remove_file(&staging);
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

fn fetch_blocking(client: OrbitClient, budget: Duration) -> Option<Entities> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?;
        runtime.block_on(async {
            tokio::time::timeout(budget, fetch(&client))
                .await
                .ok()?
                .ok()
        })
    })
    .join()
    .ok()
    .flatten()
}

async fn fetch(client: &OrbitClient) -> Result<Entities, RemoteError> {
    let request = serde_json::to_vec(&serde_json::json!({
        "query": "CALL db.schema()",
        "response_format": "raw",
    }))
    .map_err(|error| RemoteError::new(EXIT_GENERIC, error.to_string()))?;
    let body = client.query_raw(request).await?;
    let entities = parse_entities(&body);
    match entities.is_empty() {
        true => Err(RemoteError::new(
            EXIT_GENERIC,
            "the schema lists no entities",
        )),
        false => Ok(entities),
    }
}

fn parse_entities(body: &[u8]) -> Entities {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Entities::default();
    };
    let entities = value["result"]["domains"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|domain| domain["nodes"].as_array().into_iter().flatten())
        .filter_map(|node| node.as_str().or_else(|| node["name"].as_str()))
        .map(str::to_string)
        .collect();
    valid(Entities {
        providers: vec![Provider {
            name: GITLAB.to_string(),
            entities,
        }],
    })
}

fn valid(entities: Entities) -> Entities {
    let mut seen: Vec<String> = Vec::new();
    let mut chars = 0;
    let mut providers = Vec::new();
    for provider in entities.providers {
        if !is_provider(&provider.name)
            || providers
                .iter()
                .any(|kept: &Provider| kept.name == provider.name)
        {
            continue;
        }
        let mut kept = Vec::new();
        for name in provider.entities.into_iter().filter(|name| is_name(name)) {
            if seen.contains(&name) {
                continue;
            }
            if seen.len() == MAX_ENTITIES || chars + name.len() + 2 > MAX_ENTITY_CHARS {
                continue;
            }
            chars += name.len() + 2;
            seen.push(name.clone());
            kept.push(name);
        }
        if !kept.is_empty() {
            providers.push(Provider {
                name: provider.name,
                entities: kept,
            });
        }
    }
    Entities { providers }
}

fn is_provider(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_CHARS
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_name(name: &str) -> bool {
    name.len() <= MAX_NAME_CHARS
        && name.starts_with(|c: char| c.is_ascii_uppercase())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    fn gitlab(list: &[&str]) -> Entities {
        Entities {
            providers: vec![Provider {
                name: "gitlab".into(),
                entities: names(list),
            }],
        }
    }

    #[test]
    fn every_domain_is_one_gitlab_provider() {
        let body = br#"{"result":{"domains":[
            {"name":"core","nodes":["Project",{"name":"User","props":"id"}]},
            {"name":"source_code","nodes":["File","Definition"]},
            {"name":"future","nodes":["Widget","bad name","Project"]}],
            "edges":[]},"query_type":"","row_count":0}"#;
        assert_eq!(
            parse_entities(body),
            gitlab(&["Project", "User", "File", "Definition", "Widget"])
        );
        assert!(parse_entities(b"<html>").is_empty());
        assert!(parse_entities(br#"{"error":"404 Not Found"}"#).is_empty());
    }

    #[test]
    fn entities_are_capped_by_count_and_length() {
        let long: Vec<String> = (0..120).map(|n| format!("E{n:0>47}")).collect();
        let refs: Vec<&str> = long.iter().map(String::as_str).collect();
        let kept = &valid(gitlab(&refs)).providers[0].entities;
        let chars: usize = kept.iter().map(|name| name.len() + 2).sum();
        assert!(chars <= MAX_ENTITY_CHARS, "{chars}");
        assert_eq!(kept.len(), MAX_ENTITY_CHARS / 50);

        let many: Vec<String> = (0..300).map(|n| format!("Thing{n}")).collect();
        let refs: Vec<&str> = many.iter().map(String::as_str).collect();
        assert_eq!(
            valid(gitlab(&refs)).providers[0].entities.len(),
            MAX_ENTITIES
        );
        assert!(
            valid(Entities {
                providers: vec![Provider {
                    name: "Not A Provider".into(),
                    entities: names(&["Project"]),
                }],
            })
            .is_empty()
        );
    }

    fn cached(fetched_at: u64) -> Cached {
        Cached {
            version: CACHE_VERSION,
            origin: "https://gitlab.example".into(),
            fetched_at,
            entities: gitlab(&["File", "Widget"]),
        }
    }

    #[test]
    fn one_refresh_runs_at_a_time_until_its_lease_ends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("var").join("schema.json");
        let now = 1_000_000;
        assert!(take_refresh_lease(&path, now));
        assert!(!take_refresh_lease(&path, now + 1));
        assert!(take_refresh_lease(&path, now + REFRESH_LEASE.as_secs()));
        std::fs::write(lease_path(&path), b"garbage").unwrap();
        assert!(take_refresh_lease(&path, now));
    }

    #[test]
    fn a_cache_is_fresh_for_the_ttl_only() {
        let now = 1_000_000;
        assert!(fresh(&cached(now), now));
        assert!(fresh(&cached(now - TTL.as_secs() + 1), now));
        assert!(!fresh(&cached(now - TTL.as_secs()), now));
        assert!(!fresh(&cached(now + 60), now));
    }

    #[test]
    fn a_cache_is_read_only_for_its_own_origin_and_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("var").join("schema.json");
        write_cache(&path, &cached(5));
        assert_eq!(
            read_cache(&path, "https://gitlab.example").map(|cached| cached.entities),
            Some(gitlab(&["File", "Widget"]))
        );
        assert!(read_cache(&path, "https://other.example").is_none());
        let mut newer = cached(5);
        newer.version = CACHE_VERSION + 1;
        write_cache(&path, &newer);
        assert!(read_cache(&path, "https://gitlab.example").is_none());
        std::fs::write(&path, b"{not json").unwrap();
        assert!(read_cache(&path, "https://gitlab.example").is_none());
    }

    #[test]
    fn a_cached_answer_gets_the_same_checks_as_a_fresh_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.json");
        let mut old = cached(5);
        let mut many: Vec<String> = (0..250).map(|n| format!("Entity{n}")).collect();
        many.insert(0, "bad name".into());
        old.entities.providers[0].entities = many;
        write_cache(&path, &old);
        let read = read_cache(&path, "https://gitlab.example")
            .unwrap()
            .entities;
        assert_eq!(read.providers[0].entities.len(), MAX_ENTITIES);
        assert_eq!(read.providers[0].entities[0], "Entity0");
    }
}
