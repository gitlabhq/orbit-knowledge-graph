pub mod auth;
pub mod config;
pub mod credentials;
pub mod fetch;
pub mod schema;
pub mod setup;
pub mod store;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use duckdb_client::DuckDbClient;
use ontology::Ontology;

pub use config::Config;

pub const CONFIG_FILE: &str = "third-party.yaml";
pub const CONFIG_ENV: &str = "ORBIT_THIRD_PARTY_CONFIG";

pub struct ThirdParty {
    pub config: Config,
    pub ontology: Arc<Ontology>,
    store: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Scope {
    pub picks: Vec<(String, Option<String>)>,
    pub within: Vec<(String, i64)>,
    pub within_keys: Vec<(String, String, String)>,
    pub within_names: Vec<(String, String, String)>,
    pub live: BTreeMap<(String, String), Vec<i64>>,
    pub live_hits: Vec<Found>,
}

impl Scope {
    fn covers(&self, source: &str, entity: &str) -> bool {
        let within = (self.within.is_empty() && self.within_names.is_empty())
            || self
                .within
                .iter()
                .any(|(within, _)| within.eq_ignore_ascii_case(source));
        self.picks_allow(source, entity) && within
    }

    fn picks_allow(&self, source: &str, entity: &str) -> bool {
        self.picks.is_empty()
            || self.picks.iter().any(|(picked, kind)| {
                picked.eq_ignore_ascii_case(source)
                    && kind.as_deref().is_none_or(|kind| kind == entity)
            })
    }

    fn narrowed_by_name(&self, source: &str) -> bool {
        self.within_names
            .iter()
            .any(|(within, _, _)| within.eq_ignore_ascii_case(source))
    }

    fn within(&self, source: &str) -> Vec<i64> {
        self.within
            .iter()
            .filter(|(within, _)| within.eq_ignore_ascii_case(source))
            .map(|(_, id)| *id)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hit {
    pub source: String,
    pub entity: String,
    pub id: i64,
    pub reference: String,
    pub label: String,
    pub shown: Vec<String>,
    pub matched: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub source: String,
    pub entity: String,
    pub hits: Vec<Hit>,
    pub total: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Staleness {
    pub never: Vec<String>,
    pub old: Vec<String>,
    pub failed: Vec<(String, String)>,
}

const RETRY_AFTER_FAILURE: i64 = 60;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct Failures(BTreeMap<String, (i64, String)>);

#[derive(Debug, Default)]
pub struct Freshness {
    pub synced: Vec<(String, String, usize)>,
    pub failed: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub source: String,
    pub entity: String,
    pub key: String,
}

const CONTEXT_LINKS: usize = 10;

const HEADLINE: usize = 100;

impl ThirdParty {
    pub fn config_path(root: &Path) -> PathBuf {
        std::env::var_os(CONFIG_ENV)
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join(CONFIG_FILE))
    }

    pub fn open(root: &Path) -> Result<Option<Self>> {
        let Some(config) = Config::load(&Self::config_path(root))? else {
            return Ok(None);
        };
        let ontology = schema::build(&config)?;
        Ok(Some(Self {
            config,
            ontology,
            store: Self::store_path_in(root),
        }))
    }

    pub fn store_path(&self) -> &Path {
        &self.store
    }

    pub fn store_path_in(root: &Path) -> PathBuf {
        root.join("third-party").join("graph.duckdb")
    }

    pub fn staleness(&self) -> Result<Staleness> {
        let Some(client) = self.reader()? else {
            let failures = self.failures();
            return Ok(Staleness {
                never: self
                    .config
                    .sources
                    .iter()
                    .filter(|source| {
                        failures.0.get(&source.name).is_none_or(|(at, _)| {
                            store::now().saturating_sub(*at) >= RETRY_AFTER_FAILURE
                        })
                    })
                    .map(|source| source.name.clone())
                    .collect(),
                old: Vec::new(),
                failed: failures
                    .0
                    .into_iter()
                    .filter(|(source, _)| self.config.source(source).is_some())
                    .map(|(source, (_, error))| (source, error))
                    .collect(),
            });
        };
        let now = store::now();
        let failures = self.failures();
        let mut staleness = Staleness::default();
        for source in &self.config.sources {
            let failed = failures.0.get(&source.name);
            if let Some((_, error)) = failed {
                staleness.failed.push((source.name.clone(), error.clone()));
            }
            let retry = failed.is_none_or(|(at, _)| now.saturating_sub(*at) >= RETRY_AFTER_FAILURE);
            match store::synced_at(&client, &source.name, &signature(source)).unwrap_or(None) {
                None if retry => staleness.never.push(source.name.clone()),
                Some(at) if retry && now.saturating_sub(at) >= source.refresh.as_secs() as i64 => {
                    staleness.old.push(source.name.clone())
                }
                _ => {}
            }
        }
        Ok(staleness)
    }

    fn failures_path(&self) -> PathBuf {
        self.store.with_file_name("failures.json")
    }

    fn failures(&self) -> Failures {
        std::fs::read(self.failures_path())
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    /// Keeps each source's last sync failure until it syncs again, for commands to show.
    pub fn record_failure_all(&self, error: &str) {
        let freshness = Freshness {
            synced: Vec::new(),
            failed: self
                .config
                .sources
                .iter()
                .map(|source| (source.name.clone(), error.to_string()))
                .collect(),
        };
        self.record_failures(&freshness);
    }

    pub fn clear_failure(&self, source: &str) {
        let mut failures = self.failures();
        if failures.0.remove(source).is_some() {
            self.write_failures(&failures);
        }
    }

    fn write_failures(&self, failures: &Failures) {
        let Ok(bytes) = serde_json::to_vec(failures) else {
            return;
        };
        let path = self.failures_path();
        let temporary = path.with_extension("json.tmp");
        if std::fs::write(&temporary, bytes).is_ok() {
            let _ = std::fs::rename(&temporary, &path);
        }
    }

    pub fn record_failures(&self, freshness: &Freshness) {
        let mut failures = self.failures();
        for (source, _, _) in &freshness.synced {
            failures.0.remove(source);
        }
        let now = store::now();
        for (source, error) in &freshness.failed {
            failures.0.insert(source.clone(), (now, error.clone()));
        }
        failures
            .0
            .retain(|source, _| self.config.source(source).is_some());
        self.write_failures(&failures);
    }

    pub async fn ensure_fresh(&self) -> Result<Freshness> {
        let client = store::open(&self.store, &self.ontology)?;
        let now = store::now();
        let failures = self.failures();
        let mut stale = Vec::new();
        for source in &self.config.sources {
            let signature = signature(source);
            let fresh = store::synced_at(&client, &source.name, &signature)?
                .is_some_and(|at| now.saturating_sub(at) < source.refresh.as_secs() as i64);
            let backing_off = failures
                .0
                .get(&source.name)
                .is_some_and(|(at, _)| now.saturating_sub(*at) < RETRY_AFTER_FAILURE);
            if !fresh && !backing_off {
                stale.push((source, signature));
            }
        }
        drop(client);
        let mut freshness = Freshness::default();
        if stale.is_empty() {
            return Ok(freshness);
        }
        let fetcher = fetch::Fetcher::new()?;
        for (source, signature) in stale {
            let fetched = fetcher.source(source).await;
            let client = store::open(&self.store, &self.ontology)?;
            let entities = match fetched {
                Ok(entities) => entities,
                Err(error) => {
                    freshness
                        .failed
                        .push((source.name.clone(), format!("{error:#}")));
                    continue;
                }
            };
            for (entity, records) in entities {
                let kinds: Vec<_> = source.entities[&entity]
                    .properties
                    .iter()
                    .map(|(name, property)| (name.clone(), property.kind))
                    .collect();
                store::replace(&client, &source.name, &entity, &kinds, &records)?;
                freshness
                    .synced
                    .push((source.name.clone(), entity, records.len()));
            }
            store::mark_synced(&client, &source.name, &signature)?;
        }
        Ok(freshness)
    }

    pub fn scope(&self, query: &str) -> (Option<Scope>, String) {
        let mut picks = Vec::new();
        let mut rest = Vec::new();
        for word in query.split_whitespace() {
            let Some((name, value)) = word.split_once(':') else {
                rest.push(word.to_string());
                continue;
            };
            let Some(source) = self
                .config
                .sources
                .iter()
                .find(|source| source.name == name)
            else {
                rest.push(word.to_string());
                continue;
            };
            let (kind, value) = value.split_once(':').unwrap_or((value, ""));
            let exact = source
                .entities
                .iter()
                .find(|(_, entity)| entity.kind == kind);
            let Some(entity) = exact.map(|(name, _)| name.clone()) else {
                rest.push(word.to_string());
                continue;
            };
            picks.push((source.name.clone(), Some(entity)));
            if !value.is_empty() {
                rest.push(value.to_string());
            }
        }
        let rest = rest.join(" ");
        match picks.is_empty() {
            true => (None, rest),
            false => (
                Some(Scope {
                    picks,
                    ..Scope::default()
                }),
                rest,
            ),
        }
    }

    pub fn parse_target(&self, target: &str) -> Option<Target> {
        let mut parts = target.splitn(3, ':');
        let (source, kind, key) = (parts.next()?, parts.next()?, parts.next()?);
        let source = self.config.source(source)?;
        let entity = self.config.kind(source, kind)?;
        (!key.is_empty()).then(|| Target {
            source: source.name.clone(),
            entity,
            key: key.to_string(),
        })
    }

    fn reference(&self, source: &str, entity: &str, key: &str) -> String {
        match self.config.entity(entity) {
            Some((_, definition)) => format!("{source}:{}:{key}", definition.kind),
            None => format!("{source}:{entity}:{key}"),
        }
    }

    fn reader(&self) -> Result<Option<DuckDbClient>> {
        store::open_read(&self.store)
    }

    pub fn grep(
        &self,
        patterns: &[String],
        scope: Option<&Scope>,
        limit: usize,
    ) -> Result<Vec<Found>> {
        let mut found: Vec<Found> = scope
            .map(|scope| scope.live_hits.clone())
            .unwrap_or_default();
        let Some(client) = self.reader()? else {
            return Ok(found);
        };
        for (source, name, entity) in self.config.entities() {
            if entity.fetch.is_none() {
                continue;
            }
            let searched_live = scope.is_some_and(|scope| {
                scope
                    .live
                    .contains_key(&(source.name.clone(), name.to_string()))
            });
            if !searched_live && scope.is_some_and(|scope| !scope.covers(&source.name, name)) {
                continue;
            }
            let mut fields: Vec<&str> = vec![entity.label.as_str()];
            fields.extend(
                entity
                    .search
                    .iter()
                    .map(String::as_str)
                    .filter(|f| *f != entity.label),
            );
            let mut alternatives = Vec::new();
            let mut params: Vec<serde_json::Value> = vec![source.name.clone().into()];
            for pattern in patterns {
                let words: Vec<&str> = pattern.split_whitespace().collect();
                if words.is_empty() {
                    continue;
                }
                let mut all = Vec::new();
                for word in words {
                    let any: Vec<String> = fields
                        .iter()
                        .map(|field| {
                            format!(
                                "regexp_matches(coalesce(CAST(t.{field} AS VARCHAR), ''), ?, 'i')"
                            )
                        })
                        .collect();
                    for _ in &fields {
                        params.push(loose(word).into());
                    }
                    all.push(format!("({})", any.join(" OR ")));
                }
                alternatives.push(format!("({})", all.join(" AND ")));
            }
            if alternatives.is_empty() {
                continue;
            }
            let table = schema::table(name);
            let live =
                scope.and_then(|scope| scope.live.get(&(source.name.clone(), name.to_string())));
            let mut filter = match live {
                Some(ids) if ids.is_empty() => continue,
                Some(ids) => {
                    params.truncate(1);
                    let list = ids
                        .iter()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("t.source = ? AND t.id IN ({list})")
                }
                None => format!("t.source = ? AND ({})", alternatives.join(" OR ")),
            };
            let unfiltered_live = live.is_some()
                && scope.is_some_and(|scope| {
                    let filters = entity.live.as_ref().map(|live| &live.within);
                    scope
                        .within_keys
                        .iter()
                        .filter(|(within, _, _)| within.eq_ignore_ascii_case(&source.name))
                        .any(|(_, to, _)| filters.is_none_or(|filters| !filters.contains_key(to)))
                });
            if let Some(ids) = scope.map(|scope| scope.within(&source.name))
                && !ids.is_empty()
                && (live.is_none() || unfiltered_live)
            {
                let list = ids
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                filter.push_str(&format!(
                    " AND (t.id IN ({list}) OR EXISTS (SELECT 1 FROM {edges} w WHERE \
                     (w.source_id = t.id AND w.target_id IN ({list})) OR \
                     (w.target_id = t.id AND w.source_id IN ({list}))))",
                    edges = store::EDGE_TABLE,
                ));
            }
            let total = duckdb_client::i64_column(
                &client.query_arrow_json(
                    &format!("SELECT count(*) AS n FROM {table} t WHERE {filter}"),
                    &params,
                )?,
                "n",
            )
            .first()
            .copied()
            .unwrap_or_default() as usize;
            if total == 0 {
                continue;
            }
            let mut select: Vec<String> = fields
                .iter()
                .map(|field| format!("CAST(t.{field} AS VARCHAR) AS {field}"))
                .collect();
            let mut joins = Vec::new();
            for (index, shown) in entity.shows.iter().enumerate() {
                let alias = format!("_shown_{index}");
                match shown.split_once('.') {
                    Some((link, property)) => {
                        let Some(target) = entity.links.iter().find(|found| found.kind == link)
                        else {
                            continue;
                        };
                        let Some((target_name, _)) = self.config.entity(&target.to) else {
                            continue;
                        };
                        let (edge, node) = (format!("e{index}"), format!("n{index}"));
                        joins.push(format!(
                            "LEFT JOIN {edges} {edge} ON {edge}.source_id = t.id AND {edge}.relationship_kind = '{link}' \
                             LEFT JOIN {other} {node} ON {node}.id = {edge}.target_id",
                            edges = store::EDGE_TABLE,
                            other = schema::table(target_name),
                        ));
                        match entity.named.get(link) {
                            Some(own) => select.push(format!(
                                "CAST(coalesce({node}.{property}, t.{own}) AS VARCHAR) AS {alias}"
                            )),
                            None => select
                                .push(format!("CAST({node}.{property} AS VARCHAR) AS {alias}")),
                        }
                    }
                    None => select.push(format!("CAST(t.{shown} AS VARCHAR) AS {alias}")),
                }
            }
            let order = match &entity.newest {
                Some(field) => format!("ORDER BY t.{field} DESC NULLS LAST, t.id"),
                None => "ORDER BY t.id".to_string(),
            };
            let sql = format!(
                "SELECT t.id AS id, t.key AS _key, {} FROM {table} t {} WHERE {filter} {order} LIMIT {limit}",
                select.join(", "),
                joins.join(" "),
            );
            let batches = client.query_arrow_json(&sql, &params)?;
            let ids = duckdb_client::i64_column(&batches, "id");
            let keys = duckdb_client::string_column(&batches, "_key");
            let shown_columns: Vec<Vec<Option<String>>> = (0..entity.shows.len())
                .map(|index| optional_strings(&batches, &format!("_shown_{index}")))
                .collect();
            let columns: Vec<Vec<Option<String>>> = fields
                .iter()
                .map(|field| optional_strings(&batches, field))
                .collect();
            let mut hits = Vec::new();
            let matchers: Vec<regex::Regex> = patterns
                .iter()
                .flat_map(|p| p.split_whitespace().map(str::to_string).collect::<Vec<_>>())
                .filter_map(|word| {
                    regex::RegexBuilder::new(&loose(&word))
                        .case_insensitive(true)
                        .build()
                        .ok()
                })
                .collect();
            for (row, id) in ids.into_iter().enumerate() {
                let shown_values: Vec<String> = shown_columns
                    .iter()
                    .filter_map(|column| column.get(row).cloned().flatten())
                    .collect();
                let value = |column: usize| {
                    columns[column]
                        .get(row)
                        .cloned()
                        .flatten()
                        .unwrap_or_default()
                };
                let matched = fields
                    .iter()
                    .enumerate()
                    .skip(1)
                    .filter(|(_, field)| !entity.shows.iter().any(|shown| shown == *field))
                    .filter(|(column, _)| {
                        let text = value(*column);
                        !shown_values
                            .iter()
                            .any(|shown| shown.trim_start_matches('#') == text)
                    })
                    .filter_map(|(column, field)| {
                        let text = value(column);
                        matchers
                            .iter()
                            .any(|m| m.is_match(&text))
                            .then(|| (field.to_string(), one_line(&text)))
                    })
                    .collect();
                let shown = entity
                    .shows
                    .iter()
                    .zip(&shown_columns)
                    .filter_map(|(field, column)| {
                        let value = column.get(row).cloned().flatten()?;
                        let value = match field.split_once('.') {
                            Some(("IN_CHANNEL", _)) => format!("#{value}"),
                            _ => value,
                        };
                        Some(short_time(&value))
                    })
                    .filter(|value| !value.is_empty())
                    .collect();
                hits.push(Hit {
                    source: source.name.clone(),
                    entity: name.to_string(),
                    id,
                    reference: self.reference(
                        &source.name,
                        name,
                        keys.get(row).map(String::as_str).unwrap_or_default(),
                    ),
                    label: one_line(&value(0)),
                    shown,
                    matched,
                });
            }
            found.push(Found {
                source: source.name.clone(),
                entity: name.to_string(),
                hits,
                total,
            });
        }
        Ok(found)
    }

    pub async fn context_live(&self, target: &Target) -> Result<String> {
        let (name, definition) = self
            .config
            .entity(&target.entity)
            .with_context(|| format!("{} is not a configured entity", target.entity))?;
        let shown = format!("{}:{}:{}", target.source, definition.kind, target.key);
        if definition.fetch.is_some() {
            let mut out = self.context(target)?;
            let resolved = Target {
                key: self.resolve_to_key(target)?,
                ..target.clone()
            };
            for (source, linked, entity) in self.config.entities() {
                let Some(live) = &entity.live else { continue };
                if source.name != target.source || !live.within.contains_key(name) {
                    continue;
                }
                let within = [(name.to_string(), resolved.key.clone())];
                let records = fetch::Fetcher::new()?
                    .search(source, linked, entity, "", &within)
                    .await
                    .unwrap_or_default();
                if records.is_empty() {
                    continue;
                }
                let link = entity
                    .links
                    .iter()
                    .find(|link| link.to == name)
                    .map_or("LINKS", |link| link.kind.as_str());
                out.push_str(&format!(
                    "\n<-[{link}]- {} (latest {})\n",
                    entity.kind,
                    records.len().min(CONTEXT_LINKS)
                ));
                for record in records.iter().take(CONTEXT_LINKS) {
                    let hit = self.live_hit(&source.name, linked, entity, record);
                    out.push_str(&format!(
                        "  {}  {}\n",
                        hit.reference,
                        truncate(&hit.label, 80)
                    ));
                }
            }
            return Ok(out);
        }
        let source = self
            .config
            .source(&target.source)
            .with_context(|| format!("no source {}", target.source))?;
        let record = fetch::Fetcher::new()?
            .get_one(source, name, definition, &target.key)
            .await?
            .with_context(|| format!("{} has nothing at {shown}", source.name))?;
        let hit = self.live_hit(&source.name, name, definition, &record);
        let mut out = match hit.label.chars().count() > HEADLINE {
            true => format!("{shown}\n"),
            false => format!("{shown}  {}\n", hit.label),
        };
        if !hit.shown.is_empty() {
            out.push_str(&format!("{}\n", hit.shown.join(" · ")));
        }
        for (field, value) in &record.properties {
            let value = match value {
                serde_json::Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            if !value.is_empty()
                && (field != &definition.label || hit.label.chars().count() > HEADLINE)
            {
                out.push_str(&format!("{field}: {}\n", value.trim_end()));
            }
        }
        let links: Vec<String> = definition
            .links
            .iter()
            .filter_map(|link| {
                let key = record.properties.get(&link.via)?.as_str()?;
                let kind = self.config.entity(&link.to)?.1.kind.clone();
                Some(format!("-[{}]-> {}:{kind}:{key}", link.kind, target.source))
            })
            .collect();
        if !links.is_empty() {
            out.push_str(&format!("\nlinks:\n{}\n", links.join("\n")));
        }
        Ok(out)
    }

    pub fn context(&self, target: &Target) -> Result<String> {
        let client = self
            .reader()?
            .context("no third-party data yet; no source has synced")?;
        let (name, definition) = self
            .config
            .entity(&target.entity)
            .with_context(|| format!("{} is not a configured entity", target.entity))?;
        let table = schema::table(name);
        let asked = format!("{}:{}:{}", target.source, definition.kind, target.key);
        let key = self.resolve_key(&client, target, name, definition, &asked)?;
        let shown = format!("{}:{}:{key}", target.source, definition.kind);
        let mut columns = vec!["id".to_string(), "key".into(), "source".into()];
        columns.extend(definition.properties.keys().cloned());
        let select: Vec<String> = columns
            .iter()
            .map(|c| format!("CAST({c} AS VARCHAR) AS {c}"))
            .collect();
        let batches = client.query_arrow_json(
            &format!(
                "SELECT {} FROM {table} WHERE source = ? AND key = ? LIMIT 1",
                select.join(", ")
            ),
            &[target.source.clone().into(), key.into()],
        )?;
        let values: Vec<Vec<Option<String>>> = columns
            .iter()
            .map(|c| optional_strings(&batches, c))
            .collect();
        let get = |column: &str| {
            columns
                .iter()
                .position(|c| c == column)
                .and_then(|i| values[i][0].clone())
                .unwrap_or_default()
        };
        let id: i64 = get("id").parse()?;
        let label = one_line(&get(&definition.label));
        let mut out = match label.chars().count() > HEADLINE {
            true => format!("{shown}\n"),
            false => format!("{shown}  {label}\n"),
        };
        for field in definition.properties.keys() {
            let value = get(field);
            if field == &definition.label && label.chars().count() <= HEADLINE {
                continue;
            }
            if !value.is_empty() {
                out.push_str(&format!("{field}: {}\n", value.trim_end()));
            }
        }
        let edges = client.query_arrow_json(
            &format!(
                "SELECT relationship_kind, target_id AS other, target_kind AS kind, 'out' AS dir FROM {e} WHERE source_id = ? \
                 UNION ALL SELECT relationship_kind, source_id, source_kind, 'in' FROM {e} WHERE target_id = ?",
                e = store::EDGE_TABLE
            ),
            &[id.into(), id.into()],
        )?;
        let kinds = duckdb_client::string_column(&edges, "relationship_kind");
        let others = duckdb_client::i64_column(&edges, "other");
        let other_kinds = duckdb_client::string_column(&edges, "kind");
        let dirs = duckdb_client::string_column(&edges, "dir");
        let mut grouped: BTreeMap<(String, String, String), Vec<i64>> = BTreeMap::new();
        for (((kind, other), other_kind), dir) in
            kinds.into_iter().zip(others).zip(other_kinds).zip(dirs)
        {
            grouped
                .entry((dir, kind, other_kind))
                .or_default()
                .push(other);
        }
        if !grouped.is_empty() {
            out.push_str("\nlinks:\n");
        }
        for ((dir, kind, other_kind), ids) in grouped {
            let labels = self.labels(&client, &other_kind, &ids, CONTEXT_LINKS)?;
            let arrow = match dir.as_str() {
                "out" => format!("-[{kind}]->"),
                _ => format!("<-[{kind}]-"),
            };
            let other_type = self
                .config
                .entity(&other_kind)
                .map_or(other_kind.as_str(), |(_, entity)| entity.kind.as_str());
            out.push_str(&format!("{arrow} {other_type} ({})\n", ids.len()));
            for (reference, label) in labels {
                out.push_str(&format!("  {reference}  {label}\n"));
            }
            if ids.len() > CONTEXT_LINKS {
                out.push_str(&format!("  … {} more\n", ids.len() - CONTEXT_LINKS));
            }
        }
        Ok(out)
    }

    pub fn kind_of(&self, entity: &str) -> Option<String> {
        self.config
            .entity(entity)
            .map(|(_, found)| found.kind.clone())
    }

    pub fn shown(&self, target: &Target) -> String {
        let kind = self.kind_of(&target.entity).unwrap_or_default();
        format!("{}:{kind}:{}", target.source, target.key)
    }

    pub async fn search_live(
        &self,
        patterns: &[String],
        scope: &mut Scope,
    ) -> Vec<(String, String)> {
        let query = patterns
            .iter()
            .map(|pattern| pattern.trim())
            .filter(|pattern| !pattern.is_empty() && *pattern != ".")
            .collect::<Vec<_>>()
            .join(" ");
        let fetcher = match fetch::Fetcher::new() {
            Ok(fetcher) => fetcher,
            Err(error) => return vec![("third-party".into(), format!("{error:#}"))],
        };
        let wanted: Vec<(&config::Source, &str, &config::Entity)> = self
            .config
            .entities()
            .filter(|(source, name, entity)| {
                entity.live.is_some()
                    && (scope.covers(&source.name, name)
                        || (scope.narrowed_by_name(&source.name)
                            && scope.picks_allow(&source.name, name)))
            })
            .collect();
        let searches = wanted.iter().map(|(source, name, entity)| {
            let live = entity.live.as_ref().expect("filtered on live");
            let mut within: Vec<(String, String)> = scope
                .within_keys
                .iter()
                .filter(|(within, _, _)| within.eq_ignore_ascii_case(&source.name))
                .map(|(_, to, key)| (to.clone(), key.clone()))
                .collect();
            for (_, to, name) in scope
                .within_names
                .iter()
                .filter(|(within, _, _)| within.eq_ignore_ascii_case(&source.name))
            {
                if let Some(filter) = live.within_name.get(to) {
                    within.push((format!("{to}#name"), filter.replace("{name}", name)));
                }
            }
            let fetcher = &fetcher;
            let query = query.clone();
            async move {
                let found = fetcher.search(source, name, entity, &query, &within).await;
                (*source, *name, *entity, found)
            }
        });
        let results = futures::future::join_all(searches).await;
        let mut failed = Vec::new();
        for (source, name, entity, found) in results {
            let records = match found {
                Ok(records) => records,
                Err(error) => {
                    failed.push((source.name.clone(), format!("{error:#}")));
                    Vec::new()
                }
            };
            scope
                .live
                .insert((source.name.clone(), name.to_string()), Vec::new());
            let total = records.len();
            let hits = records
                .iter()
                .map(|record| self.live_hit(&source.name, name, entity, record))
                .collect();
            scope.live_hits.push(Found {
                source: source.name.clone(),
                entity: name.to_string(),
                hits,
                total,
            });
        }
        failed
    }

    fn live_hit(
        &self,
        source: &str,
        name: &str,
        entity: &config::Entity,
        record: &fetch::Record,
    ) -> Hit {
        let text = |field: &str| {
            record
                .properties
                .get(field)
                .map(|value| match value {
                    serde_json::Value::String(text) => text.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default()
        };
        let shown = entity
            .shows
            .iter()
            .filter_map(|shown| {
                let value = match shown.split_once('.') {
                    Some((link, property)) => {
                        let target = entity.links.iter().find(|found| found.kind == link)?;
                        let key = text(&target.via);
                        let stored = self.stored_label(source, &target.to, &key, property);
                        let value =
                            stored.or_else(|| entity.named.get(link).map(|own| text(own)))?;
                        match link {
                            "IN_CHANNEL" => format!("#{value}"),
                            _ => value,
                        }
                    }
                    None => text(shown),
                };
                (!value.is_empty()).then(|| short_time(&value))
            })
            .collect();
        Hit {
            source: source.to_string(),
            entity: name.to_string(),
            id: record.id,
            reference: self.reference(source, name, &record.key),
            label: one_line(&text(&entity.label)),
            shown,
            matched: Vec::new(),
        }
    }

    fn stored_label(
        &self,
        source: &str,
        entity: &str,
        key: &str,
        property: &str,
    ) -> Option<String> {
        let (name, _) = self.config.entity(entity)?;
        let client = self.reader().ok()??;
        let batches = client
            .query_arrow_json(
                &format!(
                    "SELECT CAST({property} AS VARCHAR) AS value FROM {} WHERE source = ? AND key = ?",
                    schema::table(name)
                ),
                &[source.into(), key.into()],
            )
            .ok()?;
        optional_strings(&batches, "value")
            .into_iter()
            .flatten()
            .next()
    }

    pub fn resolve(&self, target: &Target) -> Result<i64> {
        if let Some((name, definition)) = self.config.entity(&target.entity)
            && definition.fetch.is_none()
        {
            return Ok(store::id(&target.source, name, &target.key));
        }
        let client = self
            .reader()?
            .context("no third-party data yet; no source has synced")?;
        let (name, definition) = self
            .config
            .entity(&target.entity)
            .with_context(|| format!("{} is not a configured entity", target.entity))?;
        let asked = format!("{}:{}:{}", target.source, definition.kind, target.key);
        let key = self.resolve_key(&client, target, name, definition, &asked)?;
        Ok(store::id(&target.source, name, &key))
    }

    pub fn live_name(&self, target: &Target) -> Option<String> {
        let source = self.config.source(&target.source)?;
        source
            .entities
            .values()
            .filter_map(|entity| entity.live.as_ref())
            .any(|live| live.within_name.contains_key(&target.entity))
            .then(|| target.key.trim_start_matches(['@', '#']).to_string())
    }

    pub fn resolve_to_key(&self, target: &Target) -> Result<String> {
        if self
            .config
            .entity(&target.entity)
            .is_some_and(|(_, definition)| definition.fetch.is_none())
        {
            return Ok(target.key.clone());
        }
        let client = self
            .reader()?
            .context("no third-party data yet; no source has synced")?;
        let (name, definition) = self
            .config
            .entity(&target.entity)
            .with_context(|| format!("{} is not a configured entity", target.entity))?;
        let asked = format!("{}:{}:{}", target.source, definition.kind, target.key);
        self.resolve_key(&client, target, name, definition, &asked)
    }

    fn resolve_key(
        &self,
        client: &DuckDbClient,
        target: &Target,
        name: &str,
        definition: &config::Entity,
        asked: &str,
    ) -> Result<String> {
        let table = schema::table(name);
        let keys = |sql: &str, value: &str| -> Result<Vec<String>> {
            let batches = client.query_arrow_json(
                &format!("SELECT key FROM {table} WHERE source = ? AND {sql} ORDER BY key LIMIT 6"),
                &[target.source.clone().into(), value.into()],
            )?;
            Ok(duckdb_client::string_column(&batches, "key"))
        };
        if !keys("key = ?", &target.key)?.is_empty() {
            return Ok(target.key.clone());
        }
        let bare = target.key.trim_start_matches(['#', '@']);
        if bare != target.key && !keys("key = ?", bare)?.is_empty() {
            return Ok(bare.to_string());
        }
        let named = keys(
            &format!("lower(CAST({} AS VARCHAR)) = lower(?)", definition.label),
            bare,
        )?;
        match named.as_slice() {
            [] => bail!("nothing synced is {asked}"),
            [key] => Ok(key.clone()),
            several => bail!(
                "{} {}s are named {bare}; pick one: {}",
                several.len(),
                definition.kind,
                several
                    .iter()
                    .map(|key| format!("{}:{}:{key}", target.source, definition.kind))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }

    fn labels(
        &self,
        client: &DuckDbClient,
        entity: &str,
        ids: &[i64],
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        let Some((name, definition)) = self.config.entity(entity) else {
            return Ok(Vec::new());
        };
        let list: Vec<String> = ids.iter().map(i64::to_string).collect();
        let order = match &definition.newest {
            Some(field) => format!("ORDER BY {field} DESC NULLS LAST, id"),
            None => "ORDER BY id".to_string(),
        };
        let batches = client.query_arrow(&format!(
            "SELECT id, source, key, CAST({label} AS VARCHAR) AS label FROM {table} WHERE id IN ({list}) {order} LIMIT {limit}",
            label = definition.label,
            table = schema::table(name),
            list = list.join(", ")
        ))?;
        let mut found: Vec<(String, String)> = duckdb_client::string_column(&batches, "source")
            .into_iter()
            .zip(duckdb_client::string_column(&batches, "key"))
            .zip(optional_strings(&batches, "label"))
            .map(|((source, key), label)| {
                (
                    self.reference(&source, name, &key),
                    truncate(&one_line(&label.unwrap_or_default()), 80),
                )
            })
            .collect();
        let missing = ids.len().min(limit).saturating_sub(found.len());
        found.extend(
            std::iter::repeat_with(|| (format!("{}:?", definition.kind), "(not synced)".into()))
                .take(missing),
        );
        Ok(found)
    }

    pub fn labels_in(&self, query: &str) -> (Vec<String>, Vec<String>) {
        static LABEL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
            regex::Regex::new(r"\(\s*\w*\s*:\s*([A-Za-z]+)").expect("valid pattern")
        });
        let (mut ours, mut theirs) = (Vec::new(), Vec::new());
        for found in LABEL.captures_iter(query) {
            let label = found[1].to_string();
            match self.config.entity(&label) {
                Some((name, _)) => ours.push(name.to_string()),
                None => theirs.push(label),
            }
        }
        (ours, theirs)
    }

    pub fn query(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let client = self
            .reader()?
            .context("no third-party data yet; no source has synced")?;
        let compiled = compiler::compile_local(query, compiler::Frontend::Gql, &self.ontology)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let sql = compiled.base.render();
        let batches = client
            .query_arrow(&sql)
            .with_context(|| format!("running {sql}"))?;
        Ok(returned(query, batches))
    }

    pub fn schema_of(&self, only: &[String]) -> String {
        let mut out = String::new();
        for (source, name, entity) in self.config.entities() {
            if entity.fetch.is_none()
                || (!only.is_empty()
                    && !only.iter().any(|wanted| wanted.eq_ignore_ascii_case(name)))
            {
                continue;
            }
            out.push_str(&format!(
                "{name}  ({}:{})  {}\n",
                source.name, entity.kind, entity.description
            ));
            let mut properties = vec![
                "id: int64".to_string(),
                "key: string".into(),
                "source: string".into(),
            ];
            properties.extend(
                entity
                    .properties
                    .iter()
                    .map(|(field, property)| format!("{field}: {}", property.kind.ontology())),
            );
            out.push_str(&format!("  properties: {}\n", properties.join(", ")));
            for link in &entity.links {
                out.push_str(&format!(
                    "  -[{}]-> {}  (via {})\n",
                    link.kind, link.to, link.via
                ));
            }
        }
        out
    }
}

fn signature(source: &config::Source) -> String {
    let mut text = format!("{:?}|{:?}", source.base_url, source.auth.as_ref());
    for (name, entity) in &source.entities {
        text.push_str(&format!("|{name}={entity:?}"));
    }
    store::id(&source.name, "signature", &text).to_string()
}

fn returned(query: &str, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    static RETURN: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?is)\bRETURN\b(.*?)(\bORDER\s+BY\b|\bSKIP\b|\bLIMIT\b|$)")
            .expect("valid pattern")
    });
    static ITEM: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)^(?:.*\bAS\s+(\w+)|(\w+)\.(\w+)|(\w+))$").expect("valid pattern")
    });
    let Some(first) = batches.first() else {
        return batches;
    };
    let Some(items) = RETURN.captures(query).map(|found| found[1].to_string()) else {
        return batches;
    };
    let mut wanted = Vec::new();
    for item in items.split(',').map(str::trim) {
        let Some(found) = ITEM.captures(item) else {
            continue;
        };
        if let Some(alias) = found.get(1) {
            wanted.push(alias.as_str().to_string());
        } else if let (Some(node), Some(field)) = (found.get(2), found.get(3)) {
            wanted.push(format!("{}_{}", node.as_str(), field.as_str()));
        } else if let Some(node) = found.get(4) {
            let prefix = format!("{}_", node.as_str());
            wanted.extend(
                first
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .filter(|name| name.starts_with(&prefix)),
            );
        }
    }
    let schema = first.schema();
    let indices: Vec<usize> = wanted
        .iter()
        .filter_map(|name| schema.index_of(name).ok())
        .collect();
    if indices.is_empty() {
        return batches;
    }
    batches
        .into_iter()
        .filter_map(|batch| batch.project(&indices).ok())
        .collect()
}

fn loose(word: &str) -> String {
    let plain = !word.contains(['\\', '[', '(', '*', '+', '?', '{', '^', '$', '|', '.']);
    match plain {
        true => word
            .split(['_', '-'])
            .filter(|part| !part.is_empty())
            .map(regex::escape)
            .collect::<Vec<_>>()
            .join("[-_ ]?"),
        false => word.to_string(),
    }
}

fn optional_strings(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    use arrow::array::{Array, StringArray};
    batches
        .iter()
        .filter_map(|batch| batch.column_by_name(name))
        .filter_map(|column| column.as_any().downcast_ref::<StringArray>())
        .flat_map(|array| {
            (0..array.len()).map(move |i| (!array.is_null(i)).then(|| array.value(i).to_string()))
        })
        .collect()
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate(text: &str, max: usize) -> String {
    match text.chars().count() > max {
        true => format!("{}…", text.chars().take(max).collect::<String>()),
        false => text.to_string(),
    }
}

fn short_time(value: &str) -> String {
    let is_time = value.len() >= 16
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[10] == b' '
        && value.as_bytes()[13] == b':';
    match is_time {
        true => value[..16].to_string(),
        false => value.to_string(),
    }
}

pub fn render(found: &[Found]) -> String {
    let mut out = String::new();
    let mut current = None;
    for entity in found.iter().filter(|entity| !entity.hits.is_empty()) {
        if current != Some(&entity.source) {
            if current.is_some() {
                out.push('\n');
            }
            out.push_str(&format!("» {}\n", entity.source));
            current = Some(&entity.source);
        }
        for hit in &entity.hits {
            let key = hit.reference.splitn(3, ':').nth(2).unwrap_or_default();
            let named = !hit.label.is_empty() && hit.label != key;
            match (hit.shown.is_empty(), named) {
                (true, true) => out.push_str(&format!("{}  {}", hit.reference, hit.label)),
                (true, false) => out.push_str(&hit.reference),
                (false, true) => out.push_str(&format!(
                    "{}  {}\n    {}",
                    hit.reference,
                    hit.shown.join(" · "),
                    hit.label
                )),
                (false, false) => {
                    out.push_str(&format!("{}  {}", hit.reference, hit.shown.join(" · ")))
                }
            }
            for (field, text) in &hit.matched {
                out.push_str(&format!("\n    {field}: {text}"));
            }
            out.push('\n');
        }
        if entity.total > entity.hits.len() {
            let kind = entity
                .hits
                .first()
                .and_then(|hit| hit.reference.split(':').nth(1))
                .unwrap_or(entity.entity.as_str());
            out.push_str(&format!(
                "  {} of {} {kind}s shown; add words to narrow it\n",
                entity.hits.len(),
                entity.total,
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests;
