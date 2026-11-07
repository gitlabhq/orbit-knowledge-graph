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

/// The sources, and optionally one entity of each, a grep named.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Scope {
    /// `(source, entity)`; no entity means every entity of the source.
    pub picks: Vec<(String, Option<String>)>,
    /// Entities a result must be or link to, as with paths in a code grep: `(source, id)`.
    pub within: Vec<(String, i64)>,
}

impl Scope {
    fn covers(&self, source: &str, entity: &str) -> bool {
        let picked = self.picks.is_empty()
            || self.picks.iter().any(|(picked, kind)| {
                picked.eq_ignore_ascii_case(source)
                    && kind.as_deref().is_none_or(|kind| kind == entity)
            });
        let within = self.within.is_empty()
            || self
                .within
                .iter()
                .any(|(within, _)| within.eq_ignore_ascii_case(source));
        picked && within
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
    /// The `source:type:key` reference context reads.
    pub reference: String,
    pub label: String,
    /// The entity's `shows` values that are set, such as channel, author, and date.
    pub shown: Vec<String>,
    pub matched: Vec<(String, String)>,
}

/// One entity's results: the hits shown and how many matched in all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub source: String,
    pub entity: String,
    pub hits: Vec<Hit>,
    pub total: usize,
}

#[derive(Debug, Default)]
pub struct Freshness {
    pub synced: Vec<(String, String, usize)>,
    pub failed: Vec<(String, String)>,
}

/// One entity named by `source:type:key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub source: String,
    pub entity: String,
    pub key: String,
}

const CONTEXT_LINKS: usize = 10;

/// A label up to this long is the headline on context's first line; a longer one, such as a
/// message, gets its own field below instead.
const HEADLINE: usize = 100;

impl ThirdParty {
    pub fn config_path(root: &Path) -> PathBuf {
        std::env::var_os(CONFIG_ENV)
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join(CONFIG_FILE))
    }

    /// `None` when no third-party config exists.
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

    /// Fetches every source that has never synced, is older than its `refresh`, or whose
    /// entities changed. A source that fails keeps its last data and is reported in the result.
    pub async fn ensure_fresh(&self) -> Result<Freshness> {
        let client = store::open(&self.store, &self.ontology)?;
        let now = store::now();
        let mut stale = Vec::new();
        for source in &self.config.sources {
            let signature = signature(source);
            let fresh = store::synced_at(&client, &source.name, &signature)?
                .is_some_and(|at| now.saturating_sub(at) < source.refresh.as_secs() as i64);
            if !fresh {
                stale.push((source, signature));
            }
        }
        let mut freshness = Freshness::default();
        if stale.is_empty() {
            return Ok(freshness);
        }
        let fetcher = fetch::Fetcher::new()?;
        for (source, signature) in stale {
            let entities = match fetcher.source(source).await {
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

    /// Splits `slack:words` and `slack:message:words` out of a grep query. The second part
    /// is a type when it names one of the source's types, and a search word otherwise.
    pub fn scope(&self, query: &str) -> (Option<Scope>, String) {
        let mut picks = Vec::new();
        let mut rest = Vec::new();
        for word in query.split_whitespace() {
            let Some((name, value)) = word.split_once(':') else {
                rest.push(word.to_string());
                continue;
            };
            let Some(source) = self.config.source(name) else {
                rest.push(word.to_string());
                continue;
            };
            let (kind, value) = value.split_once(':').unwrap_or((value, ""));
            let (entity, value) = match self.config.kind(source, kind) {
                Some(entity) => (Some(entity), value.to_string()),
                None if value.is_empty() => (None, kind.to_string()),
                None => (None, format!("{kind}:{value}")),
            };
            picks.push((source.name.clone(), entity));
            if !value.is_empty() {
                rest.push(value);
            }
        }
        let rest = rest.join(" ");
        match picks.is_empty() {
            true => (None, rest),
            false => (
                Some(Scope {
                    picks,
                    within: Vec::new(),
                }),
                rest,
            ),
        }
    }

    /// Reads `source:type:key`, such as `slack:user:U0123ABCD`; `None` when it names no
    /// configured source and type. The key keeps any further colons.
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

    /// `source:type:key` for an entity row.
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
        let Some(client) = self.reader()? else {
            return Ok(Vec::new());
        };
        let mut found = Vec::new();
        for (source, name, entity) in self.config.entities() {
            if scope.is_some_and(|scope| !scope.covers(&source.name, name)) {
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
            let mut filter = format!("t.source = ? AND ({})", alternatives.join(" OR "));
            if let Some(ids) = scope.map(|scope| scope.within(&source.name))
                && !ids.is_empty()
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
                        select.push(format!("CAST({node}.{property} AS VARCHAR) AS {alias}"));
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

    /// The short type of a configured entity, such as `message` for `SlackMessage`.
    pub fn kind_of(&self, entity: &str) -> Option<String> {
        self.config
            .entity(entity)
            .map(|(_, found)| found.kind.clone())
    }

    /// `source:type:key` as the user wrote it.
    pub fn shown(&self, target: &Target) -> String {
        let kind = self.kind_of(&target.entity).unwrap_or_default();
        format!("{}:{kind}:{}", target.source, target.key)
    }

    /// The Orbit ID of the entity `target` names, by key or name as in context.
    pub fn resolve(&self, target: &Target) -> Result<i64> {
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

    /// The key `target` names: its key as given, or else the one entity whose label matches
    /// it, ignoring case and a leading `#` or `@`, such as a channel or user name.
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

    /// `(source:type:key, label)` for up to `limit` linked entities.
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

    /// The configured entity labels a query names, as in `MATCH (m:SlackMessage)`.
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

    /// The configured entities, or only the named ones.
    pub fn schema_of(&self, only: &[String]) -> String {
        let mut out = String::new();
        for (source, name, entity) in self.config.entities() {
            if !only.is_empty() && !only.iter().any(|wanted| wanted.eq_ignore_ascii_case(name)) {
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
    let mut text = format!(
        "{:?}|{:?}",
        source.base_url,
        source.auth.as_ref().map(|a| &a.token_env)
    );
    for (name, entity) in &source.entities {
        text.push_str(&format!("|{name}={entity:?}"));
    }
    store::id(&source.name, "signature", &text).to_string()
}

/// Keeps the columns the query's RETURN names, dropping the compiler's edge and `_orbit_` columns.
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

/// orbit grep ignores case, `_`, and `-`.
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

/// `2024-04-05 19:34:38.000100` reads as `2024-04-05 19:34`; anything else is unchanged.
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
