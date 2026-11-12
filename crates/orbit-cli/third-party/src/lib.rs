pub mod auth;
pub mod config;
pub mod fetch;
pub mod schema;
pub mod setup;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use ontology::Ontology;

use config::{Entity, Source};
use fetch::{Fetcher, Record};

pub use config::Config;

pub const CONFIG_FILE: &str = "third-party.yaml";
pub const CONFIG_ENV: &str = "ORBIT_THIRD_PARTY_CONFIG";

const GREP_LIMIT: usize = 20;
const CONTEXT_LINKS: usize = 10;
const HEADLINE: usize = 100;

pub struct ThirdParty {
    pub config: Config,
    pub ontology: Arc<Ontology>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Scope {
    pub picks: Vec<(String, String)>,
    pub within: Vec<Target>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hit {
    pub source: String,
    pub entity: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub source: String,
    pub entity: String,
    pub key: String,
}

#[derive(Debug, Default)]
pub struct Grepped {
    pub found: Vec<Found>,
    pub failed: Vec<(String, String)>,
}

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
        Ok(Some(Self { config, ontology }))
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
            let Some(entity) = self.config.kind(source, kind) else {
                rest.push(word.to_string());
                continue;
            };
            picks.push((source.name.clone(), entity));
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
                    within: Vec::new(),
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

    pub fn kind_of(&self, entity: &str) -> Option<String> {
        self.config
            .entity(entity)
            .map(|(_, found)| found.kind.clone())
    }

    pub fn shown(&self, target: &Target) -> String {
        let kind = self.kind_of(&target.entity).unwrap_or_default();
        format!("{}:{kind}:{}", target.source, target.key)
    }

    fn reference(&self, source: &str, entity: &str, key: &str) -> String {
        match self.config.entity(entity) {
            Some((_, definition)) => format!("{source}:{}:{key}", definition.kind),
            None => format!("{source}:{entity}:{key}"),
        }
    }

    fn source_and_entity(&self, target: &Target) -> Result<(&Source, &str, &Entity)> {
        let source = self
            .config
            .source(&target.source)
            .with_context(|| format!("no source {}", target.source))?;
        let (name, entity) = source
            .entities
            .get_key_value(&target.entity)
            .with_context(|| format!("{} is not an entity of {}", target.entity, source.name))?;
        Ok((source, name.as_str(), entity))
    }

    /// The key a target names: itself, or the one listed entity whose label matches it.
    pub async fn resolve_key(&self, fetcher: &Fetcher, target: &Target) -> Result<String> {
        let (source, name, entity) = self.source_and_entity(target)?;
        let bare = target.key.trim_start_matches(['#', '@']);
        if entity.list.is_none() {
            return Ok(target.key.clone());
        }
        let all = fetcher.list(source, name, entity).await?;
        if all.iter().any(|record| record.key == target.key) {
            return Ok(target.key.clone());
        }
        if all.iter().any(|record| record.key == bare) {
            return Ok(bare.to_string());
        }
        let label = |record: &Record| {
            record
                .properties
                .get(&entity.label)
                .map(fetch::text)
                .unwrap_or_default()
        };
        let named: Vec<&Record> = all
            .iter()
            .filter(|record| label(record).eq_ignore_ascii_case(bare))
            .collect();
        match named.as_slice() {
            [] => bail!("{} has nothing at {}", source.name, self.shown(target)),
            [record] => Ok(record.key.clone()),
            several => bail!(
                "{} {}s are named {bare}; pick one: {}",
                several.len(),
                entity.kind,
                several
                    .iter()
                    .map(|record| self.reference(&source.name, name, &record.key))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }

    pub async fn grep(&self, patterns: &[String], scope: &Scope) -> Grepped {
        let mut grepped = Grepped::default();
        let fetcher = match Fetcher::new() {
            Ok(fetcher) => fetcher,
            Err(error) => {
                grepped
                    .failed
                    .push(("third-party".into(), format!("{error:#}")));
                return grepped;
            }
        };
        let query = patterns
            .iter()
            .map(|pattern| pattern.trim())
            .filter(|pattern| !pattern.is_empty() && *pattern != ".")
            .collect::<Vec<_>>()
            .join(" ");
        let mut within: Vec<(String, String, String)> = Vec::new();
        for target in &scope.within {
            match self.resolve_key(&fetcher, target).await {
                Ok(key) => within.push((target.source.clone(), target.entity.clone(), key)),
                Err(_) => within.push((
                    target.source.clone(),
                    format!("{}#name", target.entity),
                    target.key.trim_start_matches(['#', '@']).to_string(),
                )),
            }
        }
        let wanted: Vec<(&Source, &str, &Entity)> = self
            .config
            .entities()
            .filter(|(source, name, _)| {
                scope.picks.is_empty()
                    || scope
                        .picks
                        .iter()
                        .any(|(picked, entity)| *picked == source.name && entity == name)
            })
            .filter(|(source, name, entity)| {
                scope.within.is_empty() || self.narrows(source, name, entity, &scope.within)
            })
            .collect();
        let searches = wanted.iter().map(|(source, name, entity)| {
            let within: Vec<(String, String)> = within
                .iter()
                .filter(|(found, _, _)| *found == source.name)
                .map(|(_, to, key)| (to.clone(), key.clone()))
                .collect();
            let fetcher = &fetcher;
            let query = query.clone();
            async move {
                let found = self
                    .grep_entity(fetcher, source, name, entity, &query, &within)
                    .await;
                (*source, *name, found)
            }
        });
        for (source, name, found) in futures::future::join_all(searches).await {
            match found {
                Ok(found) => grepped.found.push(found),
                Err(error) => grepped
                    .failed
                    .push((source.name.clone(), format!("{name}: {error:#}"))),
            }
        }
        grepped
    }

    fn narrows(&self, source: &Source, name: &str, entity: &Entity, within: &[Target]) -> bool {
        within.iter().any(|target| {
            target.source == source.name
                && (target.entity == name
                    || entity.links.iter().any(|link| link.to == target.entity))
        })
    }

    async fn grep_entity(
        &self,
        fetcher: &Fetcher,
        source: &Source,
        name: &str,
        entity: &Entity,
        query: &str,
        within: &[(String, String)],
    ) -> Result<Found> {
        let records = match &entity.search_api {
            Some(_) => fetcher.search(source, name, entity, query, within).await?,
            None => {
                let all = fetcher.list(source, name, entity).await?;
                let matchers = matchers(query);
                all.into_iter()
                    .filter(|record| {
                        within.iter().all(|(to, key)| {
                            to.ends_with("#name")
                                || self.links_to(entity, record, to, key)
                                || (to == name && &record.key == key)
                        })
                    })
                    .filter(|record| {
                        matchers.iter().all(|matcher| {
                            searched(entity)
                                .iter()
                                .any(|field| matcher.is_match(&value(record, field)))
                        })
                    })
                    .collect()
            }
        };
        let mut records = records;
        if let Some(field) = &entity.newest {
            records.sort_by_key(|record| std::cmp::Reverse(value(record, field)));
        }
        let total = records.len();
        let matchers = matchers(query);
        let hits = records
            .iter()
            .take(GREP_LIMIT)
            .map(|record| self.hit(&source.name, name, entity, record, &matchers))
            .collect();
        Ok(Found {
            source: source.name.clone(),
            entity: name.to_string(),
            hits,
            total,
        })
    }

    fn links_to(&self, entity: &Entity, record: &Record, to: &str, key: &str) -> bool {
        entity
            .links
            .iter()
            .filter(|link| link.to == to)
            .any(|link| value(record, &link.via) == key)
    }

    fn hit(
        &self,
        source: &str,
        name: &str,
        entity: &Entity,
        record: &Record,
        matchers: &[regex::Regex],
    ) -> Hit {
        let shown: Vec<String> = entity
            .shows
            .iter()
            .map(|field| value(record, field))
            .filter(|value| !value.is_empty())
            .map(|value| short_time(&value))
            .collect();
        let matched = searched(entity)
            .into_iter()
            .skip(1)
            .filter(|field| !entity.shows.contains(field))
            .filter_map(|field| {
                let text = value(record, &field);
                (!shown.contains(&text) && matchers.iter().any(|m| m.is_match(&text)))
                    .then(|| (field, one_line(&text)))
            })
            .collect();
        Hit {
            source: source.to_string(),
            entity: name.to_string(),
            reference: self.reference(source, name, &record.key),
            label: one_line(&value(record, &entity.label)),
            shown,
            matched,
        }
    }

    pub async fn context(&self, target: &Target) -> Result<String> {
        let fetcher = Fetcher::new()?;
        let (source, name, entity) = self.source_and_entity(target)?;
        let key = self.resolve_key(&fetcher, target).await?;
        let shown = self.reference(&source.name, name, &key);
        let record = fetcher
            .get_one(source, name, entity, &key)
            .await?
            .with_context(|| format!("{} has nothing at {shown}", source.name))?;
        let hit = self.hit(&source.name, name, entity, &record, &[]);
        let mut out = match hit.label.chars().count() > HEADLINE {
            true => format!("{shown}\n"),
            false => format!("{shown}  {}\n", hit.label),
        };
        for (field, property) in &record.properties {
            let text = fetch::text(property);
            if !text.is_empty() && (field != &entity.label || hit.label.chars().count() > HEADLINE)
            {
                out.push_str(&format!("{field}: {}\n", text.trim_end()));
            }
        }
        let mut links = Vec::new();
        for link in &entity.links {
            let Some(other) = record.properties.get(&link.via).map(fetch::text) else {
                continue;
            };
            if let Some((_, other_entity)) = self.config.entity(&link.to) {
                links.push(format!(
                    "-[{}]-> {}:{}:{other}",
                    link.kind, source.name, other_entity.kind
                ));
            }
        }
        for (other_name, other) in &source.entities {
            let Some(link) = other.links.iter().find(|link| link.to == name) else {
                continue;
            };
            let within = [(name.to_string(), key.clone())];
            let records = match &other.search_api {
                Some(search) if search.within.contains_key(name) => fetcher
                    .search(source, other_name, other, "", &within)
                    .await
                    .unwrap_or_default(),
                Some(_) => continue,
                None => fetcher
                    .list(source, other_name, other)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|record| value(record, &link.via) == key)
                    .collect(),
            };
            if records.is_empty() {
                continue;
            }
            links.push(format!(
                "<-[{}]- {} (latest {})",
                link.kind,
                other.kind,
                records.len().min(CONTEXT_LINKS)
            ));
            for record in records.iter().take(CONTEXT_LINKS) {
                let label = truncate(&one_line(&value(record, &other.label)), 80);
                links.push(format!(
                    "  {}  {label}",
                    self.reference(&source.name, other_name, &record.key)
                ));
            }
        }
        if !links.is_empty() {
            out.push_str(&format!("\nlinks:\n{}\n", links.join("\n")));
        }
        Ok(out)
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

    /// Runs Cypher over the third-party entities a query names, fetched live into an
    /// in-memory graph that is gone when the query returns.
    pub async fn query(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let compiled = compiler::compile_local(query, compiler::Frontend::Gql, &self.ontology)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let fetcher = Fetcher::new()?;
        let mut loaded: BTreeMap<String, Vec<Record>> = BTreeMap::new();
        for node in &compiled.input.nodes {
            let Some(label) = &node.entity else { continue };
            let Some((source, name, entity)) = self.config.entity_in(label) else {
                continue;
            };
            if loaded.contains_key(name) {
                continue;
            }
            let records = match (&entity.search_api, &entity.list) {
                (Some(_), _) => {
                    let (text, within) = self.search_terms(&compiled.input, node, entity)?;
                    fetcher.search(source, name, entity, &text, &within).await?
                }
                (None, Some(_)) => fetcher.list(source, name, entity).await?,
                (None, None) => bail!("{name} cannot be read"),
            };
            loaded.insert(name.to_string(), records);
        }
        let client = duckdb_client::DuckDbClient::open(duckdb_client::OpenMode::InMemoryArrow)?;
        client.initialize_schema(&compiler::generate_local_ddl(&self.ontology, ""))?;
        for (name, records) in &loaded {
            let (source, _, entity) = self
                .config
                .entity_in(name)
                .expect("loaded entities are configured");
            schema::load(&client, &source.name, name, entity, records)?;
        }
        let sql = compiled.base.render();
        let batches = client
            .query_arrow(&sql)
            .with_context(|| format!("running {sql}"))?;
        Ok(returned(query, batches))
    }

    /// The search text and reference filters a query gives a searched entity, such as
    /// `m.text CONTAINS 'deploy'` and a matched channel or user.
    fn search_terms(
        &self,
        input: &compiler::input::Input,
        node: &compiler::input::InputNode,
        entity: &Entity,
    ) -> Result<(String, Vec<(String, String)>)> {
        let mut words = Vec::new();
        for (field, filters) in &node.filters {
            if !searched(entity).contains(field) {
                continue;
            }
            for filter in filters {
                if let Some(text) = filter.value_str() {
                    words.push(text.to_string());
                }
            }
        }
        let mut within = Vec::new();
        for relationship in &input.relationships {
            let other = match (relationship.from == node.id, relationship.to == node.id) {
                (true, _) => &relationship.to,
                (_, true) => &relationship.from,
                _ => continue,
            };
            let Some(other) = input.nodes.iter().find(|found| &found.id == other) else {
                continue;
            };
            let Some(label) = &other.entity else { continue };
            let Some((_, other_name, other_entity)) = self.config.entity_in(label) else {
                continue;
            };
            for (field, filters) in &other.filters {
                for filter in filters {
                    let Some(value) = filter.value_str() else {
                        continue;
                    };
                    match field.as_str() {
                        "key" => within.push((other_name.to_string(), value.to_string())),
                        field if field == other_entity.label => {
                            within.push((format!("{other_name}#name"), value.to_string()))
                        }
                        _ => {}
                    }
                }
            }
        }
        if words.is_empty() && within.is_empty() {
            bail!(
                "{} is searched live, so the query needs a text filter on it, such as `CONTAINS 'deploy'`, or a match on a channel or user",
                node.entity.as_deref().unwrap_or("this entity")
            );
        }
        Ok((words.join(" "), within))
    }

    pub fn schema_of(&self, only: &[String]) -> String {
        let mut out = String::new();
        for (source, name, entity) in self.config.entities() {
            if !only.is_empty() && !only.iter().any(|wanted| wanted.eq_ignore_ascii_case(name)) {
                continue;
            }
            let how = match entity.search_api {
                Some(_) => "searched live; query it with a text filter or a linked match",
                None => "listed live",
            };
            out.push_str(&format!(
                "{name}  ({}:{})  {}; {how}\n",
                source.name, entity.kind, entity.description
            ));
            let mut properties = vec!["key: string".to_string()];
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

fn searched(entity: &Entity) -> Vec<String> {
    let mut fields = vec![entity.label.clone()];
    fields.extend(
        entity
            .search
            .iter()
            .filter(|field| **field != entity.label)
            .cloned(),
    );
    fields
}

fn value(record: &Record, field: &str) -> String {
    match field {
        "key" => record.key.clone(),
        _ => record
            .properties
            .get(field)
            .map(fetch::text)
            .unwrap_or_default(),
    }
}

fn matchers(query: &str) -> Vec<regex::Regex> {
    query
        .split_whitespace()
        .filter_map(|word| {
            regex::RegexBuilder::new(&loose(word))
                .case_insensitive(true)
                .build()
                .ok()
        })
        .collect()
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
