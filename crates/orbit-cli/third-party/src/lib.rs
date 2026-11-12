pub mod config;
pub mod fetch;
pub mod schema;

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, OnceLock};

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use compiler::input::Input;
use ontology::Ontology;
use regex::Regex;

use config::{Entity, Source};
use fetch::{Fetcher, Record, Want};

pub use async_trait::async_trait;
pub use compiler::input::{Direction, FilterOp, InputFilter, InputNode};
pub use config::Config;

const GREP_LIMIT: usize = 20;
const CONTEXT_LINKS: usize = 10;

pub struct ThirdParty {
    pub config: Config,
    ontology: OnceLock<Arc<Ontology>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Scope {
    pub picks: Vec<(String, String)>,
    pub within: Vec<Target>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hit {
    pub reference: String,
    pub label: String,
    pub shown: Vec<String>,
    pub matched: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub source: String,
    pub kind: String,
    pub hits: Vec<Hit>,
    pub more: bool,
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

type Within = Vec<(String, String)>;

#[async_trait::async_trait]
pub trait GitLab: Send + Sync {
    async fn fetch(&self, part: &Part) -> Result<Fetched>;
}

pub struct Part {
    pub nodes: Vec<compiler::input::InputNode>,
    pub relationships: Vec<compiler::input::InputRelationship>,
    pub anchor: Option<(String, Vec<String>)>,
}

#[derive(Default)]
pub struct Fetched {
    pub rows: Vec<(String, String, schema::Row)>,
    pub edges: Vec<schema::Edge>,
}

pub fn mentions(text: &str) -> Vec<(&'static str, String, i64)> {
    static URL: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"https://gitlab\.com/([\w.-]+(?:/[\w.-]+)+)/-/(merge_requests|issues|work_items)/(\d+)").expect("valid")
    });
    URL.captures_iter(text)
        .filter_map(|found| {
            let kind = match &found[2] {
                "merge_requests" => "MergeRequest",
                _ => "WorkItem",
            };
            Some((kind, found[1].to_string(), found[3].parse().ok()?))
        })
        .collect()
}

impl ThirdParty {
    pub fn from_env() -> Option<Self> {
        Self::new(Config::from_env(|name| std::env::var(name).ok()))
    }

    pub fn new(config: Config) -> Option<Self> {
        (!config.sources.is_empty()).then(|| Self {
            config,
            ontology: OnceLock::new(),
        })
    }

    fn ontology(&self) -> Result<Arc<Ontology>> {
        if let Some(ontology) = self.ontology.get() {
            return Ok(Arc::clone(ontology));
        }
        let built = schema::build()?;
        Ok(Arc::clone(self.ontology.get_or_init(|| built)))
    }

    pub fn scope(&self, query: &str) -> (Option<Scope>, String) {
        let mut picks = Vec::new();
        let mut rest = Vec::new();
        for word in query.split_whitespace() {
            let picked = word.split_once(':').and_then(|(name, value)| {
                let source = self.config.source(name)?;
                let (kind, value) = value.split_once(':')?;
                Some((source.name.clone(), self.config.kind(source, kind)?, value))
            });
            match picked {
                Some((source, entity, value)) => {
                    picks.push((source, entity));
                    rest.extend((!value.is_empty()).then(|| value.to_string()));
                }
                None => rest.push(word.to_string()),
            }
        }
        let scope = (!picks.is_empty()).then(|| Scope {
            picks,
            within: Vec::new(),
        });
        (scope, rest.join(" "))
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

    async fn resolve_key(&self, fetcher: &Fetcher, target: &Target) -> Result<String> {
        let (source, name, entity) = self
            .config
            .entity(&target.entity)
            .context("no such entity")?;
        let bare = target.key.trim_start_matches(['#', '@']);
        if entity.list.is_none() {
            return Ok(target.key.clone());
        }
        if entity.get.is_some()
            && looks_like_key(bare)
            && let Ok(Some(record)) = fetcher.get_one(source, name, entity, bare).await
        {
            return Ok(record.key);
        }
        let all = fetcher.list(source, name, entity).await?;
        let mut named = all.iter().filter(|record| {
            record.key == bare || value(record, &entity.label).eq_ignore_ascii_case(bare)
        });
        let at = reference(&source.name, &entity.kind, &target.key);
        match (named.next(), named.next()) {
            (Some(record), None) => Ok(record.key.clone()),
            (Some(_), Some(_)) => bail!("several {}s are named {bare}; use its key", entity.kind),
            (None, _) => bail!("{} has nothing at {at}", source.name),
        }
    }

    pub async fn grep(&self, query: &str, scope: &Scope) -> Grepped {
        match self.try_grep(query, scope).await {
            Ok(grepped) => grepped,
            Err((source, error)) => Grepped {
                found: Vec::new(),
                failed: vec![(source, format!("{error:#}"))],
            },
        }
    }

    async fn try_grep(
        &self,
        query: &str,
        scope: &Scope,
    ) -> Result<Grepped, (String, anyhow::Error)> {
        let fetcher = Fetcher::new().map_err(|error| ("third-party".to_string(), error))?;
        let query = match query.trim() {
            "." => "",
            query => query,
        };
        let sources: std::collections::BTreeSet<&str> = scope
            .within
            .iter()
            .map(|target| target.source.as_str())
            .collect();
        let mut within: Within = Vec::new();
        for target in &scope.within {
            within.push(
                match self.searches_by_name(target) && !looks_like_key(&target.key) {
                    true => (
                        format!("{}#name", target.entity),
                        target.key.trim_start_matches(['#', '@']).into(),
                    ),
                    false => (
                        target.entity.clone(),
                        self.resolve_key(&fetcher, target)
                            .await
                            .map_err(|error| (target.source.clone(), error))?,
                    ),
                },
            );
        }
        let wanted: Vec<(&Source, &str, &Entity)> = self
            .config
            .entities()
            .filter(|(source, name, entity)| {
                (scope.picks.is_empty()
                    || scope
                        .picks
                        .contains(&(source.name.clone(), name.to_string())))
                    && (within.is_empty() || sources.contains(source.name.as_str()))
                    && within.iter().all(|(to, _)| can_narrow(name, entity, to))
            })
            .collect();
        if sources.len() > 1 || (!within.is_empty() && wanted.is_empty()) {
            let source = sources
                .iter()
                .next()
                .copied()
                .unwrap_or_default()
                .to_string();
            let error = anyhow::anyhow!(
                "nothing asked for can be narrowed by those references, which must all come from one source"
            );
            return Err((source, error));
        }
        let fetcher = &fetcher;
        let searches = wanted.iter().map(|(source, name, entity)| async {
            let found = self
                .grep_entity(fetcher, source, name, entity, query, &within)
                .await;
            (source.name.clone(), *name, found)
        });
        let mut grepped = Grepped::default();
        for (source, name, found) in futures::future::join_all(searches).await {
            match found {
                Ok(found) => grepped.found.push(found),
                Err(error) => grepped.failed.push((source, format!("{name}: {error:#}"))),
            }
        }
        Ok(grepped)
    }

    fn searches_by_name(&self, target: &Target) -> bool {
        let Some(source) = self.config.source(&target.source) else {
            return false;
        };
        let mut users = source
            .entities
            .values()
            .filter(|entity| entity.links.iter().any(|link| link.to == target.entity))
            .peekable();
        users.peek().is_some()
            && users.all(|entity| {
                entity
                    .search_api
                    .as_ref()
                    .is_some_and(|search| search.within_name.contains_key(&target.entity))
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
        let matchers = matchers(query)?;
        let mut records = match &entity.search_api {
            Some(_) => {
                let want = Want::First(GREP_LIMIT + 1);
                fetcher
                    .search(source, name, entity, query, within, want)
                    .await?
            }
            None => fetcher
                .list(source, name, entity)
                .await?
                .into_iter()
                .filter(|record| {
                    within.iter().all(|(to, key)| {
                        (to == name && &record.key == key) || links_to(entity, record, to, key)
                    }) && matchers.iter().all(|matcher| {
                        entity
                            .search
                            .iter()
                            .any(|field| matcher.is_match(&value(record, field)))
                    })
                })
                .collect(),
        };
        if let Some(field) = &entity.newest {
            records.sort_by_key(|record| std::cmp::Reverse(value(record, field)));
        }
        Ok(Found {
            source: source.name.clone(),
            kind: entity.kind.clone(),
            more: records.len() > GREP_LIMIT,
            hits: records
                .iter()
                .take(GREP_LIMIT)
                .map(|record| hit(&source.name, entity, record, &matchers))
                .collect(),
        })
    }

    pub async fn context(&self, target: &Target) -> Result<String> {
        let fetcher = Fetcher::new()?;
        let (source, name, entity) = self
            .config
            .entity(&target.entity)
            .context("no such entity")?;
        let key = self.resolve_key(&fetcher, target).await?;
        let shown = reference(&source.name, &entity.kind, &key);
        let record = fetcher
            .get_one(source, name, entity, &key)
            .await?
            .with_context(|| format!("{} has nothing at {shown}", source.name))?;
        let mut out = format!("{shown}\n");
        for (field, property) in &record.properties {
            let text = fetch::text(property);
            if !text.is_empty() {
                out.push_str(&format!("{field}: {}\n", text.trim_end()));
            }
        }
        let mut links = Vec::new();
        for link in &entity.links {
            if let Some(other) = record.properties.get(&link.via).map(fetch::text)
                && let Some((_, _, to)) = self.config.entity(&link.to)
            {
                links.push(format!(
                    "-[{}]-> {}",
                    link.kind,
                    reference(&source.name, &to.kind, &other)
                ));
            }
        }
        for (other_name, other) in &source.entities {
            let Some(link) = other.links.iter().find(|link| link.to == name) else {
                continue;
            };
            if other.search_api.is_some() && !can_narrow(other_name, other, name) {
                continue;
            }
            let within = [(name.to_string(), key.clone())];
            let found = self
                .grep_entity(&fetcher, source, other_name, other, "", &within)
                .await;
            let found =
                found.map_err(|error| eprintln!("orbit: {}: {other_name}: {error:#}", source.name));
            let Some(found) = found.ok().filter(|found| !found.hits.is_empty()) else {
                continue;
            };
            let hits = found.hits.iter().take(CONTEXT_LINKS);
            links.push(format!(
                "<-[{}]- {} (latest {})",
                link.kind,
                other.kind,
                hits.len()
            ));
            links.extend(
                hits.map(|hit| format!("  {}  {}", hit.reference, truncate(&hit.label, 80))),
            );
        }
        if !links.is_empty() {
            out.push_str(&format!("\nlinks:\n{}\n", links.join("\n")));
        }
        Ok(out)
    }

    pub fn labels_in(&self, query: &str) -> (Vec<String>, Vec<String>) {
        static LABEL: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\(\s*\w*\s*:\s*([A-Za-z]+)").expect("valid pattern"));
        let (mut ours, mut theirs) = (Vec::new(), Vec::new());
        for found in LABEL.captures_iter(query) {
            match self.config.entity(&found[1]) {
                Some((_, name, _)) => ours.push(name.to_string()),
                None => theirs.push(found[1].to_string()),
            }
        }
        (ours, theirs)
    }

    pub async fn query(
        &self,
        query: &str,
        gitlab: Option<&dyn GitLab>,
    ) -> Result<Vec<RecordBatch>> {
        let ontology = self.ontology()?;
        let compiled = compiler::compile_local(query, compiler::Frontend::Gql, &ontology)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let fetcher = Fetcher::new()?;
        let mut rows: BTreeMap<String, Vec<schema::Row>> = BTreeMap::new();
        let mut edges = Vec::new();
        let mut mentioned: BTreeMap<&'static str, Vec<(String, i64)>> = BTreeMap::new();
        let mut theirs: Vec<&compiler::input::InputNode> = Vec::new();
        for node in &compiled.input.nodes {
            let Some(label) = node.entity.as_deref() else {
                continue;
            };
            let Some((source, name, entity)) = self.config.entity(label) else {
                theirs.push(node);
                continue;
            };
            let records = match (&entity.search_api, rows.contains_key(name)) {
                (Some(_), true) => bail!(
                    "{name} is read through a live search, so a query can match it only once; run one query per search"
                ),
                (Some(_), false) => {
                    let (text, within) = self.search_terms(&compiled.input, node, entity)?;
                    fetcher
                        .search(source, name, entity, &text, &within, Want::All)
                        .await?
                }
                (None, true) => continue,
                (None, false) => fetcher.list(source, name, entity).await?,
            };
            let (found, links) = schema::rows(&source.name, name, entity, &records);
            for (record, row) in records.iter().zip(&found) {
                for (kind, path, iid) in mentions(&value(record, &entity.label)) {
                    mentioned
                        .entry(kind)
                        .or_default()
                        .push((format!("{path}#{iid}"), row.id));
                }
            }
            rows.insert(name.to_string(), found);
            edges.extend(links);
        }
        if !theirs.is_empty() {
            let gitlab =
                gitlab.context("this query names GitLab entities, so it needs a GitLab sign-in")?;
            let ids: Vec<&str> = theirs.iter().map(|node| node.id.as_str()).collect();
            let relationships = compiled
                .input
                .relationships
                .iter()
                .filter(|edge| ids.contains(&edge.from.as_str()) && ids.contains(&edge.to.as_str()))
                .cloned()
                .collect();
            let anchor = compiled.input.relationships.iter().find_map(|edge| {
                let (ours, gitlab) = match (
                    ids.contains(&edge.from.as_str()),
                    ids.contains(&edge.to.as_str()),
                ) {
                    (false, true) => (&edge.from, &edge.to),
                    (true, false) => (&edge.to, &edge.from),
                    _ => return None,
                };
                edge.types
                    .iter()
                    .any(|kind| kind == "MENTIONS")
                    .then_some(())?;
                let label = theirs
                    .iter()
                    .find(|node| &node.id == gitlab)?
                    .entity
                    .clone()?;
                let _ = ours;
                Some((
                    gitlab.clone(),
                    mentioned
                        .get(label.as_str())
                        .map(|found| found.iter().map(|(at, _)| at.clone()).collect())
                        .unwrap_or_default(),
                ))
            });
            let part = Part {
                nodes: theirs.iter().map(|node| (*node).clone()).collect(),
                relationships,
                anchor,
            };
            let fetched = gitlab.fetch(&part).await?;
            for (label, at, row) in &fetched.rows {
                for (wanted, message) in mentioned.get(label.as_str()).into_iter().flatten() {
                    if wanted == at {
                        edges.push(schema::Edge {
                            from: (*message, "SlackMessage".into()),
                            kind: "MENTIONS".into(),
                            to: (row.id, label.clone()),
                        });
                    }
                }
            }
            for (label, _, row) in fetched.rows {
                let rows = rows.entry(label).or_default();
                if !rows.iter().any(|seen| seen.id == row.id) {
                    rows.push(row);
                }
            }
            edges.extend(fetched.edges);
        }
        let client = duckdb_client::DuckDbClient::open(duckdb_client::OpenMode::InMemoryArrow)?;
        let ddl = compiler::generate_local_ddl(&ontology, "").replace(" NOT NULL", "");
        client.initialize_schema(&ddl)?;
        for (name, rows) in &rows {
            schema::load(&client, &ontology, name, rows)?;
        }
        schema::link(&client, &edges)?;
        let sql = compiled.base.render();
        let batches = client
            .query_arrow(&sql)
            .with_context(|| format!("running {sql}"))?;
        returned(query, limited(batches, compiled.input.limit as usize))
    }

    fn search_terms(
        &self,
        input: &Input,
        node: &InputNode,
        entity: &Entity,
    ) -> Result<(String, Within)> {
        let label = node.entity.as_deref().unwrap_or_default();
        let live = format!("{label} is read through a live search, which");
        if !node.negated_filters.is_empty() {
            bail!("{live} cannot apply NOT filters");
        }
        let term = |filter: &InputFilter, contains: bool| -> Result<String> {
            let op = filter.op.unwrap_or(FilterOp::Eq);
            match (
                op == FilterOp::Eq || (contains && op == FilterOp::Contains),
                filter.value_str(),
            ) {
                (true, Some(text)) => Ok(text.to_string()),
                _ => bail!(
                    "{live} can only match = or CONTAINS on its text and = on what it links to; this filter uses {}",
                    op.as_ref()
                ),
            }
        };
        let mut words = Vec::new();
        for (field, filters) in &node.filters {
            if entity.search.contains(field) {
                words.extend(
                    filters
                        .iter()
                        .map(|filter| term(filter, true))
                        .collect::<Result<Vec<_>>>()?,
                );
            }
        }
        let mut within = Vec::new();
        let linked = input.relationships.iter().filter_map(|edge| {
            let other = [(&edge.from, &edge.to), (&edge.to, &edge.from)]
                .into_iter()
                .find_map(|(this, other)| (this == &node.id).then_some(other))?;
            let other = input.nodes.iter().find(|found| &found.id == other)?;
            Some((other, self.config.entity(other.entity.as_deref()?)?))
        });
        for (other, (_, other_name, other_entity)) in linked {
            for (field, filters) in &other.filters {
                let to = match field.as_str() {
                    "key" => other_name.to_string(),
                    field if field == other_entity.label => format!("{other_name}#name"),
                    _ => continue,
                };
                if can_narrow(label, entity, &to) {
                    for filter in filters {
                        within.push((to.clone(), term(filter, false)?));
                    }
                }
            }
        }
        if words.is_empty() && within.is_empty() {
            bail!(
                "{live} needs a text filter on it, such as `CONTAINS 'deploy'`, or a match on a channel or user"
            );
        }
        Ok((words.join(" "), within))
    }
}

fn can_narrow(name: &str, entity: &Entity, to: &str) -> bool {
    let (target, by_name) = to
        .strip_suffix("#name")
        .map_or((to, false), |target| (target, true));
    match (&entity.search_api, by_name) {
        (Some(search), true) => search.within_name.contains_key(target),
        (Some(search), false) => search.within.contains_key(target),
        (None, true) => false,
        (None, false) => target == name || entity.links.iter().any(|link| link.to == target),
    }
}

fn links_to(entity: &Entity, record: &Record, to: &str, key: &str) -> bool {
    entity
        .links
        .iter()
        .any(|link| link.to == to && value(record, &link.via) == key)
}

fn reference(source: &str, kind: &str, key: &str) -> String {
    format!("{source}:{kind}:{key}")
}

fn hit(source: &str, entity: &Entity, record: &Record, matchers: &[Regex]) -> Hit {
    let shown: Vec<String> = entity
        .shows
        .iter()
        .map(|field| short_time(&value(record, field)))
        .filter(|value| !value.is_empty())
        .collect();
    let matched = entity
        .search
        .iter()
        .filter(|field| **field != entity.label && !entity.shows.contains(field))
        .filter_map(|field| {
            let text = value(record, field);
            (!shown.contains(&text) && matchers.iter().any(|m| m.is_match(&text)))
                .then(|| (field.clone(), one_line(&text)))
        })
        .collect();
    Hit {
        reference: reference(source, &entity.kind, &record.key),
        label: one_line(&value(record, &entity.label)),
        shown,
        matched,
    }
}

fn limited(batches: Vec<RecordBatch>, mut left: usize) -> Vec<RecordBatch> {
    let mut kept = Vec::new();
    for batch in batches {
        let take = batch.num_rows().min(left);
        if take == 0 {
            break;
        }
        left -= take;
        kept.push(batch.slice(0, take));
    }
    kept
}

fn looks_like_key(text: &str) -> bool {
    let text = text.trim_start_matches(['#', '@']);
    text.len() >= 2
        && text.starts_with(|c: char| c.is_ascii_uppercase())
        && text
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
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

fn matchers(query: &str) -> Result<Vec<Regex>> {
    query
        .split_whitespace()
        .map(|word| {
            let plain = !word.contains(['\\', '[', '(', '*', '+', '?', '{', '^', '$', '|', '.']);
            let pattern = match plain {
                true => word
                    .split(['_', '-'])
                    .filter(|part| !part.is_empty())
                    .map(regex::escape)
                    .collect::<Vec<_>>()
                    .join("[-_ ]?"),
                false => word.to_string(),
            };
            regex::RegexBuilder::new(&pattern)
                .case_insensitive(true)
                .build()
                .with_context(|| {
                    format!("{word:?} is not a valid pattern; escape it or use plain words")
                })
        })
        .collect()
}

fn returned(query: &str, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    let Some(first) = batches.first() else {
        return Ok(batches);
    };
    let asked = compiler::gql::parse(query).map_err(|error| anyhow::anyhow!("{error}"))?;
    let schema = first.schema();
    let mut wanted: Vec<String> = Vec::new();
    for node in &asked.nodes {
        match &node.columns {
            Some(compiler::input::ColumnSelection::List(columns)) => {
                wanted.extend(columns.iter().map(|column| format!("{}_{column}", node.id)))
            }
            Some(compiler::input::ColumnSelection::All) => wanted.extend(
                schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .filter(|name| name.starts_with(&format!("{}_", node.id))),
            ),
            None => {}
        }
    }
    wanted.extend(
        asked
            .aggregation
            .group_by
            .iter()
            .map(|key| key.output_name()),
    );
    wanted.extend(
        asked
            .aggregation
            .metrics
            .iter()
            .map(|metric| metric.output_name()),
    );
    let indices: Vec<usize> = wanted
        .iter()
        .filter_map(|name| schema.index_of(name).ok())
        .collect();
    if indices.len() != wanted.len() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|batch| Ok(batch.project(&indices)?))
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
    let bytes = value.as_bytes();
    match value.len() >= 16 && bytes[4] == b'-' && bytes[10] == b' ' && bytes[13] == b':' {
        true => value[..16].to_string(),
        false => value.to_string(),
    }
}

pub fn render(found: &[Found]) -> String {
    let mut out = String::new();
    let mut current = None;
    for entity in found.iter().filter(|entity| !entity.hits.is_empty()) {
        if current.replace(&entity.source) != Some(&entity.source) {
            out.push_str(&format!("» {}\n", entity.source));
        }
        for hit in &entity.hits {
            let key = hit.reference.splitn(3, ':').nth(2).unwrap_or_default();
            let mut line = vec![hit.reference.clone()];
            line.extend((!hit.shown.is_empty()).then(|| hit.shown.join(" · ")));
            out.push_str(&line.join("  "));
            if !hit.label.is_empty() && hit.label != key {
                let gap = if hit.shown.is_empty() { "  " } else { "\n    " };
                out.push_str(&format!("{gap}{}", hit.label));
            }
            for (field, text) in &hit.matched {
                out.push_str(&format!("\n    {field}: {text}"));
            }
            out.push('\n');
        }
        if entity.more {
            let shown = entity.hits.len();
            let kind = &entity.kind;
            out.push_str(&format!(
                "  {shown} of more than {GREP_LIMIT} {kind}s shown; add words to narrow it\n"
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests;
