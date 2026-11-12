use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use indexmap::IndexMap;
use serde::Deserialize;

const BUILTIN: &[(&str, &str)] = &[
    ("slack", include_str!("../definitions/slack.yaml")),
    ("grafana", include_str!("../definitions/grafana.yaml")),
];

const RESERVED_SOURCES: &[&str] = &[
    "mr", "pr", "issue", "epic", "task", "comment", "def", "symbol", "dir", "folder", "file",
    "path", "repo", "in", "branch", "pipeline", "job", "commit",
];

const SQL_KEYWORDS: &[&str] = &[
    "all", "and", "as", "by", "case", "cast", "check", "column", "create", "default", "desc",
    "distinct", "else", "end", "from", "group", "having", "in", "limit", "not", "null", "offset",
    "on", "or", "order", "select", "table", "then", "to", "union", "user", "using", "when",
    "where", "with",
];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub auth: Option<Auth>,
    #[serde(default)]
    pub setup: Option<SetupSpec>,
    #[serde(default)]
    pub entities: IndexMap<String, Entity>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupSpec {
    pub title: String,
    pub token_help: String,
    #[serde(default)]
    pub asks_url: bool,
    #[serde(default)]
    pub url_help: Option<String>,
    pub verify: Verify,
    #[serde(default)]
    pub team: Option<TeamLookup>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamLookup {
    pub when: String,
    pub path: String,
    pub items: String,
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verify {
    pub path: String,
    #[serde(default)]
    pub ok: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    pub shows: String,
}

pub use crate::auth::AuthStrategy as Auth;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entity {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub description: String,
    pub key: Vec<String>,
    pub label: String,
    #[serde(default)]
    pub search: Vec<String>,
    pub properties: IndexMap<String, Property>,
    #[serde(default)]
    pub links: Vec<Link>,
    #[serde(default)]
    pub newest: Option<String>,
    #[serde(default)]
    pub shows: Vec<String>,
    #[serde(default)]
    pub list: Option<Request>,
    #[serde(default)]
    pub search_api: Option<Search>,
    #[serde(default)]
    pub get: Option<Request>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub path: String,
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    pub items: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub cursor: Option<Cursor>,
    #[serde(default = "Request::default_max_pages")]
    pub max_pages: usize,
    #[serde(default)]
    pub key: Option<Vec<String>>,
    #[serde(default)]
    pub fields: BTreeMap<String, String>,
}

impl Request {
    fn default_max_pages() -> usize {
        10
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Search {
    pub path: String,
    pub params: BTreeMap<String, String>,
    pub items: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub within: BTreeMap<String, String>,
    #[serde(default)]
    pub within_name: BTreeMap<String, String>,
    #[serde(default)]
    pub key: Option<Vec<String>>,
    #[serde(default)]
    pub fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub param: String,
    pub next: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    String,
    Int,
    Bool,
    Timestamp,
}

impl Type {
    pub(crate) fn ontology(self) -> &'static str {
        match self {
            Type::String => "string",
            Type::Int => "int64",
            Type::Bool => "boolean",
            Type::Timestamp => "timestamp",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    #[default]
    Plain,
    Unix,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Property {
    #[serde(rename = "type", default = "Property::default_type")]
    pub kind: Type,
    pub from: String,
    #[serde(default)]
    pub format: Format,
    #[serde(default)]
    pub description: String,
}

impl Property {
    fn default_type() -> Type {
        Type::String
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    #[serde(rename = "type")]
    pub kind: String,
    pub to: String,
    pub via: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceYaml {
    name: String,
    #[serde(default)]
    definition: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    token_env: Option<String>,
    #[serde(default)]
    team: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default, rename = "refresh")]
    _refresh: Option<serde::de::IgnoredAny>,
    #[serde(default, rename = "token_command")]
    _token_command: Option<serde::de::IgnoredAny>,
    #[serde(default)]
    auth: Option<Auth>,
    #[serde(default)]
    entities: IndexMap<String, Entity>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileYaml {
    #[serde(default)]
    sources: Vec<SourceYaml>,
}

#[derive(Debug, Clone)]
pub struct Source {
    pub name: String,
    pub description: String,
    pub base_url: Option<String>,
    pub auth: Option<Auth>,
    pub team: Option<String>,
    pub entities: IndexMap<String, Entity>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub path: PathBuf,
    pub sources: Vec<Source>,
}

pub fn builtin(name: &str) -> Option<Definition> {
    let (_, text) = BUILTIN.iter().find(|(builtin, _)| *builtin == name)?;
    Some(orbit_utils::yaml::from_str(text).expect("built-in source definitions are valid"))
}

pub fn connectable() -> Vec<(&'static str, Definition)> {
    builtin_names()
        .filter_map(|name| {
            let definition = builtin(name)?;
            definition.setup.as_ref()?;
            Some((name, definition))
        })
        .collect()
}

pub fn builtin_names() -> impl Iterator<Item = &'static str> {
    BUILTIN.iter().map(|(name, _)| *name)
}

impl Config {
    pub fn load(path: &Path) -> Result<Option<Self>> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
        };
        Self::parse(path, &text).map(Some)
    }

    pub fn parse(path: &Path, text: &str) -> Result<Self> {
        let file: FileYaml = orbit_utils::yaml::from_str(text)
            .with_context(|| format!("parsing {}", path.display()))?;
        let mut sources = Vec::new();
        for source in file.sources {
            sources.push(resolve(source)?);
        }
        let config = Self {
            path: path.to_path_buf(),
            sources,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn entities(&self) -> impl Iterator<Item = (&Source, &str, &Entity)> {
        self.sources.iter().flat_map(|source| {
            source
                .entities
                .iter()
                .map(move |(name, entity)| (source, name.as_str(), entity))
        })
    }

    pub fn entity(&self, name: &str) -> Option<(&str, &Entity)> {
        self.entities()
            .find(|(_, entity, _)| entity.eq_ignore_ascii_case(name))
            .map(|(_, name, entity)| (name, entity))
    }

    pub fn entity_in(&self, name: &str) -> Option<(&Source, &str, &Entity)> {
        self.entities()
            .find(|(_, entity, _)| entity.eq_ignore_ascii_case(name))
    }

    pub fn kind(&self, source: &Source, kind: &str) -> Option<String> {
        source
            .entities
            .iter()
            .find(|(_, entity)| entity.kind == kind)
            .map(|(name, _)| name.clone())
    }

    pub fn source(&self, name: &str) -> Option<&Source> {
        self.sources
            .iter()
            .find(|source| source.name.eq_ignore_ascii_case(name))
    }

    pub fn unique_entities(&self) -> IndexMap<String, Entity> {
        let mut out = IndexMap::new();
        for (_, name, entity) in self.entities() {
            out.entry(name.to_string())
                .or_insert_with(|| entity.clone());
        }
        out
    }

    fn validate(&self) -> Result<()> {
        let mut seen: BTreeMap<&str, (&str, &Entity)> = BTreeMap::new();
        let mut names = std::collections::BTreeSet::new();
        for source in &self.sources {
            ensure!(
                names.insert(source.name.to_ascii_lowercase()),
                "source {} is configured twice",
                source.name
            );
            ensure!(
                !RESERVED_SOURCES.contains(&source.name.to_ascii_lowercase().as_str()),
                "source name {} is an orbit qualifier; pick another",
                source.name
            );
            ensure!(
                source
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
                "source name {} must be lowercase letters, digits, and -",
                source.name
            );
            let mut kinds = std::collections::BTreeSet::new();
            for (name, entity) in &source.entities {
                ensure!(
                    !entity.kind.is_empty()
                        && entity
                            .kind
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b == b'_'),
                    "{}: {name} type {:?} must be lowercase letters and _",
                    source.name,
                    entity.kind
                );
                ensure!(
                    kinds.insert(entity.kind.as_str()),
                    "{}: two entities have type {}",
                    source.name,
                    entity.kind
                );
            }
        }
        for (source, name, entity) in self.entities() {
            ensure!(
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphabetic()),
                "{}: entity name {name:?} must be letters only, such as SlackMessage",
                source.name
            );
            if let Some((other, existing)) = seen.get(name) {
                ensure!(
                    *existing == entity,
                    "entity {name} is defined differently by sources {other} and {}",
                    source.name
                );
            }
            seen.insert(name, (&source.name, entity));
            for property in entity.properties.keys() {
                ensure!(
                    !["id", "key", "source", "traversal_path"].contains(&property.as_str()),
                    "{name}.{property}: id, key, source, and traversal_path are reserved"
                );
                ensure!(
                    !SQL_KEYWORDS.contains(&property.as_str()),
                    "{name}.{property}: {property} is a SQL keyword; name it something else"
                );
                ensure!(
                    property
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                    "{name}.{property}: property names are lower_snake"
                );
            }
            let declared = |field: &str| field == "key" || entity.properties.contains_key(field);
            ensure!(
                declared(&entity.label),
                "{name}: label {} is not a property",
                entity.label
            );
            for field in entity
                .search
                .iter()
                .chain(&entity.shows)
                .chain(&entity.newest)
            {
                ensure!(declared(field), "{name}: {field} is not a property");
            }
            ensure!(
                entity.list.is_some() || entity.search_api.is_some(),
                "{name}: needs list, search_api, or both"
            );
            for request in entity.list.iter().chain(&entity.get) {
                for field in request.fields.keys() {
                    ensure!(
                        entity.properties.contains_key(field),
                        "{name}: field {field} is not a property"
                    );
                }
            }
            if let Some(search) = &entity.search_api {
                for field in search.fields.keys() {
                    ensure!(
                        entity.properties.contains_key(field),
                        "{name}: search_api field {field} is not a property"
                    );
                }
                for within in search.within.keys().chain(search.within_name.keys()) {
                    ensure!(
                        entity.links.iter().any(|link| &link.to == within),
                        "{name}: search_api within {within} names no linked entity"
                    );
                }
                ensure!(
                    search
                        .params
                        .values()
                        .any(|value| value.contains("{query}")),
                    "{name}: search_api params need {{query}}"
                );
            }
            for link in &entity.links {
                ensure!(
                    entity.properties.contains_key(&link.via),
                    "{name}: link {} goes via {}, which is not a property",
                    link.kind,
                    link.via
                );
                ensure!(
                    link.kind
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b == b'_'),
                    "{name}: link type {} must be UPPER_SNAKE",
                    link.kind
                );
            }
        }
        for (source, name, entity) in self.entities() {
            for link in &entity.links {
                ensure!(
                    seen.contains_key(link.to.as_str()),
                    "{}: {name} links to unknown entity {}",
                    source.name,
                    link.to
                );
            }
        }
        Ok(())
    }
}

fn resolve(source: SourceYaml) -> Result<Source> {
    let mut definition = match &source.definition {
        Some(name) => builtin(name).with_context(|| {
            format!(
                "{}: unknown definition {name}; built in: {}",
                source.name,
                builtin_names().collect::<Vec<_>>().join(", ")
            )
        })?,
        None => Definition {
            description: String::new(),
            base_url: None,
            auth: None,
            setup: None,
            entities: IndexMap::new(),
        },
    };
    definition.entities.extend(source.entities);
    if definition.entities.is_empty() {
        bail!("{}: needs a definition or entities", source.name);
    }
    let mut auth = source.auth.or(definition.auth);
    if let Some(env) = source.token_env {
        auth.get_or_insert_with(|| Auth::bearer(None))
            .set_token_env(env);
    }
    Ok(Source {
        team: source.team,
        description: source.description.unwrap_or(definition.description),
        base_url: source.url.or(definition.base_url),
        auth,
        entities: definition.entities,
        name: source.name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sources: &str) -> Result<Config> {
        Config::parse(
            Path::new("/tmp/third-party.yaml"),
            &format!("sources:\n{sources}"),
        )
    }

    fn entity(name: &str, properties: &str, extra: &str) -> String {
        format!(
            "  - name: {name}\n    entities:\n      Thing:\n        type: thing\n        list: {{path: /t, items: /items}}\n        key: [/id]\n        label: name\n        properties: {{name: {{from: /name}}{properties}}}\n{extra}"
        )
    }

    #[test]
    fn builtins_and_the_example_parse() {
        let config = parse("  - {name: slack, definition: slack}\n  - {name: grafana, definition: grafana, url: https://g}\n").unwrap();
        assert!(config.entity("grafanadashboard").is_some());
        let example = Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/examples/third-party.yaml"
        ));
        assert_eq!(Config::load(example).unwrap().unwrap().sources.len(), 3);
    }

    #[test]
    fn invalid_configs_say_why() {
        let link = "        links: [{type: OWNED_BY, to: Nobody, via: name}]\n";
        for (sources, error) in [
            (entity("mr", "", ""), "orbit qualifier"),
            (entity("Tools", "", ""), "lowercase"),
            (entity("tools", "", link), "unknown entity Nobody"),
            (entity("tools", ", group: {from: /g}", ""), "SQL keyword"),
            (
                entity("tools", "", "").replace(
                    "list: {path: /t, items: /items}",
                    "get: {path: /t, items: /items}",
                ),
                "needs list, search_api, or both",
            ),
        ] {
            let found = parse(&sources).unwrap_err().to_string();
            assert!(found.contains(error), "{found}");
        }
    }
}
