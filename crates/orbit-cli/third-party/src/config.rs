use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use indexmap::IndexMap;
use serde::Deserialize;

const BUILTIN: &[(&str, &str)] = &[
    ("slack", include_str!("../definitions/slack.yaml")),
    ("argocd", include_str!("../definitions/argocd.yaml")),
];

/// Qualifiers grep and context already read, so no source can take these names.
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

/// What `orbit setup <source>` asks for and how it checks the answer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupSpec {
    pub title: String,
    pub token_help: String,
    #[serde(default)]
    pub token_prefix: Option<String>,
    #[serde(default)]
    pub asks_url: bool,
    #[serde(default)]
    pub url_help: Option<String>,
    pub verify: Verify,
    /// Finds the `team` for tokens that span several, such as Enterprise Grid org tokens.
    #[serde(default)]
    pub team: Option<TeamLookup>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamLookup {
    /// When this verify field is true, the token needs a team.
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Auth {
    #[serde(default = "Auth::default_header")]
    pub header: String,
    #[serde(default)]
    pub scheme: Option<String>,
    #[serde(default)]
    pub token_env: Option<String>,
}

impl Auth {
    fn default_header() -> String {
        "Authorization".into()
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entity {
    /// Short name in `source:type:key` references, such as `message` in
    /// `slack:message:C1:1712345678.000100`.
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub description: String,
    pub fetch: Fetch,
    pub key: Vec<String>,
    pub label: String,
    #[serde(default)]
    pub search: Vec<String>,
    pub properties: IndexMap<String, Property>,
    #[serde(default)]
    pub links: Vec<Link>,
    /// Property grep and context order results by, newest first.
    #[serde(default)]
    pub newest: Option<String>,
    /// What grep shows under each result: properties, or `LINK.property` to name a linked
    /// entity, such as `IN_CHANNEL.name`.
    #[serde(default)]
    pub shows: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fetch {
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    pub items: String,
    #[serde(default)]
    pub for_each: Option<String>,
    #[serde(default)]
    pub within: Option<String>,
    #[serde(default)]
    pub cursor: Option<Cursor>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default = "Fetch::default_max_pages")]
    pub max_pages: usize,
    /// With `for_each`, fetch for at most this many parents, the most recently active first.
    #[serde(default)]
    pub max_parents: Option<usize>,
    /// With `max_parents`, the parent property to order by, newest first.
    #[serde(default)]
    pub parents_by: Option<String>,
}

impl Fetch {
    fn default_max_pages() -> usize {
        5
    }
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

#[derive(Debug, Clone, Deserialize)]
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
    /// Removed; read only to warn that it is ignored.
    #[serde(default)]
    token_command: Option<String>,
    #[serde(default)]
    refresh: Option<String>,
    #[serde(default)]
    description: Option<String>,
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
    /// Fills `{team}` in fetch params, such as the Slack workspace an Enterprise Grid token
    /// lists channels in.
    pub team: Option<String>,
    pub refresh: std::time::Duration,
    pub entities: IndexMap<String, Entity>,
}

pub const DEFAULT_REFRESH: std::time::Duration = std::time::Duration::from_secs(15 * 60);

fn duration(text: &str) -> Result<std::time::Duration> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: u64 = number
        .parse()
        .with_context(|| format!("refresh {text:?} needs a number, such as 15m"))?;
    let seconds = match unit {
        "s" => 1,
        "m" | "" => 60,
        "h" => 3600,
        "d" => 86400,
        _ => bail!("refresh {text:?}: use s, m, h, or d"),
    };
    Ok(std::time::Duration::from_secs(number * seconds))
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

/// Built-in definitions `orbit setup` can connect, with their setup prompts.
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

    /// The entity a source calls `kind`, also matching the plural, as in `slack:messages`.
    pub fn kind(&self, source: &Source, kind: &str) -> Option<String> {
        let lower = kind.to_ascii_lowercase();
        [
            Some(lower.as_str()),
            lower.strip_suffix("es"),
            lower.strip_suffix('s'),
        ]
        .into_iter()
        .flatten()
        .find_map(|candidate| {
            source
                .entities
                .iter()
                .find(|(_, entity)| entity.kind == candidate)
                .map(|(name, _)| name.clone())
        })
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
            for field in &entity.search {
                ensure!(
                    declared(field),
                    "{name}: search field {field} is not a property"
                );
            }
            if let Some(field) = &entity.newest {
                ensure!(
                    entity.properties.contains_key(field),
                    "{name}: newest {field} is not a property"
                );
            }
            for shown in &entity.shows {
                match shown.split_once('.') {
                    Some((link, _)) => ensure!(
                        entity.links.iter().any(|found| found.kind == link),
                        "{name}: shows {shown} names no link {link}"
                    ),
                    None => ensure!(declared(shown), "{name}: shows {shown} is not a property"),
                }
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
            let parent = entity.fetch.for_each.iter().chain(&entity.fetch.within);
            for parent in parent {
                ensure!(
                    source.entities.contains_key(parent),
                    "{name}: fetch parent {parent} is not an entity of {}",
                    source.name
                );
            }
            ensure!(
                entity.fetch.path.is_some() || entity.fetch.within.is_some(),
                "{name}: fetch needs a path or within"
            );
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
    if source.token_command.is_some() {
        let env = source
            .token_env
            .clone()
            .unwrap_or_else(|| format!("{}_TOKEN", source.name.to_ascii_uppercase()));
        eprintln!(
            "orbit: {}: token_command is no longer read; set {env} instead, and run `orbit auth {}` to drop the line",
            source.name, source.name
        );
    }
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
    let default_auth = || Auth {
        header: Auth::default_header(),
        scheme: Some("Bearer".into()),
        token_env: None,
    };
    if let Some(env) = source.token_env {
        auth.get_or_insert_with(default_auth).token_env = Some(env);
    }
    let refresh = match &source.refresh {
        Some(text) => duration(text).with_context(|| source.name.clone())?,
        None => DEFAULT_REFRESH,
    };
    Ok(Source {
        team: source.team,
        refresh,
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

    #[test]
    fn builtins_parse_and_validate() {
        let config = Config::parse(
            Path::new("/tmp/sources.yaml"),
            "sources:\n  - {name: slack, definition: slack}\n  - {name: argocd, definition: argocd, url: https://argo}\n",
        )
        .unwrap();
        assert!(config.entity("SlackMessage").is_some());
        assert!(config.entity("argoapplication").is_some());
        assert_eq!(
            config.source("argocd").unwrap().base_url.as_deref(),
            Some("https://argo")
        );
    }

    #[test]
    fn the_example_config_parses() {
        let path = Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/examples/third-party.yaml"
        ));
        let config = Config::load(path).unwrap().unwrap();
        assert_eq!(config.sources.len(), 3);
        assert!(config.entity("Incident").is_some());
    }

    #[test]
    fn source_names_cannot_take_a_grep_qualifier_and_types_are_unique() {
        let config = |name: &str, kinds: (&str, &str)| {
            Config::parse(
                Path::new("/tmp/sources.yaml"),
                &format!(
                    "sources:\n  - name: {name}\n    entities:\n      One:\n        type: {}\n        fetch: {{path: /t, items: /items}}\n        key: [/id]\n        label: name\n        properties: {{name: {{from: /name}}}}\n      Two:\n        type: {}\n        fetch: {{path: /t, items: /items}}\n        key: [/id]\n        label: name\n        properties: {{name: {{from: /name}}}}\n",
                    kinds.0, kinds.1
                ),
            )
        };
        assert!(config("tools", ("one", "two")).is_ok());
        let error = config("mr", ("one", "two")).unwrap_err();
        assert!(error.to_string().contains("orbit qualifier"), "{error}");
        let error = config("Tools", ("one", "two")).unwrap_err();
        assert!(error.to_string().contains("lowercase"), "{error}");
        let error = config("tools", ("one", "one")).unwrap_err();
        assert!(
            error.to_string().contains("two entities have type one"),
            "{error}"
        );
    }

    #[test]
    fn refresh_reads_units() {
        assert_eq!(duration("90s").unwrap().as_secs(), 90);
        assert_eq!(duration("2h").unwrap().as_secs(), 7200);
        assert_eq!(duration("0").unwrap().as_secs(), 0);
        assert!(duration("soon").is_err());
    }

    #[test]
    fn links_must_name_known_entities() {
        let error = Config::parse(
            Path::new("/tmp/sources.yaml"),
            "sources:\n  - name: x\n    entities:\n      Thing:\n        type: thing\n        fetch: {path: /t, items: /items}\n        key: [/id]\n        label: name\n        properties: {name: {from: /name}, owner: {from: /owner}}\n        links: [{type: OWNED_BY, to: Nobody, via: owner}]\n",
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("unknown entity Nobody"),
            "{error}"
        );
    }

    #[test]
    fn sql_keywords_are_rejected_as_property_names() {
        let error = Config::parse(
            Path::new("/tmp/sources.yaml"),
            "sources:\n  - name: x\n    entities:\n      Thing:\n        type: thing\n        fetch: {path: /t, items: /items}\n        key: [/id]\n        label: group\n        properties: {group: {from: /group}}\n",
        )
        .unwrap_err();
        assert!(error.to_string().contains("SQL keyword"), "{error}");
    }
}
