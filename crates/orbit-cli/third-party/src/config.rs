use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::Deserialize;

const BUILTIN: &[(&str, &str)] = &[
    ("slack", include_str!("../definitions/slack.yaml")),
    ("grafana", include_str!("../definitions/grafana.yaml")),
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Definition {
    #[serde(default)]
    base_url: Option<String>,
    env: Env,
    entities: IndexMap<String, Entity>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Env {
    token: String,
    url: String,
    #[serde(default)]
    team: Option<String>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Entity {
    #[serde(rename = "type")]
    pub kind: String,
    pub description: String,
    pub key: Vec<String>,
    pub label: String,
    pub search: Vec<String>,
    pub properties: IndexMap<String, Property>,
    pub links: Vec<Link>,
    pub newest: Option<String>,
    pub shows: Vec<String>,
    pub list: Option<Request>,
    pub search_api: Option<Request>,
    pub get: Option<Request>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Request {
    pub path: String,
    pub params: BTreeMap<String, String>,
    pub items: String,
    pub cursor: Option<Cursor>,
    pub max_pages: Option<usize>,
    pub limit: Option<usize>,
    pub within: BTreeMap<String, String>,
    pub within_name: BTreeMap<String, String>,
    pub key: Option<Vec<String>>,
    pub fields: BTreeMap<String, String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub param: String,
    pub next: String,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    #[default]
    String,
    Int,
    Timestamp,
    UnixTime,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Property {
    #[serde(rename = "type", default)]
    pub kind: Type,
    pub from: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    #[serde(rename = "type")]
    pub kind: String,
    pub to: String,
    pub via: String,
}

pub struct Source {
    pub name: String,
    pub base_url: Result<String, String>,
    pub token: String,
    pub team: Option<String>,
    pub entities: IndexMap<String, Entity>,
}

#[derive(Default)]
pub struct Config {
    pub sources: Vec<Source>,
}

impl Config {
    pub fn from_env(env: impl Fn(&str) -> Option<String>) -> Self {
        let set = |name: &str| {
            env(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let sources = BUILTIN.iter().filter_map(|(name, text)| {
            let definition: Definition =
                orbit_utils::yaml::from_str(text).expect("built-in definitions are valid");
            Some(Source {
                token: set(&definition.env.token)?,
                base_url: set(&definition.env.url)
                    .or(definition.base_url)
                    .ok_or(format!("{name} needs {}", definition.env.url)),
                team: definition.env.team.as_deref().and_then(set),
                name: name.to_string(),
                entities: definition.entities,
            })
        });
        Self {
            sources: sources.collect(),
        }
    }

    pub fn entities(&self) -> impl Iterator<Item = (&Source, &str, &Entity)> {
        let sources = self.sources.iter();
        sources.flat_map(|source| {
            source
                .entities
                .iter()
                .map(move |(name, entity)| (source, name.as_str(), entity))
        })
    }

    pub fn entity(&self, name: &str) -> Option<(&Source, &str, &Entity)> {
        self.entities()
            .find(|(_, entity, _)| entity.eq_ignore_ascii_case(name))
    }

    pub fn kind(&self, source: &Source, kind: &str) -> Option<String> {
        let mut entities = source.entities.iter();
        entities
            .find(|(_, entity)| entity.kind == kind)
            .map(|(name, _)| name.clone())
    }

    pub fn source(&self, name: &str) -> Option<&Source> {
        self.sources.iter().find(|source| source.name == name)
    }
}
