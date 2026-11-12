use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::config::{Entity, Request, Source, Type};

#[derive(Debug, Clone)]
pub struct Record {
    pub key: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Want {
    First(usize),
    All,
}

pub struct Fetcher {
    http: reqwest::Client,
}

impl Fetcher {
    pub fn new() -> Result<Self> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let http = reqwest::Client::builder()
            .user_agent(concat!("orbit-third-party/", env!("CARGO_PKG_VERSION")))
            .timeout(std::time::Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { http })
    }

    pub async fn list(&self, source: &Source, name: &str, entity: &Entity) -> Result<Vec<Record>> {
        let request = entity
            .list
            .as_ref()
            .with_context(|| format!("{name} has no list"))?;
        let params = filled(&request.params, |value| fill(value, source, &[]));
        self.pages(source, name, entity, request, params, Want::All)
            .await
    }

    pub async fn search(
        &self,
        source: &Source,
        name: &str,
        entity: &Entity,
        query: &str,
        within: &[(String, String)],
        want: Want,
    ) -> Result<Vec<Record>> {
        let search = entity
            .search_api
            .as_ref()
            .with_context(|| format!("{name} has no search_api"))?;
        let mut terms = vec![query.trim().to_string()];
        let mut extra = Vec::new();
        for (to, value) in within {
            let filter = match to.strip_suffix("#name") {
                Some(to) => search
                    .within_name
                    .get(to)
                    .map(|f| f.replace("{name}", value)),
                None => search.within.get(to).map(|f| f.replace("{key}", value)),
            };
            let Some(filter) = filter else {
                bail!("{name} cannot be narrowed by {to}");
            };
            match filter.split_once('=') {
                Some((param, value)) if !param.contains([' ', ':']) => {
                    extra.push((param.to_string(), value.to_string()))
                }
                _ => terms.push(filter),
            }
        }
        terms.retain(|term| !term.is_empty());
        let text = terms.join(" ");
        let mut params = filled(&search.params, |value| {
            fill(value, source, &[]).replace("{query}", &text)
        });
        params.extend(extra);
        self.pages(source, name, entity, search, params, want).await
    }

    async fn pages(
        &self,
        source: &Source,
        name: &str,
        entity: &Entity,
        request: &Request,
        mut params: Vec<(String, String)>,
        want: Want,
    ) -> Result<Vec<Record>> {
        let mut items = Vec::new();
        let path = fill(&request.path, source, &[]);
        let pages = request.max_pages.unwrap_or(10).max(1);
        for page in 1..=pages {
            let body = self.get(source, &path, &params).await?;
            let found = pointer_items(&body, &request.items);
            let full = request.limit.is_some_and(|limit| found.len() >= limit);
            items.extend(found);
            if let Want::First(enough) = want
                && items.len() >= enough
            {
                items.truncate(enough);
                break;
            }
            let next = request.cursor.as_ref().and_then(|cursor| {
                let next = body.pointer(&cursor.next)?.as_str()?;
                (!next.is_empty()).then(|| (cursor.param.clone(), next.to_string()))
            });
            match next {
                Some(_) if page == pages && want != Want::All => break,
                None if !full || want != Want::All => break,
                Some((param, next)) if page < pages => {
                    params.retain(|(found, _)| *found != param);
                    params.push((param, next));
                }
                _ => bail!(
                    "{name}: there are more than {} results, so they would be cut off; narrow it with more words or a reference",
                    items.len()
                ),
            }
        }
        let shape = shaped(entity, request);
        items
            .into_iter()
            .map(|item| record(name, &shape, &[], item))
            .collect()
    }

    pub async fn get_one(
        &self,
        source: &Source,
        name: &str,
        entity: &Entity,
        key: &str,
    ) -> Result<Option<Record>> {
        let Some(request) = &entity.get else {
            let all = self.list(source, name, entity).await?;
            return Ok(all.into_iter().find(|record| record.key == key));
        };
        let parts: Vec<&str> = key.splitn(entity.key.len().max(1), ':').collect();
        let fill = |text: &str| fill(text, source, &parts).replace("{key}", key);
        let body = self
            .get(source, &fill(&request.path), &filled(&request.params, fill))
            .await?;
        let items = match body.pointer(&request.items) {
            Some(Value::Array(items)) => items.clone(),
            Some(item @ Value::Object(_)) => vec![item.clone()],
            _ => Vec::new(),
        };
        let shape = shaped(entity, request);
        items
            .into_iter()
            .map(|item| record(name, &shape, &parts, item))
            .find(|found| found.as_ref().is_ok_and(|found| found.key == key))
            .transpose()
    }

    async fn get(&self, source: &Source, path: &str, params: &[(String, String)]) -> Result<Value> {
        let base = source.base_url.clone().map_err(anyhow::Error::msg)?;
        let response = self
            .http
            .get(format!("{}{path}", base.trim_end_matches('/')))
            .query(params)
            .bearer_auth(&source.token)
            .send()
            .await?;
        let status = response.status();
        if status.is_redirection() {
            bail!("{path} redirected, so it wants a sign-in; check the token");
        }
        let body: Value = response
            .json()
            .await
            .with_context(|| format!("{path} answered {status} without JSON"))?;
        if !status.is_success() {
            bail!(
                "{path} answered {status}: {}",
                body.to_string().chars().take(200).collect::<String>()
            );
        }
        if let Some(error) = body.pointer("/error").and_then(Value::as_str) {
            bail!("{path}: {error}");
        }
        Ok(body)
    }
}

fn fill(text: &str, source: &Source, parts: &[&str]) -> String {
    let mut text = text.replace("{team}", source.team.as_deref().unwrap_or_default());
    for (index, part) in parts.iter().enumerate() {
        text = text.replace(&format!("{{key.{index}}}"), part);
    }
    text
}

fn filled(
    params: &BTreeMap<String, String>,
    fill: impl Fn(&str) -> String,
) -> Vec<(String, String)> {
    params
        .iter()
        .map(|(param, value)| (param.clone(), fill(value)))
        .filter(|(_, value)| !value.is_empty())
        .collect()
}

fn shaped(entity: &Entity, request: &Request) -> Entity {
    let mut shape = entity.clone();
    if let Some(key) = &request.key {
        shape.key = key.clone();
    }
    for (field, from) in &request.fields {
        if let Some(property) = shape.properties.get_mut(field) {
            property.from = from.clone();
        }
    }
    shape
}

fn pointer_items(value: &Value, pointer: &str) -> Vec<Value> {
    match value.pointer(pointer) {
        Some(Value::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

fn lookup(item: &Value, parts: &[&str], pointer: &str) -> Option<Value> {
    if let Some(index) = pointer
        .strip_prefix("{key.")
        .and_then(|rest| rest.strip_suffix('}'))
    {
        let part = parts.get(index.parse::<usize>().ok()?)?;
        return Some(Value::String(part.to_string()));
    }
    item.pointer(pointer)
        .filter(|value| !value.is_null())
        .cloned()
}

pub(crate) fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => items.iter().map(text).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

fn record(name: &str, entity: &Entity, parts: &[&str], item: Value) -> Result<Record> {
    let key: Vec<String> = entity
        .key
        .iter()
        .map(|pointer| {
            lookup(&item, parts, pointer)
                .map(|value| text(&value))
                .unwrap_or_default()
        })
        .collect();
    anyhow::ensure!(
        key.iter().any(|part| !part.is_empty()),
        "{name}: an item has no key at {:?}",
        entity.key
    );
    let properties = entity
        .properties
        .iter()
        .filter_map(|(field, property)| {
            let value = lookup(&item, parts, &property.from)?;
            Some((field.clone(), convert(&value, property.kind)?))
        })
        .collect();
    Ok(Record {
        key: key.join(":"),
        properties,
    })
}

fn convert(value: &Value, kind: Type) -> Option<Value> {
    let at = match kind {
        Type::String => return Some(Value::String(text(value))),
        Type::Int => return text(value).parse::<i64>().ok().map(Value::from),
        Type::UnixTime => {
            let seconds: f64 = text(value).parse().ok()?;
            chrono::DateTime::from_timestamp_micros((seconds * 1e6) as i64)?
        }
        Type::Timestamp => chrono::DateTime::parse_from_rfc3339(value.as_str()?)
            .ok()?
            .to_utc(),
    };
    Some(Value::String(
        at.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
    ))
}
