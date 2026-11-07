use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::config::{Auth, Entity, Format, Source, Type};

#[derive(Debug, Clone)]
pub struct Record {
    pub id: i64,
    pub key: String,
    pub properties: BTreeMap<String, Value>,
    pub links: Vec<Link>,
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub struct Link {
    pub kind: String,
    pub target_kind: String,
    pub target_id: i64,
}

const PARENT_KEY: &str = "^key";

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

    /// Every entity of the source, parents before the entities fetched through them.
    pub async fn source(&self, source: &Source) -> Result<Vec<(String, Vec<Record>)>> {
        let header = auth(&source.name, source.auth.as_ref())
            .with_context(|| format!("{}: getting the token", source.name))?;
        let header = header.as_ref();
        let mut done: BTreeMap<String, Vec<Record>> = BTreeMap::new();
        let mut order = Vec::new();
        let mut pending: Vec<&String> = source.entities.keys().collect();
        while !pending.is_empty() {
            let before = pending.len();
            let mut next = Vec::new();
            for name in pending {
                let entity = &source.entities[name];
                let parent = entity
                    .fetch
                    .for_each
                    .as_ref()
                    .or(entity.fetch.within.as_ref());
                match parent {
                    Some(parent) if !done.contains_key(parent) => next.push(name),
                    _ => {
                        let parents = parent.map(|parent| done[parent].as_slice());
                        let records = self
                            .entity(source, header, name, entity, parents)
                            .await
                            .with_context(|| format!("{}: fetching {name}", source.name))?;
                        done.insert(name.clone(), records);
                        order.push(name.clone());
                    }
                }
            }
            if next.len() == before {
                bail!("{}: fetch parents form a cycle", source.name);
            }
            pending = next;
        }
        Ok(order
            .into_iter()
            .map(|name| {
                let records = done.remove(&name).unwrap_or_default();
                (name, records)
            })
            .collect())
    }

    async fn entity(
        &self,
        source: &Source,
        header: Option<&(String, String)>,
        name: &str,
        entity: &Entity,
        parents: Option<&[Record]>,
    ) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        let scopes: Vec<Option<&Record>> = match parents {
            Some(parents) => limit_parents(parents, entity)
                .into_iter()
                .map(Some)
                .collect(),
            None => vec![None],
        };
        for parent in scopes {
            let items = match (&entity.fetch.within, parent) {
                (Some(_), Some(parent)) => pointer_items(&parent.raw, &entity.fetch.items),
                _ => self.pages(source, header, entity, parent).await?,
            };
            for item in items {
                records.push(record(source, name, entity, parent, item)?);
            }
        }
        Ok(records)
    }

    async fn pages(
        &self,
        source: &Source,
        header: Option<&(String, String)>,
        entity: &Entity,
        parent: Option<&Record>,
    ) -> Result<Vec<Value>> {
        let base = source
            .base_url
            .as_deref()
            .with_context(|| format!("{} has no url", source.name))?
            .trim_end_matches('/');
        let fill = |text: &str| {
            let text = match parent {
                Some(parent) => text.replace("{key}", &parent.key),
                None => text.to_string(),
            };
            text.replace("{team}", source.team.as_deref().unwrap_or_default())
        };
        let path = fill(entity.fetch.path.as_deref().unwrap_or_default());
        let mut params: Vec<(String, String)> = entity
            .fetch
            .params
            .iter()
            .map(|(name, value)| (name.clone(), fill(value)))
            .filter(|(_, value)| !value.is_empty())
            .collect();
        let mut items = Vec::new();
        for _ in 0..entity.fetch.max_pages.max(1) {
            let mut request = self.http.get(format!("{base}{path}")).query(&params);
            if let Some((name, value)) = header {
                request = request.header(name, value);
            }
            let response = request.send().await?;
            let status = response.status();
            if status.is_redirection() {
                let to = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|location| location.to_str().ok())
                    .and_then(|location| reqwest::Url::parse(location).ok())
                    .and_then(|url| url.host_str().map(str::to_string))
                    .unwrap_or_else(|| "another page".into());
                bail!(
                    "{path} redirected to {to}, so it wants a sign-in; check the token or cookie"
                );
            }
            let body: Value = response
                .json()
                .await
                .with_context(|| format!("{path} answered {status} without JSON"))?;
            if !status.is_success() {
                bail!("{path} answered {status}: {}", short(&body));
            }
            if let Some(pointer) = &entity.fetch.error
                && let Some(error) = body.pointer(pointer).and_then(Value::as_str)
            {
                bail!("{path}: {error}");
            }
            items.extend(pointer_items(&body, &entity.fetch.items));
            let next = entity.fetch.cursor.as_ref().and_then(|cursor| {
                let next = body.pointer(&cursor.next)?.as_str()?;
                (!next.is_empty()).then(|| (cursor.param.clone(), next.to_string()))
            });
            let Some((param, next)) = next else {
                break;
            };
            params.retain(|(name, _)| *name != param);
            params.push((param, next));
        }
        Ok(items)
    }
}

/// The parents a `for_each` fetch runs for: all of them, or the `max_parents` with the
/// newest `parents_by` value.
fn limit_parents<'a>(parents: &'a [Record], entity: &Entity) -> Vec<&'a Record> {
    let mut chosen: Vec<&Record> = parents.iter().collect();
    let Some(max) = entity.fetch.max_parents else {
        return chosen;
    };
    if let Some(field) = &entity.fetch.parents_by {
        let at = |record: &Record| record.properties.get(field).map(text).unwrap_or_default();
        chosen.sort_by_key(|record| std::cmp::Reverse(at(record)));
    }
    chosen.truncate(max);
    chosen
}

fn auth(source: &str, auth: Option<&Auth>) -> Result<Option<(String, String)>> {
    let Some(auth) = auth else {
        return Ok(None);
    };
    let Some((token, _)) = crate::credentials::resolve(source, auth.token_env.as_deref()) else {
        return Ok(None);
    };
    let value = match &auth.scheme {
        Some(scheme) => format!("{scheme} {token}"),
        None => token,
    };
    Ok(Some((auth.header.clone(), value)))
}

fn short(body: &Value) -> String {
    body.to_string().chars().take(200).collect()
}

fn pointer_items(value: &Value, pointer: &str) -> Vec<Value> {
    match value.pointer(pointer) {
        Some(Value::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

fn lookup(item: &Value, parent: Option<&Record>, pointer: &str) -> Option<Value> {
    if pointer == PARENT_KEY {
        return parent.map(|parent| Value::String(parent.key.clone()));
    }
    item.pointer(pointer)
        .filter(|value| !value.is_null())
        .cloned()
}

fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn record(
    source: &Source,
    name: &str,
    entity: &Entity,
    parent: Option<&Record>,
    item: Value,
) -> Result<Record> {
    let parts: Vec<String> = entity
        .key
        .iter()
        .map(|pointer| {
            lookup(&item, parent, pointer)
                .map(|v| text(&v))
                .unwrap_or_default()
        })
        .collect();
    anyhow::ensure!(
        parts.iter().any(|part| !part.is_empty()),
        "{name}: an item has no key at {:?}",
        entity.key
    );
    let key = parts.join(":");
    let mut properties = BTreeMap::new();
    for (field, property) in &entity.properties {
        let Some(value) = lookup(&item, parent, &property.from) else {
            continue;
        };
        if let Some(value) = convert(&value, property.kind, property.format) {
            properties.insert(field.clone(), value);
        }
    }
    let links = entity
        .links
        .iter()
        .filter_map(|link| {
            let target = properties.get(&link.via).map(text)?;
            Some(Link {
                kind: link.kind.clone(),
                target_kind: link.to.clone(),
                target_id: crate::store::id(&source.name, &link.to, &target),
            })
        })
        .collect();
    Ok(Record {
        id: crate::store::id(&source.name, name, &key),
        key,
        properties,
        links,
        raw: item,
    })
}

fn convert(value: &Value, kind: Type, format: Format) -> Option<Value> {
    match kind {
        Type::String => Some(Value::String(text(value))),
        Type::Int => match value {
            Value::Number(n) => n.as_i64().map(Value::from),
            Value::String(s) => s.parse::<i64>().ok().map(Value::from),
            _ => None,
        },
        Type::Bool => match value {
            Value::Bool(b) => Some(Value::Bool(*b)),
            Value::String(s) => s.parse::<bool>().ok().map(Value::Bool),
            _ => None,
        },
        Type::Timestamp => timestamp(value, format).map(Value::String),
    }
}

fn timestamp(value: &Value, format: Format) -> Option<String> {
    let at = match format {
        Format::Unix => {
            let seconds: f64 = match value {
                Value::Number(n) => n.as_f64()?,
                Value::String(s) => s.parse().ok()?,
                _ => return None,
            };
            chrono::DateTime::from_timestamp_micros((seconds * 1_000_000.0) as i64)?
        }
        Format::Plain => chrono::DateTime::parse_from_rfc3339(value.as_str()?)
            .ok()?
            .with_timezone(&chrono::Utc),
    };
    Some(at.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_read_unix_seconds_and_rfc3339() {
        assert_eq!(
            timestamp(&Value::from("1712345678.000100"), Format::Unix).as_deref(),
            Some("2024-04-05 19:34:38.000100")
        );
        assert_eq!(
            timestamp(&Value::from("2024-04-05T19:34:38Z"), Format::Plain).as_deref(),
            Some("2024-04-05 19:34:38.000000")
        );
    }
}
