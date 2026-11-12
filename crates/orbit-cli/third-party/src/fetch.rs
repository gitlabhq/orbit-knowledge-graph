use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::config::{Entity, Format, Source, Type};

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
        let headers = headers(source)?;
        let base = base(source)?;
        let fill = |text: &str| fill(text, source, &[]);
        let path = fill(&request.path);
        let mut params = filled(&request.params, fill);
        let mut items = Vec::new();
        for page in 0..request.max_pages.max(1) {
            let body = self
                .get(base, &path, &params, &headers, request.error.as_deref())
                .await?;
            items.extend(pointer_items(&body, &request.items));
            let next = request.cursor.as_ref().and_then(|cursor| {
                let next = body.pointer(&cursor.next)?.as_str()?;
                (!next.is_empty()).then(|| (cursor.param.clone(), next.to_string()))
            });
            let Some((param, next)) = next else {
                break;
            };
            params.retain(|(found, _)| *found != param);
            params.push((param, next));
            if page + 1 == request.max_pages.max(1) {
                bail!(
                    "{name}: listing stopped after {} pages, so results would be cut off",
                    request.max_pages
                );
            }
        }
        let shape = shaped(entity, request.key.as_ref(), &request.fields);
        items
            .into_iter()
            .map(|item| record(name, &shape, &[], item))
            .collect()
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
        let headers = headers(source)?;
        let base = base(source)?;
        let mut terms = vec![query.trim().to_string()];
        let mut extra: Vec<(String, String)> = Vec::new();
        for (to, value) in within {
            let filter = match to.strip_suffix("#name") {
                Some(to) => search
                    .within_name
                    .get(to)
                    .map(|filter| filter.replace("{name}", value)),
                None => search
                    .within
                    .get(to)
                    .map(|filter| filter.replace("{key}", value)),
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
        let text = terms
            .into_iter()
            .filter(|term| !term.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let mut params = filled(&search.params, |value| {
            fill(value, source, &[]).replace("{query}", &text)
        });
        params.extend(extra);
        let mut items = Vec::new();
        let complete = want == Want::All;
        let pages = search.max_pages.max(1);
        for page in 0..pages {
            let body = self
                .get(
                    base,
                    &search.path,
                    &params,
                    &headers,
                    search.error.as_deref(),
                )
                .await?;
            let found = pointer_items(&body, &search.items);
            let full = complete && search.limit.is_some_and(|limit| found.len() >= limit);
            items.extend(found);
            let next = search.cursor.as_ref().and_then(|cursor| {
                let next = body.pointer(&cursor.next)?.as_str()?;
                (!next.is_empty()).then(|| (cursor.param.clone(), next.to_string()))
            });
            if let Want::First(enough) = want
                && items.len() >= enough
            {
                items.truncate(enough);
                break;
            }
            match next {
                Some(_) if page + 1 == pages && !complete => break,
                Some((param, next)) => {
                    if page + 1 == pages {
                        bail!(
                            "{name}: the search has more than {} results, so they would be cut off; narrow it with more words or a reference",
                            items.len()
                        );
                    }
                    params.retain(|(found, _)| *found != param);
                    params.push((param, next));
                }
                None if full => bail!(
                    "{name}: the search returned its limit of {}, so results would be cut off; narrow it with more words or a reference",
                    items.len()
                ),
                None => break,
            }
        }
        let shape = shaped(entity, search.key.as_ref(), &search.fields);
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
        let headers = headers(source)?;
        let base = base(source)?;
        let parts: Vec<&str> = key.splitn(entity.key.len().max(1), ':').collect();
        let fill = |text: &str| fill(text, source, &parts).replace("{key}", key);
        let params = filled(&request.params, fill);
        let body = self
            .get(
                base,
                &fill(&request.path),
                &params,
                &headers,
                request.error.as_deref(),
            )
            .await?;
        let shape = shaped(entity, request.key.as_ref(), &request.fields);
        let items = match body.pointer(&request.items) {
            Some(Value::Array(items)) => items.clone(),
            Some(item @ Value::Object(_)) => vec![item.clone()],
            _ => Vec::new(),
        };
        items
            .into_iter()
            .map(|item| record(name, &shape, &parts, item))
            .find(|found| found.as_ref().is_ok_and(|found| found.key == key))
            .transpose()
    }

    async fn get(
        &self,
        base: &str,
        path: &str,
        params: &[(String, String)],
        headers: &[(String, String)],
        error: Option<&str>,
    ) -> Result<Value> {
        let request = crate::auth::apply(
            self.http.get(format!("{base}{path}")).query(params),
            headers,
        );
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
            bail!("{path} redirected to {to}, so it wants a sign-in; check the token");
        }
        let body: Value = response
            .json()
            .await
            .with_context(|| format!("{path} answered {status} without JSON"))?;
        if !status.is_success() {
            bail!("{path} answered {status}: {}", short(&body));
        }
        if let Some(pointer) = error
            && let Some(error) = body.pointer(pointer).and_then(Value::as_str)
        {
            bail!("{path}: {error}");
        }
        Ok(body)
    }
}

fn headers(source: &Source) -> Result<Vec<(String, String)>> {
    match &source.auth {
        Some(auth) => auth.resolve(&source.name),
        None => Ok(Vec::new()),
    }
}

fn base(source: &Source) -> Result<&str> {
    Ok(source
        .base_url
        .as_deref()
        .with_context(|| format!("{} has no url", source.name))?
        .trim_end_matches('/'))
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

fn shaped(entity: &Entity, key: Option<&Vec<String>>, fields: &BTreeMap<String, String>) -> Entity {
    let mut shape = entity.clone();
    if let Some(key) = key {
        shape.key = key.clone();
    }
    for (field, from) in fields {
        if let Some(property) = shape.properties.get_mut(field) {
            property.from = from.clone();
        }
    }
    shape
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
    let key_parts: Vec<String> = entity
        .key
        .iter()
        .map(|pointer| {
            lookup(&item, parts, pointer)
                .map(|value| text(&value))
                .unwrap_or_default()
        })
        .collect();
    anyhow::ensure!(
        key_parts.iter().any(|part| !part.is_empty()),
        "{name}: an item has no key at {:?}",
        entity.key
    );
    let mut properties = BTreeMap::new();
    for (field, property) in &entity.properties {
        let Some(value) = lookup(&item, parts, &property.from) else {
            continue;
        };
        if let Some(value) = convert(&value, property.kind, property.format) {
            properties.insert(field.clone(), value);
        }
    }
    Ok(Record {
        key: key_parts.join(":"),
        properties,
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
