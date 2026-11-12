use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::config::{self, Definition};

pub struct Connectable {
    pub name: &'static str,
    pub definition: Definition,
}

impl Connectable {
    pub fn all() -> Vec<Self> {
        config::connectable()
            .into_iter()
            .map(|(name, definition)| Self { name, definition })
            .collect()
    }

    pub fn named(name: &str) -> Option<Self> {
        Self::all()
            .into_iter()
            .find(|found| found.name.eq_ignore_ascii_case(name))
    }

    pub fn names() -> Vec<&'static str> {
        Self::all().into_iter().map(|found| found.name).collect()
    }

    pub fn spec(&self) -> &config::SetupSpec {
        self.definition
            .setup
            .as_ref()
            .expect("connectable definitions have a setup block")
    }

    pub fn default_url(&self) -> Option<&str> {
        self.definition.base_url.as_deref()
    }

    pub fn token_env(&self) -> Option<&str> {
        self.definition
            .auth
            .as_ref()
            .and_then(|auth| auth.token_env())
    }

    async fn get(&self, url: &str, token: &str, path: &str) -> Result<(String, reqwest::Response)> {
        let headers = self
            .definition
            .auth
            .clone()
            .unwrap_or_else(|| config::Auth::bearer(None))
            .headers(token);
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()?;
        let endpoint = format!("{}{}", url.trim_end_matches('/'), path);
        let response = crate::auth::apply(http.get(&endpoint), &headers)
            .send()
            .await
            .with_context(|| format!("reaching {endpoint}"))?;
        Ok((endpoint, response))
    }

    pub async fn verify(&self, url: &str, token: &str) -> Result<Verified> {
        let verify = &self.spec().verify;
        let (endpoint, response) = self.get(url, token, &verify.path).await?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .with_context(|| format!("{endpoint} answered {status} without JSON"))?;
        if !status.is_success() {
            bail!("{endpoint} answered {status}; check the token");
        }
        if let Some(pointer) = &verify.error
            && let Some(error) = body.pointer(pointer).and_then(Value::as_str)
        {
            bail!("{} rejected the token: {error}", self.spec().title);
        }
        if let Some(pointer) = &verify.ok
            && body.pointer(pointer) != Some(&Value::Bool(true))
        {
            bail!("{} did not accept the token", self.spec().title);
        }
        let shows = body
            .pointer(&verify.shows)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let teams = match &self.spec().team {
            Some(lookup) if body.pointer(&lookup.when) == Some(&Value::Bool(true)) => {
                self.teams(url, token, lookup).await?
            }
            _ => Vec::new(),
        };
        Ok(Verified { shows, teams })
    }

    async fn teams(
        &self,
        url: &str,
        token: &str,
        lookup: &config::TeamLookup,
    ) -> Result<Vec<(String, String)>> {
        let (endpoint, response) = self.get(url, token, &lookup.path).await?;
        let body: Value = response
            .json()
            .await
            .with_context(|| format!("{endpoint} answered without JSON"))?;
        let teams: Vec<(String, String)> = body
            .pointer(&lookup.items)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|team| {
                let id = team.pointer(&lookup.id)?.as_str()?.to_string();
                let name = team
                    .pointer(&lookup.name)
                    .and_then(Value::as_str)
                    .unwrap_or(&id)
                    .to_string();
                Some((id, name))
            })
            .collect();
        anyhow::ensure!(
            !teams.is_empty(),
            "{} spans several workspaces but listed none it can read",
            self.spec().title
        );
        Ok(teams)
    }
}

#[derive(Debug)]
pub struct Verified {
    pub shows: String,
    pub teams: Vec<(String, String)>,
}

pub fn write_source(path: &Path, name: &str, url: Option<&str>, team: Option<&str>) -> Result<()> {
    let mut file: Value = match std::fs::read_to_string(path) {
        Ok(text) => orbit_utils::yaml::from_str(&text)
            .with_context(|| format!("parsing {}", path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            serde_json::json!({"sources": []})
        }
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    let sources = file
        .as_object_mut()
        .context("the third-party config must be a map")?
        .entry("sources")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .context("sources must be a list")?;
    for source in sources.iter_mut() {
        if let Some(map) = source.as_object_mut() {
            for retired in ["token_command", "refresh"] {
                map.remove(retired);
            }
        }
    }
    let position = sources.iter().position(|source| {
        source
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|found| found.eq_ignore_ascii_case(name))
    });
    let source = match position {
        Some(index) => &mut sources[index],
        None => {
            sources.push(serde_json::json!({"name": name, "definition": name}));
            sources.last_mut().expect("just pushed")
        }
    };
    let map = source
        .as_object_mut()
        .context("each source must be a map")?;
    match url {
        Some(url) => {
            map.insert("url".into(), url.trim_end_matches('/').into());
        }
        None => {
            map.remove("url");
        }
    }
    match team {
        Some(team) => {
            map.insert("team".into(), team.into());
        }
        None => {
            map.remove("team");
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let text = orbit_utils::yaml::to_string(&file)?;
    config::Config::parse(path, &text).context("the updated config does not validate")?;
    std::fs::write(path, text).with_context(|| format!("writing {}", path.display()))
}

pub fn remove_source(path: &Path, name: &str) -> Result<bool> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    let mut file: Value = orbit_utils::yaml::from_str(&text)?;
    let Some(sources) = file.get_mut("sources").and_then(Value::as_array_mut) else {
        return Ok(false);
    };
    let before = sources.len();
    sources.retain(|source| {
        !source
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|found| found.eq_ignore_ascii_case(name))
    });
    if sources.len() == before {
        return Ok(false);
    }
    std::fs::write(path, orbit_utils::yaml::to_string(&file)?)?;
    Ok(true)
}

pub fn configured(path: &Path, name: &str) -> bool {
    config::Config::load(path)
        .ok()
        .flatten()
        .is_some_and(|config| config.source(name).is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_source_adds_updates_and_removes_one_source() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("third-party.yaml");
        std::fs::write(
            &path,
            "sources:\n  - {name: slack, definition: slack, refresh: 5m, token_command: old}\n",
        )
        .unwrap();
        write_source(&path, "grafana", Some("https://g/"), None).unwrap();
        write_source(&path, "slack", None, Some("T1")).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            !text.contains("token_command") && !text.contains("refresh"),
            "{text}"
        );
        let config = config::Config::load(&path).unwrap().unwrap();
        assert_eq!(config.source("slack").unwrap().team.as_deref(), Some("T1"));
        assert_eq!(
            config.source("grafana").unwrap().base_url.as_deref(),
            Some("https://g")
        );
        assert!(remove_source(&path, "grafana").unwrap());
        assert!(!configured(&path, "grafana"));
        assert!(configured(&path, "slack"));
    }
}
