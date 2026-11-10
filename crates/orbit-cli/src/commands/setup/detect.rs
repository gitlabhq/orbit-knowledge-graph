use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::spec::{self, Agent};

#[derive(Clone)]
pub(crate) struct Machine {
    home: PathBuf,
    env: BTreeMap<String, String>,
}

impl Machine {
    pub(crate) fn current() -> Result<Machine> {
        let home = dirs::home_dir().context("could not determine home directory")?;
        Ok(Machine::new(home, std::env::vars().collect()))
    }

    pub(crate) fn new(home: PathBuf, env: BTreeMap<String, String>) -> Machine {
        Machine { home, env }
    }

    pub(super) fn home(&self) -> &Path {
        &self.home
    }

    /// Expands a global path: `~/x`, or `${VAR:-~/default}/x` where a non-empty
    /// `VAR` replaces the default prefix and must be absolute.
    pub(super) fn expand_global_path(&self, global: &str) -> Result<PathBuf> {
        let Some(tail) = global.strip_prefix("${") else {
            return self.expand_home(global, global);
        };
        let (spec, rest) = tail
            .split_once('}')
            .with_context(|| format!("unterminated ${{ in global path {global}"))?;
        let (name, default) = spec
            .split_once(":-")
            .with_context(|| format!("global path {global} needs a :- default"))?;
        match self.env.get(name).filter(|value| !value.is_empty()) {
            Some(value) if Path::new(value).is_absolute() => {
                Ok(PathBuf::from(format!("{value}{rest}")))
            }
            Some(value) => anyhow::bail!("{name}={value} must be an absolute path"),
            None => self.expand_home(&format!("{default}{rest}"), global),
        }
    }

    fn expand_home(&self, path: &str, global: &str) -> Result<PathBuf> {
        match path.strip_prefix("~/") {
            Some(relative) => Ok(self.home.join(relative)),
            None => anyhow::bail!("global path {global} must start with ~/"),
        }
    }

    pub(super) fn display_with_tilde(&self, path: &Path) -> String {
        match path.strip_prefix(&self.home) {
            Ok(rest) => format!("~/{}", rest.display()),
            Err(_) => path.display().to_string(),
        }
    }

    pub(super) fn installed_agents(&self) -> Vec<(Agent, PathBuf)> {
        spec::agents()
            .filter_map(|agent| {
                let found = agent
                    .detect
                    .iter()
                    .filter_map(|pattern| self.expand_detect_pattern(pattern))
                    .find(|path| path.exists())?;
                Some((agent, found))
            })
            .collect()
    }

    fn expand_detect_pattern(&self, pattern: &str) -> Option<PathBuf> {
        if let Some(rest) = pattern.strip_prefix("~/") {
            return Some(self.home.join(rest));
        }
        let variable = pattern.strip_prefix('$')?;
        let (name, rest) = variable.split_once('/').unwrap_or((variable, ""));
        let base = self.env.get(name).filter(|value| !value.is_empty())?;
        Some(Path::new(base).join(rest))
    }
}
