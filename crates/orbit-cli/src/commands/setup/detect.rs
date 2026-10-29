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
    /// `VAR` replaces the default prefix.
    pub(super) fn expand_global_path(&self, global: &str) -> Result<PathBuf> {
        let (prefix, rest) = match global.strip_prefix("${") {
            Some(tail) => {
                let (spec, rest) = tail
                    .split_once('}')
                    .with_context(|| format!("unterminated ${{ in global path {global}"))?;
                let (name, default) = spec
                    .split_once(":-")
                    .with_context(|| format!("global path {global} needs a :- default"))?;
                let value = self.env.get(name).filter(|value| !value.is_empty());
                (value.map_or(default, String::as_str), rest)
            }
            None => (global, ""),
        };
        let joined = format!("{prefix}{rest}");
        let relative = joined
            .strip_prefix("~/")
            .map(|relative| self.home.join(relative));
        match (relative, Path::new(prefix).is_absolute()) {
            (Some(path), _) => Ok(path),
            (None, true) => Ok(PathBuf::from(joined)),
            (None, false) => anyhow::bail!("global path {global} must resolve to ~/ or absolute"),
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
