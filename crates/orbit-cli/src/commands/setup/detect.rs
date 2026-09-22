use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::spec::{self, Agent};

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

    pub(super) fn display_with_tilde(&self, path: &Path) -> String {
        match path.strip_prefix(&self.home) {
            Ok(rest) => format!("~/{}", rest.display()),
            Err(_) => path.display().to_string(),
        }
    }

    pub(super) fn installed_assistants(&self) -> Vec<(Agent, PathBuf)> {
        spec::all()
            .filter_map(|assistant| {
                let found = assistant
                    .detect
                    .iter()
                    .filter_map(|pattern| self.expand(pattern))
                    .find(|path| path.exists())?;
                Some((assistant, found))
            })
            .collect()
    }

    fn expand(&self, pattern: &str) -> Option<PathBuf> {
        if let Some(rest) = pattern.strip_prefix("~/") {
            return Some(self.home.join(rest));
        }
        let variable = pattern.strip_prefix('$')?;
        let (name, rest) = variable.split_once('/').unwrap_or((variable, ""));
        let base = self.env.get(name).filter(|value| !value.is_empty())?;
        Some(Path::new(base).join(rest))
    }
}
