use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use rust_embed::Embed;

use crate::spec::{Fragment, TEMPLATE_EXTENSION};
use crate::{DataModelError, Model};

#[derive(Embed)]
#[folder = "$ORBIT_CONFIG_DIR/model"]
#[include = "*.yaml"]
#[include = "*.sql.j2"]
struct Embedded;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packages {
    sources: BTreeMap<String, String>,
}

impl Packages {
    pub fn new(sources: BTreeMap<String, String>) -> Self {
        Self { sources }
    }

    pub fn embedded() -> Self {
        Self::new(
            Embedded::iter()
                .filter_map(|path| {
                    let file = Embedded::get(&path)?;
                    let text = String::from_utf8_lossy(&file.data).into_owned();
                    Some((path.into_owned(), text))
                })
                .collect(),
        )
    }

    pub fn read_dir(root: &Path) -> std::io::Result<Self> {
        let mut sources = BTreeMap::new();
        for package in std::fs::read_dir(root)? {
            let package = package?.path();
            if !package.is_dir() {
                continue;
            }
            for file in std::fs::read_dir(&package)? {
                let file = file?.path();
                let name = file.to_string_lossy();
                if name.ends_with(".yaml") || name.ends_with(TEMPLATE_EXTENSION) {
                    let relative = file.strip_prefix(root).map_err(std::io::Error::other)?;
                    let key = relative.to_string_lossy().into_owned();
                    sources.insert(key, std::fs::read_to_string(&file)?);
                }
            }
        }
        Ok(Self::new(sources))
    }

    pub fn sources(&self) -> &BTreeMap<String, String> {
        &self.sources
    }

    pub fn fragments(&self) -> Result<Vec<Fragment>, DataModelError> {
        Fragment::parse_all(
            self.sources
                .iter()
                .map(|(path, yaml)| (path.as_str(), yaml.as_str())),
        )
    }

    pub fn load(&self) -> Result<Arc<Model>, DataModelError> {
        Model::load(&self.fragments()?).map(Arc::new)
    }
}
