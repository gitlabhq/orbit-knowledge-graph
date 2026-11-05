use std::collections::BTreeMap;
use std::sync::Arc;

use rust_embed::Embed;

use crate::{DataModelError, Model};

#[derive(Embed)]
#[folder = "$ORBIT_CONFIG_DIR/model"]
#[include = "*.yaml"]
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

    pub fn sources(&self) -> &BTreeMap<String, String> {
        &self.sources
    }

    pub fn load(&self) -> Result<Arc<Model>, DataModelError> {
        Model::from_yaml(
            self.sources
                .iter()
                .map(|(path, yaml)| (path.as_str(), yaml.as_str())),
        )
        .map(Arc::new)
    }
}
