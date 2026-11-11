use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use rust_embed::Embed;

use crate::spec::{Fragments, TEMPLATE_EXTENSION};
use crate::{DataModelError, Model};

#[derive(Embed)]
#[folder = "$ORBIT_CONFIG_DIR/model"]
#[include = "*.yaml"]
#[include = "*.sql.j2"]
struct Embedded;

/// How far the loader trusts a package, from where the package was loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    Platform,
    Integration,
    Tenant,
}

/// Package files by path, each package directory loaded at one tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packages {
    sources: BTreeMap<String, String>,
    tiers: BTreeMap<String, Tier>,
}

impl Packages {
    pub fn new(tier: Tier, sources: BTreeMap<String, String>) -> Self {
        let tiers = sources
            .keys()
            .map(|path| (directory(path).to_string(), tier))
            .collect();
        Self { sources, tiers }
    }

    pub fn embedded() -> Self {
        let sources = Embedded::iter().map(|path| {
            let file = Embedded::get(&path).unwrap_or_else(|| panic!("embedded {path} vanished"));
            let text = String::from_utf8(file.data.into_owned())
                .unwrap_or_else(|error| panic!("embedded {path} is not UTF-8: {error}"));
            (path.into_owned(), text)
        });
        Self::new(Tier::Platform, sources.collect())
    }

    pub fn read_dir(root: &Path, tier: Tier) -> std::io::Result<Self> {
        let mut sources = BTreeMap::new();
        for package in std::fs::read_dir(root)? {
            let package = package?.path();
            if package.is_dir() {
                read_package(root, &package, &mut sources)?;
            }
        }
        Ok(Self::new(tier, sources))
    }

    /// These packages and `other`'s, each at the tier it was loaded with.
    #[must_use]
    pub fn with(mut self, other: Self) -> Self {
        self.sources.extend(other.sources);
        self.tiers.extend(other.tiers);
        self
    }

    pub fn sources(&self) -> &BTreeMap<String, String> {
        &self.sources
    }

    pub fn tiers(&self) -> &BTreeMap<String, Tier> {
        &self.tiers
    }

    pub fn fragments(&self) -> Result<Fragments, DataModelError> {
        Fragments::parse(
            self.sources
                .iter()
                .map(|(path, yaml)| (path.as_str(), yaml.as_str())),
            &self.tiers,
        )
    }

    pub fn load(&self) -> Result<Arc<Model>, DataModelError> {
        Model::load(&self.fragments()?).map(Arc::new)
    }
}

/// The package directory a package file sits in.
pub(crate) fn directory(path: &str) -> &str {
    path.split_once('/')
        .map_or(path, |(directory, _)| directory)
}

fn read_package(
    root: &Path,
    directory: &Path,
    sources: &mut BTreeMap<String, String>,
) -> std::io::Result<()> {
    for file in std::fs::read_dir(directory)? {
        let file = file?.path();
        let name = file.to_string_lossy();
        if file.is_dir() {
            read_package(root, &file, sources)?;
        } else if name.ends_with(".yaml") || name.ends_with(TEMPLATE_EXTENSION) {
            let relative = file.strip_prefix(root).map_err(std::io::Error::other)?;
            let key = relative.to_string_lossy().into_owned();
            sources.insert(key, std::fs::read_to_string(&file)?);
        }
    }
    Ok(())
}
