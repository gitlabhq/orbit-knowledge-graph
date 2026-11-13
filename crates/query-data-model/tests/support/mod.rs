#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use query_data_model::{DataModelError, Model, Packages, Tier};

/// The tier each fixture package loads at: the platform, an integration, or a tenant overlay.
pub fn tier(package: &str) -> Tier {
    match package {
        "gitlab" => Tier::Platform,
        package if package.starts_with("integration.") => Tier::Integration,
        _ => Tier::Tenant,
    }
}

/// The fixture packages named, each file passed through `edit`, at each package's tier.
pub fn packages(packages: &[&str], edit: impl Fn(&str, String) -> String) -> Packages {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/packages");
    let all = Packages::read_dir(&root, Tier::Platform).unwrap();
    let loaded = packages.iter().map(|package| {
        let prefix = format!("{package}/");
        let sources = all
            .sources()
            .iter()
            .filter(|(name, _)| name.starts_with(&prefix))
            .map(|(name, yaml)| (name.clone(), edit(name, yaml.clone())))
            .collect();
        Packages::new(tier(package), sources)
    });
    loaded.reduce(Packages::with).unwrap()
}

pub fn load_with(
    names: &[&str],
    edit: impl Fn(&str, String) -> String,
) -> Result<Arc<Model>, DataModelError> {
    packages(names, edit).deployment("server")
}

pub fn gitlab() -> Model {
    Arc::try_unwrap(load_with(&["gitlab"], |_, yaml| yaml).unwrap()).unwrap()
}
/// The embedded packages with edge-owned attributes on `ON_BRANCH`, in the access classes `classes`.
pub fn branch_attributes(classes: &str) -> Model {
    let mut sources = Packages::embedded().sources().clone();
    sources.insert(
        "overlay/package.yaml".into(),
        "kind: package\nname: overlay\nrequires: [gitlab]\n".into(),
    );
    sources.insert(
        "overlay/graph.yaml".into(),
        "kind: graph\npackage: overlay\nrelationships:\n  ON_BRANCH:\n    variants: {}\n    attributes: {branch: string, project_id: int64}\n".into(),
    );
    let access = sources.get_mut("gitlab/authorization.yaml").unwrap();
    let entry = "  ON_BRANCH:\n    scope: namespace\n    permission: read\n    enforce: {clickhouse: {scope_key: traversal_path, of: both}}\n";
    *access = access.replacen(entry, &format!("{entry}    attributes: {classes}\n"), 1);
    Arc::try_unwrap(
        Packages::new(Tier::Platform, sources)
            .deployment("server")
            .unwrap(),
    )
    .unwrap()
}
