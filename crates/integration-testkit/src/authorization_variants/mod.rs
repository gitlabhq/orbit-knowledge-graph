//! Model variants that only tests load: small platform packages and deployment configs that
//! bind their provider names to seeded fakes of other authorization systems.

pub mod fakes;
pub mod slack;

use std::path::{Path, PathBuf};

use orbit_server::authorization::Registry;
use orbit_server_config::AuthorizationConfig;
use query_data_model::{Packages, Tier};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("model-variants")
}

/// The variant platform packages named, loaded at the platform tier.
pub fn platform(packages: &[&str]) -> Packages {
    let all = Packages::read_dir(&root().join("platform"), Tier::Platform)
        .unwrap_or_else(|error| panic!("reading the variant packages: {error}"));
    let sources = all
        .sources()
        .iter()
        .filter(|(path, _)| {
            packages
                .iter()
                .any(|name| path.starts_with(&format!("{name}/")))
        })
        .map(|(path, text)| (path.clone(), text.clone()))
        .collect();
    Packages::new(Tier::Platform, sources)
}

/// The deployment config `deployments/<name>.yaml`.
pub fn deployment(name: &str) -> AuthorizationConfig {
    let path = root().join("deployments").join(format!("{name}.yaml"));
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));
    orbit_utils::yaml::from_str(&text)
        .unwrap_or_else(|error| panic!("parsing {}: {error}", path.display()))
}

/// The GitLab modules and every fake, so one registry binds every variant.
pub fn registry() -> Registry {
    Registry::gitlab()
        .with::<fakes::OidcGroups>()
        .with::<fakes::Zanzibar>()
        .with::<fakes::Cedar>()
        .with::<slack::SlackSignIn>()
        .with::<crate::dag_fakes::AllowedKeys>()
}
