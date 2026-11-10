use serde::Deserialize;

use crate::Packages;

const OVERLAYS: &str = concat!(env!("ORBIT_CONFIG_DIR"), "/seeds/overlays");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Edit {
    file: String,
    find: String,
    replace: String,
}

/// The embedded packages with each `(file, find, replace)` edit applied to the single
/// occurrence of `find` in the package file `file`.
pub fn patched<'a>(edits: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>) -> Packages {
    let mut sources = Packages::embedded().sources().clone();
    for (file, find, replace) in edits {
        let source = sources
            .get_mut(file)
            .unwrap_or_else(|| panic!("no package file {file}"));
        assert_eq!(
            source.matches(find).count(),
            1,
            "{file} must contain {find:?} exactly once"
        );
        *source = source.replacen(find, replace, 1);
    }
    Packages::new(sources)
}

/// The embedded packages with the edits of `config/seeds/overlays/<name>.yaml` applied.
pub fn overlay(name: &str) -> Packages {
    let path = format!("{OVERLAYS}/{name}.yaml");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading overlay {path}: {error}"));
    let edits: Vec<Edit> = orbit_utils::yaml::from_str(&text)
        .unwrap_or_else(|error| panic!("parsing overlay {path}: {error}"));
    patched(edits.iter().map(|edit| {
        (
            edit.file.as_str(),
            edit.find.as_str(),
            edit.replace.as_str(),
        )
    }))
}

/// The embedded packages plus the demo packages of `config/demo`, whose connections no server
/// opens.
pub fn with_demo() -> Packages {
    let root = concat!(env!("ORBIT_CONFIG_DIR"), "/demo");
    let demo = Packages::read_dir(std::path::Path::new(root))
        .unwrap_or_else(|error| panic!("reading {root}: {error}"));
    let mut sources = Packages::embedded().sources().clone();
    sources.extend(demo.sources().clone());
    Packages::new(sources)
}
