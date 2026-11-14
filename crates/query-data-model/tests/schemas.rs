use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use query_data_model::schemas;
use serde_json::Value;

fn config() -> &'static Path {
    Path::new(env!("ORBIT_CONFIG_DIR"))
}

fn files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        match path.is_dir() {
            true => files.extend(self::files(&path)),
            false => files.push(path),
        }
    }
    files
}

#[test]
fn committed_schemas_match_the_spec_types() {
    let dir = config().join("schemas/model");
    let generated = schemas();
    for (name, text) in &generated {
        let committed = fs::read_to_string(dir.join(name)).unwrap_or_default();
        assert!(
            committed == *text,
            "{name} is out of date; run `mise model:schema:generate`"
        );
    }
    let mut committed: Vec<String> = files(&dir)
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    committed.sort();
    let generated: Vec<&str> = generated.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(committed, generated);
}

/// The validation script picks a storage file's schema by its name, since a fragment that adds
/// entities to a store declared elsewhere does not name the store's engine kind.
#[test]
fn every_storage_file_is_named_after_its_store_kind() {
    let roots = [
        config().join("model"),
        config().join("demo"),
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../integration-testkit/model-variants/platform"),
    ];
    let storage: Vec<(PathBuf, Value)> = roots
        .iter()
        .flat_map(|root| files(root))
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .map(|path| {
            let fragment: Value =
                orbit_utils::yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
            (path, fragment)
        })
        .filter(|(_, fragment)| fragment["kind"] == "storage")
        .collect();
    let mut kinds = BTreeMap::new();
    for (path, fragment) in &storage {
        if let Some(kind) = fragment["engine"]["kind"].as_str() {
            let store = fragment["store"].as_str().unwrap();
            let declared = kinds.entry(store).or_insert(kind);
            assert_eq!(
                *declared,
                kind,
                "{} redeclares store {store}",
                path.display()
            );
        }
    }
    assert!(!storage.is_empty());
    for (path, fragment) in &storage {
        let store = fragment["store"].as_str().unwrap();
        assert_eq!(
            path.file_stem().and_then(|stem| stem.to_str()),
            kinds.get(store).copied(),
            "{} holds entities of store {store}",
            path.display()
        );
    }
}
