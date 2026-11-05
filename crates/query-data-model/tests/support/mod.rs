use std::path::Path;

use query_data_model::Model;

pub fn gitlab() -> Model {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/packages/gitlab");
    let mut files: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    files.sort();
    let sources: Vec<_> = files
        .iter()
        .map(|file| {
            (
                file.display().to_string(),
                std::fs::read_to_string(file).unwrap(),
            )
        })
        .collect();
    Model::from_yaml(
        sources
            .iter()
            .map(|(name, yaml)| (name.as_str(), yaml.as_str())),
    )
    .unwrap()
}
