use std::path::Path;

use query_data_model::{Model, Packages};

pub fn gitlab() -> Model {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/packages");
    let packages = Packages::read_dir(&root).unwrap();
    Model::from_yaml(
        packages
            .sources()
            .iter()
            .filter(|(name, _)| name.starts_with("gitlab/"))
            .map(|(name, yaml)| (name.as_str(), yaml.as_str())),
    )
    .unwrap()
}
