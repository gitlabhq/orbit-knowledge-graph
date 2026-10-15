#[test]
fn yaml_compiler_contracts() {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/compiler/compiler_contract/fixtures");
    integration_testkit::compiler_contract::run_dir(&directory, super::setup::embedded_ontology());
}
