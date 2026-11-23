use std::path::Path;
use std::sync::Arc;

#[test]
fn yaml_compiler_fixtures() {
    orbit_server_config::query::init(orbit_server_config::AppConfig::embedded_defaults().query);
    integration_testkit::compiler::run_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/compiler/fixtures"),
        Arc::new(ontology::Ontology::load_embedded().unwrap()),
    );
}

#[test]
fn yaml_schema_fixtures() {
    integration_testkit::compiler::run_schema_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/compiler/schema"),
        &ontology::Ontology::load_embedded().unwrap(),
    );
}
