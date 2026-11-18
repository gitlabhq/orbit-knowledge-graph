use std::path::Path;
use std::sync::Arc;

#[test]
fn yaml_compiler_fixtures() {
    query_settings::init(orbit_server_config::AppConfig::embedded_defaults().query);
    integration_testkit::compiler::run_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/compiler/fixtures"),
        Arc::new(ontology::Ontology::load_embedded().unwrap()),
    );
}
