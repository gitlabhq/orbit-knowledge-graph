use std::path::Path;
use std::sync::Arc;

#[test]
fn yaml_plan_shapes() {
    orbit_server_config::query::init(orbit_server_config::AppConfig::embedded_defaults().query);
    integration_testkit::plan_shape::run_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/compiler/plan_shape/fixtures"),
        Arc::new(ontology::Ontology::load_embedded().unwrap()),
    );
}
