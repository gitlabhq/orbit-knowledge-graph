use integration_testkit::graph_validator::run_yaml_suite;

macro_rules! yaml_test {
    ($name:ident, $path:expr) => {
        #[tokio::test]
        async fn $name() {
            run_yaml_suite(include_str!(concat!(
                "../../../integration-testkit/src/graph_validator/fixtures/",
                $path
            )))
            .await;
        }
    };
}

// ── Structural ──────────────────────────────────────────────────
yaml_test!(structural_invariants, "structural.yaml");
yaml_test!(containment_hierarchy, "containment.yaml");

// ── Java ────────────────────────────────────────────────────────
yaml_test!(java_call_resolution, "java_resolution.yaml");
yaml_test!(java_intrafile_resolution, "java/intrafile_resolution.yaml");
yaml_test!(java_interfile_resolution, "java/interfile_resolution.yaml");
