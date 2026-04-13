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

// ── Python ──────────────────────────────────────────────────────
yaml_test!(python_simple_call, "python/simple_call.yaml");
yaml_test!(python_self_method_call, "python/self_method_call.yaml");
yaml_test!(python_cross_file_import, "python/cross_file_import.yaml");
yaml_test!(python_nested_functions, "python/nested_functions.yaml");
yaml_test!(python_lambda_call, "python/lambda_call.yaml");
yaml_test!(python_decorated_function, "python/decorated_function.yaml");
yaml_test!(python_class_methods, "python/class_methods.yaml");
yaml_test!(python_nested_classes, "python/nested_classes.yaml");
yaml_test!(python_recursive_call, "python/recursive_call.yaml");
yaml_test!(python_class_inheritance, "python/class_inheritance.yaml");
yaml_test!(
    python_comprehensive_definitions,
    "python/comprehensive_definitions.yaml"
);
yaml_test!(
    python_comprehensive_imports,
    "python/comprehensive_imports.yaml"
);
yaml_test!(python_call_resolution, "python_resolution.yaml");

// ── Java ────────────────────────────────────────────────────────
yaml_test!(java_call_resolution, "java_resolution.yaml");

// ── Kotlin ──────────────────────────────────────────────────────
yaml_test!(kotlin_call_resolution, "kotlin_resolution.yaml");
