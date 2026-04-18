use integration_tests_codegraph::run_yaml_suite;

macro_rules! yaml_test {
    ($name:ident, $path:expr) => {
        #[tokio::test]
        async fn $name() {
            run_yaml_suite(include_str!(concat!("../fixtures/", $path))).await;
        }
    };
}

// Structural
yaml_test!(structural_invariants, "structural.yaml");
yaml_test!(containment_hierarchy, "containment.yaml");

// Python
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
yaml_test!(python_scope_resolution, "python/scope_resolution.yaml");
yaml_test!(python_async_functions, "python/async_functions.yaml");
yaml_test!(python_higher_order, "python/higher_order.yaml");
yaml_test!(python_class_patterns, "python/class_patterns.yaml");
yaml_test!(python_match_statement, "python/match_statement.yaml");
yaml_test!(
    python_static_classmethod,
    "python/static_classmethod_calls.yaml"
);
yaml_test!(python_multi_file_imports, "python/multi_file_imports.yaml");
yaml_test!(
    python_intrafile_resolution,
    "python/intrafile_resolution.yaml"
);
yaml_test!(
    python_interfile_resolution,
    "python/interfile_resolution.yaml"
);
yaml_test!(
    python_unresolved_and_edge_cases,
    "python/unresolved_and_edge_cases.yaml"
);
yaml_test!(
    python_conditional_bindings,
    "python/conditional_bindings.yaml"
);
yaml_test!(
    python_chained_and_callable,
    "python/chained_and_callable.yaml"
);
yaml_test!(python_import_extraction, "python/import_extraction.yaml");
yaml_test!(python_interfile_imports, "python/interfile_imports.yaml");
yaml_test!(
    python_aliased_and_partial,
    "python/aliased_and_partial.yaml"
);
yaml_test!(python_wildcard_import, "python/wildcard_import.yaml");
yaml_test!(python_callable_objects, "python/callable_objects.yaml");
yaml_test!(python_instance_attrs, "python/instance_attrs.yaml");
yaml_test!(
    python_return_type_inference,
    "python/return_type_inference.yaml"
);
yaml_test!(
    python_cross_file_return_type,
    "python/cross_file_return_type.yaml"
);
yaml_test!(python_call_resolution, "python_resolution.yaml");
yaml_test!(python_type_flow, "python/type_flow.yaml");
yaml_test!(
    python_decorator_references,
    "python/decorator_references.yaml"
);

// Java
yaml_test!(java_call_resolution, "java_resolution.yaml");
yaml_test!(java_intrafile_resolution, "java/intrafile_resolution.yaml");
yaml_test!(java_interfile_resolution, "java/interfile_resolution.yaml");
yaml_test!(java_type_flow, "java/type_flow.yaml");
yaml_test!(
    java_generic_type_stripping,
    "java/generic_type_stripping.yaml"
);
yaml_test!(java_cross_file_type_flow, "java/cross_file_type_flow.yaml");

// Kotlin
yaml_test!(kotlin_call_resolution, "kotlin_resolution.yaml");
yaml_test!(
    kotlin_intrafile_resolution,
    "kotlin/intrafile_resolution.yaml"
);

// Examples (custom pipelines)
yaml_test!(ruby_custom_pipeline, "examples/ruby_custom_pipeline.yaml");
