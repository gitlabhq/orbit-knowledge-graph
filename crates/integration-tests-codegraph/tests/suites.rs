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

// JavaScript / TypeScript
yaml_test!(
    typescript_js_module_resolution,
    "typescript/js_module_resolution.yaml"
);
yaml_test!(
    javascript_commonjs_module_resolution,
    "javascript/commonjs_module_resolution.yaml"
);
yaml_test!(
    javascript_repo_boundary_enforcement,
    "javascript/repo_boundary_enforcement.yaml"
);
yaml_test!(
    javascript_extension_routing,
    "javascript/extension_routing.yaml"
);
yaml_test!(
    javascript_vue_monolith_patterns,
    "javascript/vue_monolith_patterns.yaml"
);
yaml_test!(
    javascript_gitlab_monolith_config_resolution,
    "javascript/gitlab_monolith_config_resolution.yaml"
);
yaml_test!(
    javascript_security_hardening,
    "javascript/security_hardening.yaml"
);
yaml_test!(
    typescript_gitlab_monolith_barrels,
    "typescript/gitlab_monolith_barrels.yaml"
);
yaml_test!(
    typescript_gitlab_monolith_patterns,
    "typescript/gitlab_monolith_patterns.yaml"
);
yaml_test!(
    vue_options_api_resolution,
    "vue/options_api_resolution.yaml"
);
yaml_test!(
    vue_gitlab_monolith_aliases,
    "vue/gitlab_monolith_aliases.yaml"
);
yaml_test!(
    vue_async_and_setup_patterns,
    "vue/async_and_setup_patterns.yaml"
);
yaml_test!(
    typescript_type_only_and_definitions,
    "typescript/type_only_and_definitions.yaml"
);

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
yaml_test!(python_relative_imports, "python/relative_imports.yaml");
yaml_test!(
    python_v1_interfile_resolution,
    "python/v1_interfile_resolution.yaml"
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
yaml_test!(
    java_annotation_references,
    "java/annotation_references.yaml"
);

// Kotlin
yaml_test!(kotlin_call_resolution, "kotlin_resolution.yaml");
yaml_test!(
    kotlin_intrafile_resolution,
    "kotlin/intrafile_resolution.yaml"
);
yaml_test!(
    kotlin_annotation_references,
    "kotlin/annotation_references.yaml"
);
yaml_test!(kotlin_companion_object, "kotlin/companion_object.yaml");

// Go
yaml_test!(go_method_call, "go/method_call.yaml");
yaml_test!(go_struct_embedding, "go/struct_embedding.yaml");
yaml_test!(go_comprehensive, "go/comprehensive.yaml");
yaml_test!(go_interfaces, "go/interfaces.yaml");
yaml_test!(go_multi_return, "go/multi_return.yaml");
yaml_test!(go_nested_calls, "go/nested_calls.yaml");
yaml_test!(go_composite_literals, "go/composite_literals.yaml");
yaml_test!(go_scope_and_branching, "go/scope_and_branching.yaml");
yaml_test!(go_multiple_embedding, "go/multiple_embedding.yaml");
yaml_test!(go_higher_order, "go/higher_order.yaml");
yaml_test!(go_var_reassignment, "go/var_reassignment.yaml");

// Java v1 parity
yaml_test!(java_v1_main_resolution, "java/v1_main_resolution.yaml");
yaml_test!(java_v1_imported_symbols, "java/v1_imported_symbols.yaml");
yaml_test!(java_v1_same_class_name, "java/v1_same_class_name.yaml");
yaml_test!(java_v1_deep_nested, "java/v1_deep_nested.yaml");
yaml_test!(java_v1_stack_safety, "java/v1_stack_safety.yaml");
yaml_test!(java_e2e_weather_app, "java/e2e_weather_app.yaml");

// Kotlin v1 parity
yaml_test!(kotlin_v1_main_resolution, "kotlin/v1_main_resolution.yaml");
yaml_test!(kotlin_v1_super_and_inner, "kotlin/v1_super_and_inner.yaml");
yaml_test!(kotlin_v1_type_inference, "kotlin/v1_type_inference.yaml");
yaml_test!(kotlin_v1_nested_classes, "kotlin/v1_nested_classes.yaml");
yaml_test!(kotlin_v1_same_class_name, "kotlin/v1_same_class_name.yaml");
yaml_test!(
    kotlin_v1_operator_functions,
    "kotlin/v1_operator_functions.yaml"
);
yaml_test!(kotlin_v1_enum_methods, "kotlin/v1_enum_methods.yaml");
yaml_test!(kotlin_v1_extensions, "kotlin/v1_extensions.yaml");

// C
yaml_test!(c_resolution, "c/resolution.yaml");
yaml_test!(c_hardcore, "c/hardcore.yaml");

// C#
yaml_test!(csharp_resolution, "csharp/resolution.yaml");
yaml_test!(
    csharp_advanced_resolution,
    "csharp/advanced_resolution.yaml"
);
yaml_test!(csharp_edge_cases, "csharp/edge_cases.yaml");

// Ruby
yaml_test!(ruby_v1_resolution, "ruby/v1_resolution.yaml");
yaml_test!(ruby_e2e_weather_app, "ruby/e2e_weather_app.yaml");

// Rust
yaml_test!(rust_intrafile_resolution, "rust/intrafile_resolution.yaml");
yaml_test!(rust_interfile_resolution, "rust/interfile_resolution.yaml");
yaml_test!(
    rust_definitions_and_imports,
    "rust/definitions_and_imports.yaml"
);
yaml_test!(
    rust_trait_and_callable_resolution,
    "rust/trait_and_callable_resolution.yaml"
);
yaml_test!(
    rust_module_keywords_resolution,
    "rust/module_keywords_resolution.yaml"
);
yaml_test!(
    rust_workspace_multi_crate_resolution,
    "rust/workspace_multi_crate_resolution.yaml"
);
yaml_test!(
    rust_workspace_globs_and_inheritance,
    "rust/workspace_globs_and_inheritance.yaml"
);
yaml_test!(
    rust_workspace_members_outside_root,
    "rust/workspace_members_outside_root.yaml"
);
yaml_test!(
    rust_nested_manifests_resolution,
    "rust/nested_manifests_resolution.yaml"
);
yaml_test!(
    rust_reexports_and_self_resolution,
    "rust/reexports_and_self_resolution.yaml"
);
yaml_test!(rust_structural_entities, "rust/structural_entities.yaml");
yaml_test!(rust_precision_resolution, "rust/precision_resolution.yaml");
yaml_test!(rust_local_items, "rust/local_items.yaml");
yaml_test!(rust_local_flow_ssa, "rust/local_flow_ssa.yaml");
yaml_test!(rust_mod_rs_resolution, "rust/mod_rs_resolution.yaml");
yaml_test!(
    rust_macro_expansion_resolution,
    "rust/macro_expansion_resolution.yaml"
);
yaml_test!(rust_cfg_gated_resolution, "rust/cfg_gated_resolution.yaml");
yaml_test!(
    rust_semantic_impl_alias_resolution,
    "rust/semantic_impl_alias_resolution.yaml"
);
yaml_test!(
    rust_operator_and_control_flow_resolution,
    "rust/operator_and_control_flow_resolution.yaml"
);
yaml_test!(
    rust_trait_ambiguity_resolution,
    "rust/trait_ambiguity_resolution.yaml"
);
yaml_test!(
    rust_extends_relationships,
    "rust/extends_relationships.yaml"
);
yaml_test!(rust_extends_cross_file, "rust/extends_cross_file.yaml");
yaml_test!(
    rust_field_sensitive_local_flow,
    "rust/field_sensitive_local_flow.yaml"
);
yaml_test!(rust_local_flow_fixes, "rust/local_flow_fixes.yaml");
yaml_test!(
    rust_edge_collection_fixes,
    "rust/edge_collection_fixes.yaml"
);
yaml_test!(rust_ast_structural_fixes, "rust/ast_structural_fixes.yaml");
yaml_test!(
    rust_manifest_resolution_fixes,
    "rust/manifest_resolution_fixes.yaml"
);
yaml_test!(
    rust_workspace_setup_fixes,
    "rust/workspace_setup_fixes.yaml"
);
