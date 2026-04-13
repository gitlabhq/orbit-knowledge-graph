use super::helpers::setup_js_fixture_pipeline;
use crate::analysis::languages::js::{JsAnalyzer, JsCallTarget, JsDefKind};
use tracing_test::traced_test;

#[traced_test]
#[tokio::test]
async fn options_api_creates_virtual_class() {
    let setup = setup_js_fixture_pipeline("cross-file/vue-options-api").await;
    assert!(
        setup.has_definition("MyComponent"),
        "should create virtual class for the component"
    );
}

#[traced_test]
#[tokio::test]
async fn options_api_extracts_methods() {
    let setup = setup_js_fixture_pipeline("cross-file/vue-options-api").await;
    assert!(setup.has_definition("MyComponent::handleClick"));
    assert!(setup.has_definition("MyComponent::submitForm"));
}

#[traced_test]
#[tokio::test]
async fn options_api_this_method_resolves() {
    let setup = setup_js_fixture_pipeline("cross-file/vue-options-api").await;
    let calls = setup.find_calls_from_method("MyComponent::handleClick");
    assert!(
        calls.iter().any(|fqn| fqn == "MyComponent::submitForm"),
        "this.submitForm() should resolve in Options API"
    );
}

#[test]
fn options_api_component_name_from_name_property() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default { name: 'MyWidget', methods: { click() {} } };"#,
        "test.vue.js",
        "test.vue.js",
    )
    .unwrap();
    assert!(
        analysis
            .defs
            .iter()
            .any(|d| d.fqn == "MyWidget" && d.kind == JsDefKind::Class)
    );
    assert!(analysis.defs.iter().any(|d| d.fqn == "MyWidget::click"));
}

#[test]
fn options_api_component_name_from_filename() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default { methods: { save() {} } };"#,
        "UserProfile.vue.js",
        "UserProfile.vue.js",
    )
    .unwrap();
    // Falls back to filename stem when no `name` property
    assert!(analysis.defs.iter().any(|d| d.fqn.ends_with("::save")));
}

#[test]
fn options_api_computed_properties_extracted() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default {
            name: 'Comp',
            computed: {
                fullName() { return this.first + this.last; }
            }
        };"#,
        "test.vue.js",
        "test.vue.js",
    )
    .unwrap();
    assert!(analysis.defs.iter().any(|d| d.fqn == "Comp::fullName"));
}

#[test]
fn options_api_this_resolves_to_computed() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default {
            name: 'Comp',
            computed: { label() { return "x"; } },
            methods: { render() { return this.label(); } }
        };"#,
        "test.vue.js",
        "test.vue.js",
    )
    .unwrap();
    assert!(analysis.calls.iter().any(|c| matches!(
        &c.callee,
        JsCallTarget::ThisMethod { method_name, resolved_fqn: Some(fqn), .. }
        if method_name == "label" && fqn == "Comp::label"
    )));
}

#[test]
fn vue_default_export_has_binding() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default {
            name: 'TestWidget',
            methods: { click() {} }
        };"#,
        "test.vue.js",
        "test.vue.js",
    )
    .unwrap();

    // The default export should have a binding pointing to the virtual class
    let default_binding = analysis
        .module_info
        .exports
        .get("default")
        .expect("should have default export binding");
    assert!(default_binding.is_default, "should be marked as default");
    assert_eq!(
        default_binding.local_fqn, "TestWidget",
        "default export local_fqn should be the component name, not 'default'"
    );
    assert!(
        default_binding.definition_range.is_some(),
        "default export should have a definition_range pointing to the virtual class"
    );
}

#[test]
fn wrapped_options_api_creates_virtual_class() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default normalizeRender({
            name: 'WrappedWidget',
            methods: { click() {} }
        });"#,
        "wrapped.vue.js",
        "wrapped.vue.js",
    )
    .unwrap();

    assert!(analysis.defs.iter().any(|d| d.fqn == "WrappedWidget"));
    assert!(
        analysis
            .defs
            .iter()
            .any(|d| d.fqn == "WrappedWidget::click")
    );
}

#[test]
fn props_only_wrapped_component_still_creates_component_definition() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default defineComponent({
            name: 'PropsOnly',
            props: { message: String }
        });"#,
        "props_only.vue.js",
        "props_only.vue.js",
    )
    .unwrap();

    assert!(
        analysis
            .defs
            .iter()
            .any(|d| d.fqn == "PropsOnly" && d.kind == JsDefKind::Class)
    );
}

#[test]
fn wrapped_component_extracts_setup_method() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default normalizeRender({
            name: 'SetupWidget',
            setup() { return {}; }
        });"#,
        "setup_widget.vue.js",
        "setup_widget.vue.js",
    )
    .unwrap();

    assert!(analysis.defs.iter().any(|d| d.fqn == "SetupWidget::setup"));
}

#[test]
fn non_vue_export_default_object_ignored() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default { key: "value" };"#,
        "config.ts",
        "config.ts",
    )
    .unwrap();
    // No methods/computed = no virtual class created
    assert!(
        analysis.defs.iter().all(|d| d.kind != JsDefKind::Class),
        "plain object export should not create virtual class"
    );
}

#[test]
fn non_vue_export_default_object_with_vue_like_keys_is_ignored() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default {
            methods: { save() {} },
            data() { return {}; }
        };"#,
        "component.ts",
        "component.ts",
    )
    .unwrap();

    assert!(
        analysis.defs.iter().all(|d| d.kind != JsDefKind::Class),
        "non-.vue files should not infer Vue components from plain object exports alone"
    );
}

#[test]
fn explicit_define_component_in_non_vue_file_is_detected() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default defineComponent({
            name: 'Widget',
            methods: { save() {} }
        });"#,
        "component.ts",
        "component.ts",
    )
    .unwrap();

    assert!(
        analysis
            .defs
            .iter()
            .any(|d| d.fqn == "Widget" && d.kind == JsDefKind::Class)
    );
    assert!(analysis.defs.iter().any(|d| d.fqn == "Widget::save"));
}

#[test]
fn wrapped_props_only_component_requires_explicit_name() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default defineComponent({
            props: { message: String }
        });"#,
        "component.ts",
        "component.ts",
    )
    .unwrap();

    assert!(
        analysis.defs.iter().all(|d| d.kind != JsDefKind::Class),
        "contract-only wrapped components should require an explicit name"
    );
}

#[test]
fn wrapped_component_requires_single_object_argument() {
    let analysis = JsAnalyzer::analyze_file(
        r#"const options = { methods: { save() {} } };
export default defineComponent(options, {
  methods: { save() {} }
});"#,
        "component.ts",
        "component.ts",
    )
    .unwrap();

    assert!(
        analysis.defs.iter().all(|d| d.kind != JsDefKind::Class),
        "wrappers should only unwrap a single direct options object argument"
    );
}

#[test]
fn invalid_vue_option_value_shapes_are_ignored() {
    let analysis = JsAnalyzer::analyze_file(
        r#"export default {
            methods: 1,
            setup: true
        };"#,
        "component.vue.js",
        "component.vue.js",
    )
    .unwrap();

    assert!(
        analysis.defs.iter().all(|d| d.kind != JsDefKind::Class),
        "invalid value shapes should not classify a file as a Vue component"
    );
}
