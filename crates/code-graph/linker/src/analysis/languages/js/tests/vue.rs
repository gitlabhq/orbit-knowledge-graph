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
