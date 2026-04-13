use super::helpers::process_fixture_file;
use crate::analysis::languages::js::{JsAnalyzer, JsDefKind};

#[test]
fn default_export_class_binding() {
    let processed = process_fixture_file("analysis/default-export-class", "src/main.ts");
    let analysis = processed.js_analysis.expect("should produce JS analysis");
    let bar = analysis
        .defs
        .iter()
        .find(|d| d.fqn == "Bar")
        .expect("should extract Bar");
    let default_binding = analysis
        .module_info
        .exports
        .get("default")
        .expect("default export");
    assert_eq!(default_binding.definition_range, Some(bar.range));
}

#[test]
fn typed_variable_annotation() {
    let processed = process_fixture_file("analysis/typed-variable", "src/main.ts");
    let analysis = processed.js_analysis.expect("should produce JS analysis");
    let x = analysis
        .defs
        .iter()
        .find(|d| d.name == "x")
        .expect("should find x");
    assert_eq!(x.type_annotation.as_deref(), Some("string"));
}

#[test]
fn function_parameters_not_definitions() {
    let analysis = JsAnalyzer::analyze_file(
        "function foo(a: string, b: number) { return a + b; }",
        "test.ts",
        "test.ts",
    )
    .unwrap();
    // Only foo should be a definition; params should be filtered
    let def_names: Vec<_> = analysis.defs.iter().map(|d| d.name.as_str()).collect();
    assert!(def_names.contains(&"foo"));
    assert!(
        analysis
            .defs
            .iter()
            .all(|d| d.kind != JsDefKind::Function || d.name == "foo"),
        "only foo should be a Function definition, not parameters: {def_names:?}"
    );
}

#[test]
fn class_with_methods() {
    let analysis = JsAnalyzer::analyze_file(
        r#"
class Greeter {
    greet() { return "hello"; }
    farewell() { return "bye"; }
}
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();

    assert!(
        analysis
            .defs
            .iter()
            .any(|d| d.fqn == "Greeter" && d.kind == JsDefKind::Class)
    );
    assert!(analysis.defs.iter().any(|d| d.fqn == "Greeter::greet"));
    assert!(analysis.defs.iter().any(|d| d.fqn == "Greeter::farewell"));
    assert_eq!(
        analysis
            .defs
            .iter()
            .filter(|d| matches!(d.kind, JsDefKind::Method { .. }))
            .count(),
        2
    );
}

#[test]
fn interface_and_type_alias() {
    let analysis = JsAnalyzer::analyze_file(
        "interface Foo { bar: string; }\ntype Baz = Foo & { qux: number; };",
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(
        analysis
            .defs
            .iter()
            .any(|d| d.kind == JsDefKind::Interface && d.name == "Foo")
    );
    assert!(
        analysis
            .defs
            .iter()
            .any(|d| d.kind == JsDefKind::TypeAlias && d.name == "Baz")
    );
}

#[test]
fn enum_and_enum_members() {
    let analysis =
        JsAnalyzer::analyze_file("enum Color { Red, Green, Blue }", "test.ts", "test.ts").unwrap();
    assert!(
        analysis
            .defs
            .iter()
            .any(|d| d.kind == JsDefKind::Enum && d.name == "Color")
    );
    assert_eq!(
        analysis
            .defs
            .iter()
            .filter(|d| d.kind == JsDefKind::EnumMember)
            .count(),
        3
    );
}

#[test]
fn catch_variable_not_a_definition() {
    let analysis = JsAnalyzer::analyze_file(
        "try { throw 1; } catch (e) { console.log(e); }",
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(
        analysis.defs.iter().all(|d| d.name != "e"),
        "catch variable should not be a definition"
    );
}

#[test]
fn nested_function_fqn() {
    let analysis = JsAnalyzer::analyze_file(
        "function outer() { function inner() {} }",
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(analysis.defs.iter().any(|d| d.fqn == "outer"));
    assert_eq!(
        analysis
            .defs
            .iter()
            .find(|d| d.name == "inner")
            .unwrap()
            .fqn,
        "inner",
        "inner function should have flat FQN (JS function scopes are not FQN namespaces)"
    );
}

#[test]
fn local_variables_get_scoped_unique_fqns() {
    let analysis = JsAnalyzer::analyze_file(
        r#"
class Widget {
    render() {
        const canEdit = 1;
        if (canEdit) {
            const canEdit = 2;
            return canEdit;
        }
        return canEdit;
    }
}
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();

    let can_edit_fqns: Vec<_> = analysis
        .defs
        .iter()
        .filter(|d| d.name == "canEdit" && d.kind == JsDefKind::Variable)
        .map(|d| d.fqn.as_str())
        .collect();

    assert_eq!(can_edit_fqns.len(), 2);
    assert!(
        can_edit_fqns
            .iter()
            .all(|fqn| fqn.starts_with("Widget::render::canEdit@")),
    );
    assert_ne!(can_edit_fqns[0], can_edit_fqns[1]);
}

#[test]
fn exported_flag_set_correctly() {
    let analysis = JsAnalyzer::analyze_file(
        "export function pub_fn() {}\nfunction priv_fn() {}",
        "test.ts",
        "test.ts",
    )
    .unwrap();
    let pub_fn = analysis.defs.iter().find(|d| d.name == "pub_fn").unwrap();
    let priv_fn = analysis.defs.iter().find(|d| d.name == "priv_fn").unwrap();
    assert!(pub_fn.is_exported);
    assert!(!priv_fn.is_exported);
}

#[test]
fn skips_minified_long_lines() {
    let long_line = "x".repeat(60_000);
    let source = format!("const x = \"{long_line}\";");
    let result = JsAnalyzer::analyze_file(&source, "test.ts", "test.ts");
    assert!(result.is_err(), "should skip files with lines > 50K bytes");
}
