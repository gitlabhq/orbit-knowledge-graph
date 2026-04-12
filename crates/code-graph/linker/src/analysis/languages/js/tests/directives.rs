use super::helpers::process_fixture_file;
use crate::analysis::languages::js::JsDirective;

#[test]
fn use_server_directive() {
    let processed = process_fixture_file("directives/use-server", "src/action.ts");
    let analysis = processed.js_analysis.expect("should produce JS analysis");
    assert_eq!(analysis.directive, Some(JsDirective::UseServer));
}

#[test]
fn use_client_directive() {
    let processed = process_fixture_file("directives/use-client", "src/page.tsx");
    let analysis = processed.js_analysis.expect("should produce JS analysis");
    assert_eq!(analysis.directive, Some(JsDirective::UseClient));
}

#[test]
fn no_directive() {
    let processed = process_fixture_file("directives/no-directive", "src/value.ts");
    let analysis = processed.js_analysis.expect("should produce JS analysis");
    assert_eq!(analysis.directive, None);
}

#[test]
fn directive_not_in_string_position() {
    let analysis = crate::analysis::languages::js::JsAnalyzer::analyze_file(
        r#"const x = "use server"; export function foo() {}"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert_eq!(
        analysis.directive, None,
        "string literal should not trigger directive"
    );
}
