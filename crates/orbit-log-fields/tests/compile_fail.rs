#[test]
fn safe_fields_reject_formatted_and_borrowed_values() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
