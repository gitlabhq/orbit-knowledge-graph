use integration_tests_codegraph::run_incremental_suite;

const LOCALS: usize = 6000;

#[test]
fn a_method_with_thousands_of_locals_keeps_resolving_each_argument() {
    let mut body = String::new();
    for i in 0..LOCALS {
        body.push_str(&format!("          int a{i} = {i}; k(a{i});\n"));
    }
    body.push_str("          String last = \"\"; k(last); k(a0); k(later); String later = \"\";\n");
    let yaml = format!(
        r#"name: "Java: a very large method resolves arguments without a quadratic scan"

fixtures:
  - path: p/Big.java
    content: |
      package p;

      class Big {{
        String later = "";

        void k(int a) {{}}
        void k(String a) {{}}

        void big() {{
{body}        }}
      }}

tests:
  - name: ints and the string local each reach their own overload
    query: |
      MATCH (c:Definition)-[:CALLS]->(t:Definition)
      WHERE c.name = 'big' AND t.name = 'k'
      RETURN t.signature AS signature, count(c) AS calls
    assert:
      - {{ row_count: 2 }}
      - {{ row: {{ signature: "(int)" }} }}
      - {{ row: {{ signature: "(String)" }} }}
"#
    );
    run_incremental_suite(&yaml);
}
