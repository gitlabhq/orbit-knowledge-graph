use integration_tests_codegraph::run_incremental_suite;

const LOCALS: usize = 6000;

#[test]
fn a_method_with_thousands_of_locals_narrows_every_call() {
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
      - {{ row: {{ signature: "(int)", calls: {} }} }}
      - {{ row: {{ signature: "(String)", calls: 2 }} }}
"#,
        LOCALS + 2
    );
    run_incremental_suite(&yaml);
}

const DEPTH: usize = 400;

#[test]
fn a_call_nested_deeper_than_the_scope_walk_keeps_every_overload() {
    let yaml = format!(
        r#"name: "Java: a call nested past the scope walk limit is unknown"

fixtures:
  - path: p/Deep.java
    content: |
      package p;

      class Deep {{
        void k(int a) {{}}
        void k(String a) {{}}

        void shallow() {{ int x = 1; k(x); }}

        void deep() {{
          int x = 1;
          {open}
          k(x);
          {close}
        }}
      }}

tests:
  - name: the shallow call narrows and the deep call keeps both
    query: |
      MATCH (c:Definition)-[:CALLS]->(t:Definition)
      WHERE t.name = 'k'
      RETURN c.name AS caller, t.signature AS signature
      ORDER BY c.name, t.signature
    assert:
      - {{ row_count: 3 }}
      - {{ row: {{ caller: "shallow", signature: "(int)" }} }}
      - {{ row: {{ caller: "deep", signature: "(int)" }} }}
      - {{ row: {{ caller: "deep", signature: "(String)" }} }}
"#,
        open = "{".repeat(DEPTH),
        close = "}".repeat(DEPTH),
    );
    run_incremental_suite(&yaml);
}

const OVERLOADS: usize = 40;

#[test]
fn a_call_to_more_overloads_than_the_fan_out_cap_keeps_its_original_edges() {
    let mut overloads = String::new();
    for i in 0..OVERLOADS {
        overloads.push_str(&format!("        void f(T{i} x) {{}}\n"));
    }
    let yaml = format!(
        r#"name: "Java: a call with too many candidate overloads is not expanded"

fixtures:
  - path: p/Many.java
    content: |
      package p;

      class Many {{
        static Object g() {{ return null; }}
{overloads}
        void call() {{ f(g()); }}
      }}

tests:
  - name: the unknown argument does not fan out to every overload
    query: |
      MATCH (c:Definition)-[:CALLS]->(t:Definition)
      WHERE c.name = 'call' AND t.name = 'f'
      RETURN t.signature AS signature
      LIMIT 100
    assert:
      - {{ row_count: 1 }}
"#
    );
    run_incremental_suite(&yaml);
}
