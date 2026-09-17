use orbit_utils::toon::encode;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Fixtures {
    category: String,
    tests: Vec<Fixture>,
}

#[derive(Deserialize)]
struct Fixture {
    input: Value,
    expected: String,
    #[serde(default)]
    options: FixtureOptions,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FixtureOptions {
    delimiter: Option<String>,
    indent_size: Option<usize>,
}

#[test]
fn official_default_profile_encoding_fixtures() {
    let mut files: Vec<_> = std::fs::read_dir(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/toon/encode"
    ))
    .unwrap()
    .map(|entry| entry.unwrap().path())
    .collect();
    files.sort();
    let (mut passed, mut skipped) = (0, 0);
    for file in files {
        let fixtures: Fixtures = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
        assert_eq!(fixtures.category, "encode");
        for (index, fixture) in fixtures.tests.into_iter().enumerate() {
            let delimiter = fixture.options.delimiter.as_deref().unwrap_or(",");
            let indent = fixture.options.indent_size.unwrap_or(2);
            assert!(matches!(delimiter, "," | "\t" | "|"));
            assert!(indent > 0);
            if delimiter != "," || indent != 2 {
                skipped += 1;
                continue;
            }
            assert_eq!(
                encode(&fixture.input).unwrap(),
                fixture.expected,
                "{}[{index}]",
                file.display()
            );
            passed += 1;
        }
    }
    assert_eq!((passed, skipped), (156, 23));
}

#[test]
fn float_wire_tokens_never_saturate_integer_boundaries() {
    let signed = 2_f64.powi(63);
    let unsigned = 2_f64.powi(64);
    for (value, expected) in [
        (signed.next_down(), "9223372036854775000"),
        (signed, "9223372036854776000"),
        (signed.next_up(), "9223372036854778000"),
        (unsigned.next_down(), "18446744073709550000"),
        (unsigned, "18446744073709552000"),
        (unsigned.next_up(), "18446744073709556000"),
        (-signed, "-9223372036854776000"),
        (-unsigned, "-18446744073709552000"),
        (-0.0, "0"),
        (1.0, "1"),
        (1.25, "1.25"),
        (-0.125, "-0.125"),
        (1e-6, "0.000001"),
        (1e-7, "1e-7"),
        (1e20, "100000000000000000000"),
        (1e21, "1e+21"),
        (f64::from_bits(1), "5e-324"),
        (f64::MAX, "1.7976931348623157e+308"),
    ] {
        assert_eq!(encode(&value).unwrap(), expected, "{value:?}");
        assert_eq!(encode(&vec![value]).unwrap(), format!("[1]: {expected}"));
        assert_eq!(
            encode(&json!({"nested": [{"value": value}]})).unwrap(),
            format!("nested[1]{{value}}:\n  {expected}")
        );
        assert_eq!(expected.parse::<f64>().unwrap(), value);
    }
}

#[test]
fn integer_digits_remain_exact_at_host_limits() {
    for (value, expected) in [
        (json!(i64::MIN), "-9223372036854775808"),
        (json!(i64::MAX), "9223372036854775807"),
        (json!(u64::MAX), "18446744073709551615"),
        (json!(1_u64 << 63), "9223372036854775808"),
    ] {
        assert_eq!(encode(&value).unwrap(), expected);
        assert_eq!(
            encode(&json!({"value": value})).unwrap(),
            format!("value: {expected}")
        );
        assert_eq!(serde_json::from_str::<Value>(expected).unwrap(), value);
    }
}

#[test]
fn canonical_range_neighbors_preserve_float_value() {
    for value in [
        1e-6_f64.next_down(),
        1e-6_f64.next_up(),
        1e21_f64.next_down(),
        1e21_f64.next_up(),
    ] {
        let text = encode(&value).unwrap();
        assert_eq!(text.parse::<f64>().unwrap(), value);
        assert_eq!(text.contains('e'), !(1e-6..1e21).contains(&value));
        assert!(!text.ends_with(".0"));
    }
}

#[test]
fn strings_keys_and_controls_are_unambiguous() {
    assert_eq!(
        encode(&json!({"true": true, "false": false, "null": null})).unwrap(),
        "true: true\nfalse: false\nnull: null"
    );
    for (value, expected) in [
        ("\u{8}\u{c}\0\u{1f}", r#""\u0008\u000c\u0000\u001f""#),
        ("\n\r\t\\\"", r#""\n\r\t\\\"""#),
        ("世界 🎉", "世界 🎉"),
        ("١", "١"),
        ("+01e-2", "\"+01e-2\""),
        ("#comment", "\"#comment\""),
        ("\u{feff}data", "\"\u{feff}data\""),
    ] {
        assert_eq!(encode(value).unwrap(), expected);
    }
    assert_eq!(
        encode(&json!({"é": [{"x:y": "#"}]})).unwrap(),
        "\"é\"[1]{\"x:y\"}:\n  \"#\""
    );
}

#[test]
fn empty_forms_and_nested_arrays_remain_distinct() {
    assert_eq!(encode(&json!({})).unwrap(), "");
    assert_eq!(encode(&json!([])).unwrap(), "[]");
    assert_eq!(encode(&Value::Null).unwrap(), "null");
    assert_eq!(
        encode(&json!({"n": null, "a": [], "o": {}, "s": ""})).unwrap(),
        "n: null\na: []\no:\ns: \"\""
    );
    assert_eq!(
        encode(&json!([[], {}, null, [{"x": 1}], [[true, false], "x"]])).unwrap(),
        "[5]:\n  - [0]:\n  -\n  - null\n  - [1]:\n    - x: 1\n  - [2]:\n    - [2]: true,false\n    - x"
    );
}

#[test]
fn nested_tables_reorder_columns_not_rows() {
    let value: Value =
        serde_json::from_str(r#"[{"z":2,"n":{"b":"B","a":1}},{"n":{"a":2,"b":"A"},"z":1}]"#)
            .unwrap();
    assert_eq!(encode(&value).unwrap(), "[2]{z,n{b,a}}:\n  2,B,1\n  1,A,2");
    assert_eq!(
        encode(&json!({"b": {"n": {"x": 2}}, "a": {"n": {"x": 1}}})).unwrap(),
        "[2:]{n{x}}:\n  b: 2\n  a: 1"
    );
    for cell in [json!({}), json!([]), Value::Null] {
        assert_eq!(
            encode(&json!([{"n": {"x": 1}}, {"n": cell}]))
                .unwrap()
                .lines()
                .next(),
            Some("[2]:")
        );
    }
}

#[test]
fn list_object_first_field_has_logical_field_depth() {
    assert_eq!(
        encode(&json!([{"table": [{"x": 1}], "next": []}])).unwrap(),
        "[1]:\n  - table[1]{x}:\n      1\n    next: []"
    );
    assert_eq!(
        encode(&json!([{"table": {"b": {"x": 2}, "a": {"x": 1}}, "next": []}])).unwrap(),
        "[1]:\n  - table[2:]{x}:\n      b: 2\n      a: 1\n    next: []"
    );
    assert_eq!(
        encode(&json!([{"a": {"x": 1}, "b": {"x": 2}}, null])).unwrap(),
        "[2]:\n  - a:\n      x: 1\n    b:\n      x: 2\n  - null"
    );
}

#[test]
fn serde_normalization_and_errors_propagate() {
    #[derive(Serialize)]
    struct Payload {
        value: f64,
        optional: Option<bool>,
    }
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            encode(&Payload {
                value,
                optional: None
            })
            .unwrap(),
            "value: null\noptional: null"
        );
    }
    struct Failing;
    impl Serialize for Failing {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("intentional serialization error"))
        }
    }
    #[derive(Serialize)]
    struct InvalidPayload {
        field: Failing,
    }
    assert_eq!(
        encode(&InvalidPayload { field: Failing })
            .unwrap_err()
            .to_string(),
        "intentional serialization error"
    );
    assert!(encode(&u128::MAX).is_err());
}
