use orbit_utils::toon::encode;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Fixtures {
    category: String,
    tests: Vec<Fixture>,
}

#[derive(Deserialize)]
struct Fixture {
    input: serde_content::Value<'static>,
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

fn ordered_json(input: &str) -> serde_content::Value<'static> {
    serde_json::from_str(input).unwrap()
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
fn serde_encounter_order_is_independent_of_json_map_order() {
    #[derive(Serialize)]
    struct StructOrder {
        zebra: u8,
        alpha: u8,
        middle: u8,
    }

    let map = r#"{"zebra":1,"alpha":2,"middle":3}"#;
    let expected = "zebra: 1\nalpha: 2\nmiddle: 3";
    assert_eq!(
        encode(&StructOrder {
            zebra: 1,
            alpha: 2,
            middle: 3,
        })
        .unwrap(),
        expected
    );
    assert_eq!(encode(&ordered_json(map)).unwrap(), expected);
    assert_eq!(
        encode(&serde_json::from_str::<Value>(map).unwrap()).unwrap(),
        "alpha: 2\nmiddle: 3\nzebra: 1"
    );
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
        "false: false\nnull: null\ntrue: true"
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
        "a: []\nn: null\no:\ns: \"\""
    );
    assert_eq!(
        encode(&json!([[], {}, null, [{"x": 1}], [[true, false], "x"]])).unwrap(),
        "[5]:\n  - [0]:\n  -\n  - null\n  - [1]:\n    - x: 1\n  - [2]:\n    - [2]: true,false\n    - x"
    );
}

#[test]
fn nested_tables_reorder_columns_not_rows() {
    let value = ordered_json(r#"[{"z":2,"n":{"b":"B","a":1}},{"n":{"a":2,"b":"A"},"z":1}]"#);
    assert_eq!(encode(&value).unwrap(), "[2]{z,n{b,a}}:\n  2,B,1\n  1,A,2");
    assert_eq!(
        encode(&ordered_json(r#"{"b":{"n":{"x":2}},"a":{"n":{"x":1}}}"#)).unwrap(),
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
        encode(&ordered_json(r#"[{"table":[{"x":1}],"next":[]}]"#)).unwrap(),
        "[1]:\n  - table[1]{x}:\n      1\n    next: []"
    );
    assert_eq!(
        encode(&ordered_json(
            r#"[{"table":{"b":{"x":2},"a":{"x":1}},"next":[]}]"#
        ))
        .unwrap(),
        "[1]:\n  - table[2:]{x}:\n      b: 2\n      a: 1\n    next: []"
    );
    assert_eq!(
        encode(&ordered_json(r#"[{"a":{"x":1},"b":{"x":2}},null]"#)).unwrap(),
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

#[test]
fn serde_data_model_normalizes_to_json_shapes() {
    #[derive(Serialize)]
    struct Newtype(u8);

    #[derive(Serialize)]
    enum External {
        Unit,
        Newtype(u8),
        Tuple(u8, bool),
        Struct { zebra: u8, alpha: u8 },
    }

    struct Bytes;
    impl Serialize for Bytes {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_bytes(&[3, 1, 2])
        }
    }

    struct HumanReadable;
    impl Serialize for HumanReadable {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let human_readable = serializer.is_human_readable();
            serializer.serialize_bool(human_readable)
        }
    }

    assert_eq!(encode(&Newtype(7)).unwrap(), "7");
    assert_eq!(encode(&External::Unit).unwrap(), "Unit");
    assert_eq!(encode(&External::Newtype(7)).unwrap(), "Newtype: 7");
    assert_eq!(
        encode(&External::Tuple(1, true)).unwrap(),
        "Tuple[2]: 1,true"
    );
    assert_eq!(
        encode(&External::Struct { zebra: 1, alpha: 2 }).unwrap(),
        "Struct:\n  zebra: 1\n  alpha: 2"
    );
    assert_eq!(encode(&Some(Newtype(9))).unwrap(), "9");
    assert_eq!(encode(&Option::<u8>::None).unwrap(), "null");
    assert_eq!(encode(&()).unwrap(), "null");
    assert_eq!(encode(&Bytes).unwrap(), "[3]: 3,1,2");
    assert_eq!(encode(&HumanReadable).unwrap(), "true");
    assert_eq!(encode(&(i64::MIN as i128)).unwrap(), i64::MIN.to_string());
    assert_eq!(encode(&(u64::MAX as u128)).unwrap(), u64::MAX.to_string());
    assert!(encode(&(i64::MIN as i128 - 1)).is_err());
    assert!(encode(&(u64::MAX as u128 + 1)).is_err());
}

#[test]
fn map_keys_follow_json_compatibility() {
    struct Keys;
    impl Serialize for Keys {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut map = serializer.serialize_map(Some(4))?;
            map.serialize_entry(&true, &1)?;
            map.serialize_entry(&-2_i16, &2)?;
            map.serialize_entry(&'x', &3)?;
            map.serialize_entry(&NewtypeKey(4), &4)?;
            map.end()
        }
    }

    #[derive(Serialize)]
    struct NewtypeKey(u8);

    struct InvalidKey;
    impl Serialize for InvalidKey {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry(&[1_u8, 2], &true)?;
            map.end()
        }
    }

    assert_eq!(encode(&Keys).unwrap(), "true: 1\n\"-2\": 2\nx: 3\n\"4\": 4");
    assert_eq!(
        encode(&InvalidKey).unwrap_err().to_string(),
        "key must be a string"
    );
}

#[test]
fn lexical_boundaries_encode_in_values_headers_and_entry_keys() {
    for (text, expected) in [
        ("05", "\"05\""),
        ("+1", "\"+1\""),
        ("1E-6", "\"1E-6\""),
        ("1.", "1."),
        (".5", ".5"),
        ("1e+", "1e+"),
        ("١٢", "١٢"),
        (" x", "\" x\""),
        ("x ", "\"x \""),
        ("x\t", "\"x\\t\""),
        ("two words", "two words"),
        ("a\u{feff}", "a\u{feff}"),
        ("\u{a0}x\u{a0}", "\u{a0}x\u{a0}"),
    ] {
        assert_eq!(encode(text).unwrap(), expected);
        assert_eq!(
            encode(&json!([text, text])).unwrap(),
            format!("[2]: {expected},{expected}")
        );
        assert_eq!(
            encode(&json!([{"value": text}])).unwrap(),
            format!("[1]{{value}}:\n  {expected}")
        );
    }
    for (key, expected) in [
        ("true", "true"),
        ("a0_.", "a0_."),
        ("05", "\"05\""),
        ("é", "\"é\""),
        ("a:b", "\"a:b\""),
        ("a\n", "\"a\\n\""),
    ] {
        assert_eq!(
            encode(&json!({key: true})).unwrap(),
            format!("{expected}: true")
        );
        assert_eq!(
            encode(&json!([{key: true}])).unwrap(),
            format!("[1]{{{expected}}}:\n  true")
        );
        let keyed_rows = format!(r#"{{{}:{{"v":1}},"other":{{"v":2}}}}"#, json!(key));
        assert_eq!(
            encode(&ordered_json(&keyed_rows)).unwrap(),
            format!("[2:]{{v}}:\n  {expected}: 1\n  other: 2")
        );
    }
}

#[test]
fn every_c0_control_uses_a_permitted_escape() {
    for c in '\0'..='\u{1f}' {
        let escaped = match c {
            '\n' => "\\n".to_owned(),
            '\r' => "\\r".to_owned(),
            '\t' => "\\t".to_owned(),
            _ => format!("\\u{:04x}", u32::from(c)),
        };
        let text = format!("a{c}b");
        assert_eq!(encode(&text).unwrap(), format!("\"a{escaped}b\""));
        assert_eq!(
            encode(&json!({&text: &text})).unwrap(),
            format!("\"a{escaped}b\": \"a{escaped}b\"")
        );
    }
}
