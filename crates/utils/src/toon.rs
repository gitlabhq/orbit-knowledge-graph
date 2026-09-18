use std::fmt::Write;

use pest::Parser;
use serde::Serialize;
use serde_json::{Map, Number, Value};

use lexical::{LexicalParser, Rule};

mod lexical {
    #[derive(pest_derive::Parser)]
    #[grammar = "toon.pest"]
    pub(super) struct LexicalParser;
}

pub fn encode(value: &(impl Serialize + ?Sized)) -> Result<String, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    let mut encoder = Encoder(String::new());
    encoder.value(&value, None, 0, false);
    Ok(encoder.0)
}

fn primitive(value: &Value) -> bool {
    !value.is_object() && !value.is_array()
}

fn same_columns(template: &Value, value: &Value) -> bool {
    match (template, value) {
        (Value::Object(fields), Value::Object(row)) => {
            !fields.is_empty()
                && fields.len() == row.len()
                && fields
                    .iter()
                    .all(|(key, field)| row.get(key).is_some_and(|cell| same_columns(field, cell)))
        }
        _ => primitive(template) && primitive(value),
    }
}

fn table<'a>(mut values: impl Iterator<Item = &'a Value>) -> Option<&'a Value> {
    let first = values.next()?;
    (first.is_object() && same_columns(first, first) && values.all(|v| same_columns(first, v)))
        .then_some(first)
}

fn quoted(text: &str) -> String {
    let mut out = String::from("\"");
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0'..='\u{1f}' => write!(out, "\\u{:04x}", u32::from(c)).unwrap(),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

fn classified(text: &str, rule: Rule) -> String {
    match LexicalParser::parse(rule, text)
        .unwrap()
        .next()
        .unwrap()
        .as_rule()
    {
        Rule::BareKey | Rule::BareString => text.to_owned(),
        Rule::Quoted => quoted(text),
        _ => unreachable!("key and string rules classify every input"),
    }
}

fn key(text: &str) -> String {
    classified(text, Rule::Key)
}

fn string(text: &str) -> String {
    classified(text, Rule::String)
}

fn number(number: &Number) -> String {
    let text = number.to_string();
    if !number.is_f64() {
        return text;
    }
    let value = number.as_f64().unwrap();
    if value == 0.0 {
        return "0".into();
    }
    if !(1e-6..1e21).contains(&value.abs()) {
        return text;
    }
    let (mut negative, mut integer, mut fraction, mut exponent) = (false, "", "", None);
    for part in LexicalParser::parse(Rule::Number, &text).unwrap() {
        match part.as_rule() {
            Rule::Negative => negative = true,
            Rule::Integer => integer = part.as_str(),
            Rule::Fraction => fraction = part.as_str(),
            Rule::Exponent => exponent = Some(part.as_str().parse::<i32>().unwrap()),
            Rule::EOI => {}
            _ => unreachable!("number rules only emit numeric components"),
        }
    }
    let Some(exponent) = exponent else {
        return text.strip_suffix(".0").unwrap_or(&text).to_owned();
    };
    let point = integer.len() as i32 + exponent;
    let digits = format!("{integer}{fraction}");
    let mut out = if negative {
        "-".to_owned()
    } else {
        String::new()
    };
    if point <= 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-point) as usize));
        out.push_str(&digits);
    } else if point as usize >= digits.len() {
        out.push_str(&digits);
        out.extend(std::iter::repeat_n('0', point as usize - digits.len()));
    } else {
        let (integer, fraction) = digits.split_at(point as usize);
        write!(out, "{integer}.{fraction}").unwrap();
    }
    if out.contains('.') {
        out.truncate(out.trim_end_matches('0').trim_end_matches('.').len());
    }
    out
}

struct Encoder(String);

impl Encoder {
    fn line(&mut self, depth: usize) {
        if !self.0.is_empty() {
            self.0.push('\n');
        }
        self.0.extend(std::iter::repeat_n(' ', depth * 2));
    }

    fn scalar(&mut self, value: &Value) {
        self.0.push_str(&match value {
            Value::String(text) => string(text),
            Value::Number(n) => number(n),
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".into(),
            _ => unreachable!("only primitive cells are emitted"),
        });
    }

    fn fields(&mut self, template: &Value) {
        self.0.push('{');
        for (i, (name, field)) in template.as_object().unwrap().iter().enumerate() {
            if i > 0 {
                self.0.push(',');
            }
            self.0.push_str(&key(name));
            if field.is_object() {
                self.fields(field);
            }
        }
        self.0.push('}');
    }

    fn cells(&mut self, template: &Value, value: &Value) {
        for (i, (name, field)) in template.as_object().unwrap().iter().enumerate() {
            if i > 0 {
                self.0.push(',');
            }
            if field.is_object() {
                self.cells(field, &value[name]);
            } else {
                self.scalar(&value[name]);
            }
        }
    }

    fn object(&mut self, object: &Map<String, Value>, depth: usize) {
        for (name, value) in object {
            self.line(depth);
            self.value(value, Some(name), depth, false);
        }
    }

    fn item(&mut self, value: &Value, depth: usize) {
        self.line(depth);
        self.0.push('-');
        if let Value::Object(object) = value {
            for (i, (name, field)) in object.iter().enumerate() {
                if i == 0 {
                    self.0.push(' ');
                } else {
                    self.line(depth + 1);
                }
                self.value(field, Some(name), depth + 1, false);
            }
        } else {
            self.0.push(' ');
            self.value(value, None, depth, true);
        }
    }

    fn value(&mut self, value: &Value, name: Option<&str>, depth: usize, item: bool) {
        if let Some(name) = name {
            self.0.push_str(&key(name));
        }
        match value {
            Value::Object(object) => {
                if object.len() >= 2
                    && let Some(template) = table(object.values())
                {
                    write!(self.0, "[{}:]", object.len()).unwrap();
                    self.fields(template);
                    self.0.push(':');
                    for (name, row) in object {
                        self.line(depth + 1);
                        write!(self.0, "{}: ", key(name)).unwrap();
                        self.cells(template, row);
                    }
                } else {
                    if name.is_some() {
                        self.0.push(':');
                    }
                    self.object(object, depth + usize::from(name.is_some()));
                }
            }
            Value::Array(array) => {
                if array.is_empty() && !item {
                    if name.is_some() {
                        self.0.push_str(": ");
                    }
                    self.0.push_str("[]");
                    return;
                }
                write!(self.0, "[{}]", array.len()).unwrap();
                if !item && let Some(template) = table(array.iter()) {
                    self.fields(template);
                    self.0.push(':');
                    for row in array {
                        self.line(depth + 1);
                        self.cells(template, row);
                    }
                } else if array.iter().all(primitive) {
                    self.0.push(':');
                    for (i, cell) in array.iter().enumerate() {
                        self.0.push(if i == 0 { ' ' } else { ',' });
                        self.scalar(cell);
                    }
                } else {
                    self.0.push(':');
                    for value in array {
                        self.item(value, depth + 1);
                    }
                }
            }
            _ => {
                if name.is_some() {
                    self.0.push_str(": ");
                }
                self.scalar(value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LexicalParser, Rule};
    use pest::Parser;

    #[test]
    fn grammar_classifies_complete_keys_and_strings() {
        for (rule, bare, accepted, rejected) in [
            (
                Rule::Key,
                Rule::BareKey,
                vec!["a", "Z", "_", "a0_.", "true", "false", "null"],
                vec!["", "0a", ".a", "a-b", "a b", "a\n", "a ", " a", "é", "a١"],
            ),
            (
                Rule::String,
                Rule::BareString,
                vec![
                    "word",
                    "two words",
                    "truex",
                    "False",
                    "NULL",
                    "+",
                    "1.",
                    ".5",
                    "1e",
                    "1e+",
                    "1.2.3",
                    "١",
                    "世界 🎉",
                    "a#",
                    "a-b",
                    "a\u{feff}",
                    "\u{a0}x\u{a0}",
                    "a\u{7f}",
                    "!",
                    "/",
                    ";",
                    "Z",
                    "^",
                    "z",
                    "|",
                    "~",
                    "\u{10ffff}",
                ],
                vec![
                    "",
                    "true",
                    "false",
                    "null",
                    "05",
                    "+05.0E-2",
                    "-",
                    "-x",
                    "#",
                    "#x",
                    "\u{feff}x",
                    " x",
                    "x ",
                    "\tx",
                    "x\t",
                    "a\tb",
                    "a\nb",
                    "a,b",
                    "a:b",
                    "a\"b",
                    "a\\b",
                    "a[b",
                    "a]b",
                    "a{b",
                    "a}b",
                ],
            ),
        ] {
            for (inputs, expected) in [(accepted, bare), (rejected, Rule::Quoted)] {
                for input in inputs {
                    let mut pairs = LexicalParser::parse(rule, input).unwrap();
                    let classified = pairs.next().unwrap();
                    assert_eq!(classified.as_rule(), expected, "{rule:?}: {input:?}");
                    assert_eq!(classified.as_str(), input);
                    assert!(pairs.next().is_none());
                }
            }
        }
        for c in '\0'..='\u{1f}' {
            let input = format!("a{c}b");
            for rule in [Rule::Key, Rule::String] {
                assert_eq!(
                    LexicalParser::parse(rule, &input)
                        .unwrap()
                        .next()
                        .unwrap()
                        .as_rule(),
                    Rule::Quoted,
                    "{input:?}"
                );
            }
        }
    }

    #[test]
    fn grammar_distinguishes_numeric_strings_from_json_numbers() {
        for (input, numeric_like, number) in [
            ("0", true, true),
            ("-0", true, true),
            ("05", true, false),
            ("+1", true, false),
            ("-05.1", true, false),
            ("1.25e-6", true, true),
            ("-1.25E+21", true, true),
            ("1e01", true, true),
            ("18446744073709551615", true, true),
            ("5e-324", true, true),
            ("1.7976931348623157e+308", true, true),
            ("", false, false),
            ("1.", false, false),
            (".1", false, false),
            ("1e", false, false),
            ("1e+", false, false),
            ("1e-", false, false),
            (" 1", false, false),
            ("1 ", false, false),
            ("1\n", false, false),
            ("١", false, false),
            ("1.١", false, false),
            ("NaN", false, false),
            ("Infinity", false, false),
            ("1x", false, false),
        ] {
            assert_eq!(
                LexicalParser::parse(Rule::NumericLike, input).is_ok(),
                numeric_like,
                "{input:?}"
            );
            assert_eq!(
                LexicalParser::parse(Rule::Number, input).is_ok(),
                number,
                "{input:?}"
            );
        }
    }

    #[test]
    fn grammar_decomposes_numbers_without_converting_digits() {
        for (input, expected) in [
            (
                "-1.25E+21",
                vec![
                    (Rule::Negative, "-"),
                    (Rule::Integer, "1"),
                    (Rule::Fraction, "25"),
                    (Rule::Exponent, "+21"),
                ],
            ),
            (
                "0.000001",
                vec![(Rule::Integer, "0"), (Rule::Fraction, "000001")],
            ),
            (
                "18446744073709551615",
                vec![(Rule::Integer, "18446744073709551615")],
            ),
            (
                "5e-324",
                vec![(Rule::Integer, "5"), (Rule::Exponent, "-324")],
            ),
        ] {
            let parts: Vec<_> = LexicalParser::parse(Rule::Number, input)
                .unwrap()
                .filter(|part| part.as_rule() != Rule::EOI)
                .map(|part| (part.as_rule(), part.as_str()))
                .collect();
            assert_eq!(parts, expected, "{input:?}");
        }
    }
}
