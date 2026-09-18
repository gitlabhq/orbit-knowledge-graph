use std::fmt::Write;

use pest::Parser;
use serde::Serialize;
use serde::ser::Error as _;
use serde_content::{Data, Error, Number, Value};

use lexical::{LexicalParser, Rule};

mod lexical {
    #[derive(pest_derive::Parser)]
    #[grammar = "toon.pest"]
    pub(super) struct LexicalParser;
}

pub fn encode(value: &(impl Serialize + ?Sized)) -> Result<String, Error> {
    let value = serde_content::Serializer::new()
        .human_readable()
        .serialize(value)?;
    let mut out = String::new();
    write_value(&mut out, &Node::from_value(value)?, None, 0, false);
    Ok(out)
}

enum Node {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    Str(String),
    Array(Vec<Node>),
    Object(Vec<(String, Node)>),
}

impl Node {
    fn from_value(value: Value<'_>) -> Result<Self, Error> {
        Ok(match value {
            Value::Unit | Value::Option(None) => Self::Null,
            Value::Option(Some(value)) => Self::from_value(*value)?,
            Value::Bool(value) => Self::Bool(value),
            Value::Number(value) => Self::from_number(value)?,
            Value::Char(value) => Self::Str(value.to_string()),
            Value::String(value) => Self::Str(value.into_owned()),
            Value::Bytes(bytes) => {
                Self::Array(bytes.iter().map(|b| Self::Number((*b).into())).collect())
            }
            Value::Seq(values) | Value::Tuple(values) => Self::array(values)?,
            Value::Map(fields) => Self::Object(
                fields
                    .into_iter()
                    .map(|(key, value)| Ok((Self::key(key)?, Self::from_value(value)?)))
                    .collect::<Result<_, Error>>()?,
            ),
            Value::Struct(value) => Self::from_data(value.data)?,
            Value::Enum(value) => match value.data {
                Data::Unit => Self::Str(value.variant.into_owned()),
                data => Self::Object(vec![(value.variant.into_owned(), Self::from_data(data)?)]),
            },
        })
    }

    fn from_data(data: Data<'_>) -> Result<Self, Error> {
        match data {
            Data::Unit => Ok(Self::Null),
            Data::NewType { value } => Self::from_value(value),
            Data::Tuple { values } => Self::array(values),
            Data::Struct { fields } => Ok(Self::Object(
                fields
                    .into_iter()
                    .map(|(key, value)| Ok((key.into_owned(), Self::from_value(value)?)))
                    .collect::<Result<_, Error>>()?,
            )),
        }
    }

    fn from_number(number: Number) -> Result<Self, Error> {
        let json = match number {
            Number::I8(v) => v.into(),
            Number::I16(v) => v.into(),
            Number::I32(v) => v.into(),
            Number::I64(v) => v.into(),
            Number::U8(v) => v.into(),
            Number::U16(v) => v.into(),
            Number::U32(v) => v.into(),
            Number::U64(v) => v.into(),
            Number::I128(v) => i64::try_from(v)
                .map_err(|_| Error::custom("number out of range"))?
                .into(),
            Number::U128(v) => u64::try_from(v)
                .map_err(|_| Error::custom("number out of range"))?
                .into(),
            Number::F32(v) => {
                return Ok(serde_json::Number::from_f64(v.into()).map_or(Self::Null, Self::Number));
            }
            Number::F64(v) => {
                return Ok(serde_json::Number::from_f64(v).map_or(Self::Null, Self::Number));
            }
            _ => return Err(Error::custom("unsupported number type")),
        };
        Ok(Self::Number(json))
    }

    fn array(values: Vec<Value<'_>>) -> Result<Self, Error> {
        Ok(Self::Array(
            values
                .into_iter()
                .map(Self::from_value)
                .collect::<Result<_, _>>()?,
        ))
    }

    fn key(value: Value<'_>) -> Result<String, Error> {
        match Self::from_value(value)? {
            Self::Str(key) => Ok(key),
            Self::Bool(key) => Ok(key.to_string()),
            Self::Number(key) if !key.is_f64() => Ok(key.to_string()),
            _ => Err(Error::custom("key must be a string")),
        }
    }

    fn object(&self) -> Option<&[(String, Node)]> {
        match self {
            Self::Object(fields) => Some(fields),
            _ => None,
        }
    }

    fn array_items(&self) -> Option<&[Node]> {
        match self {
            Self::Array(values) => Some(values),
            _ => None,
        }
    }

    fn field(&self, name: &str) -> Option<&Node> {
        self.object()?
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value))
    }

    fn primitive(&self) -> bool {
        self.object().is_none() && self.array_items().is_none()
    }
}

fn same_columns(template: &Node, value: &Node) -> bool {
    match (template.object(), value.object()) {
        (Some(fields), Some(row)) => {
            !fields.is_empty()
                && fields.len() == row.len()
                && fields.iter().all(|(key, template)| {
                    value
                        .field(key)
                        .is_some_and(|cell| same_columns(template, cell))
                })
        }
        _ => template.primitive() && value.primitive(),
    }
}

fn table<'a>(mut values: impl Iterator<Item = &'a Node>) -> Option<&'a Node> {
    let first = values.next()?;
    (first.object().is_some()
        && same_columns(first, first)
        && values.all(|v| same_columns(first, v)))
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

fn number(number: &serde_json::Number) -> String {
    let text = number.to_string();
    let Some(value) = number.as_f64().filter(|_| number.is_f64()) else {
        return text;
    };
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

fn line(out: &mut String, depth: usize) {
    if !out.is_empty() {
        out.push('\n');
    }
    out.extend(std::iter::repeat_n(' ', depth * 2));
}

fn write_scalar(out: &mut String, value: &Node) {
    match value {
        Node::Str(text) => out.push_str(&classified(text, Rule::String)),
        Node::Number(value) => out.push_str(&number(value)),
        Node::Bool(value) => write!(out, "{value}").unwrap(),
        Node::Null => out.push_str("null"),
        Node::Array(_) | Node::Object(_) => unreachable!("only primitive cells are emitted"),
    }
}

fn write_fields(out: &mut String, template: &Node) {
    out.push('{');
    for (index, (name, field)) in template.object().unwrap().iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&classified(name, Rule::Key));
        if field.object().is_some() {
            write_fields(out, field);
        }
    }
    out.push('}');
}

fn write_cells(out: &mut String, template: &Node, value: &Node) {
    for (index, (name, template)) in template.object().unwrap().iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let cell = value
            .field(name)
            .expect("table rows contain every template field");
        if template.object().is_some() {
            write_cells(out, template, cell);
        } else {
            write_scalar(out, cell);
        }
    }
}

fn write_item(out: &mut String, value: &Node, depth: usize) {
    line(out, depth);
    out.push('-');
    if let Some(fields) = value.object() {
        for (index, (name, field)) in fields.iter().enumerate() {
            if index == 0 {
                out.push(' ');
            } else {
                line(out, depth + 1);
            }
            write_value(out, field, Some(name), depth + 1, false);
        }
    } else {
        out.push(' ');
        write_value(out, value, None, depth, true);
    }
}

fn write_value(out: &mut String, value: &Node, name: Option<&str>, depth: usize, item: bool) {
    if let Some(name) = name {
        out.push_str(&classified(name, Rule::Key));
    }
    if let Some(fields) = value.object() {
        if fields.len() >= 2
            && let Some(template) = table(fields.iter().map(|(_, value)| value))
        {
            write!(out, "[{}:]", fields.len()).unwrap();
            write_fields(out, template);
            out.push(':');
            for (name, row) in fields {
                line(out, depth + 1);
                write!(out, "{}: ", classified(name, Rule::Key)).unwrap();
                write_cells(out, template, row);
            }
        } else {
            let depth = depth + usize::from(name.is_some());
            if name.is_some() {
                out.push(':');
            }
            for (name, value) in fields {
                line(out, depth);
                write_value(out, value, Some(name), depth, false);
            }
        }
    } else if let Some(values) = value.array_items() {
        if values.is_empty() && !item {
            if name.is_some() {
                out.push_str(": ");
            }
            out.push_str("[]");
            return;
        }
        write!(out, "[{}]", values.len()).unwrap();
        if !item && let Some(template) = table(values.iter()) {
            write_fields(out, template);
            out.push(':');
            for row in values {
                line(out, depth + 1);
                write_cells(out, template, row);
            }
        } else if values.iter().all(Node::primitive) {
            out.push(':');
            for (index, cell) in values.iter().enumerate() {
                out.push(if index == 0 { ' ' } else { ',' });
                write_scalar(out, cell);
            }
        } else {
            out.push(':');
            for value in values {
                write_item(out, value, depth + 1);
            }
        }
    } else {
        if name.is_some() {
            out.push_str(": ");
        }
        write_scalar(out, value);
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
                vec!["a", "Z", "_", "a0_.", "true"],
                vec!["", "0a", ".a", "a-b", "a b", "é"],
            ),
            (
                Rule::String,
                Rule::BareString,
                vec!["word", "two words", "1.", "世界 🎉", "a-b", "!"],
                vec![
                    "",
                    "true",
                    "null",
                    "05",
                    "+05.0E-2",
                    "-",
                    "#",
                    "\u{feff}x",
                    " x",
                    "x ",
                    "a\tb",
                    "a\nb",
                    "a,b",
                    "a:b",
                    "a\"b",
                    "a\\b",
                    "a[b",
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
            for rule in [Rule::Key, Rule::String] {
                let rule_matched = LexicalParser::parse(rule, &format!("a{c}b"))
                    .unwrap()
                    .next()
                    .unwrap()
                    .as_rule();
                assert_eq!(rule_matched, Rule::Quoted);
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
            ("18446744073709551615", true, true),
            ("5e-324", true, true),
            ("", false, false),
            ("1.", false, false),
            (".1", false, false),
            ("1e", false, false),
            (" 1", false, false),
            ("١", false, false),
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
}
