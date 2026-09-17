use std::fmt::Write;
use std::sync::LazyLock;

use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Number, Value};

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

fn key(text: &str) -> String {
    let mut chars = text.chars();
    if chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        text.to_owned()
    } else {
        quoted(text)
    }
}

fn string(text: &str) -> String {
    static NUMERIC: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\A[+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\z").unwrap());
    if text.is_empty()
        || matches!(text, "true" | "false" | "null")
        || text.starts_with([' ', '\t', '-', '#', '\u{feff}'])
        || text.ends_with([' ', '\t'])
        || text
            .chars()
            .any(|c| c <= '\u{1f}' || ":\"\\[]{},".contains(c))
        || NUMERIC.is_match(text)
    {
        quoted(text)
    } else {
        text.to_owned()
    }
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
    let Some((mantissa, exponent)) = text.split_once('e') else {
        return text.strip_suffix(".0").unwrap_or(&text).to_owned();
    };
    let exponent: i32 = exponent.parse().unwrap();
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.trim_start_matches('-');
    let point = mantissa.find('.').unwrap_or(mantissa.len()) as i32 + exponent;
    let digits = mantissa.replace('.', "");
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
