use std::cell::Cell;
use std::collections::HashMap;

use compiler::{QueryError, Result};
use pest::iterators::Pair;
use serde_json::{Number, Value};

use crate::{MAX_QUERY_BYTES, Parameters, Rule, invalid, unescape, unexpected};

pub(crate) struct Bindings<'a> {
    parameters: &'a Parameters,
    footprints: HashMap<&'a str, usize>,
    expanded: Cell<usize>,
}

impl<'a> Bindings<'a> {
    pub(crate) fn new(parameters: &'a Parameters, footprints: HashMap<&'a str, usize>) -> Self {
        Self {
            parameters,
            footprints,
            expanded: Cell::new(0),
        }
    }

    fn resolve(&self, pair: &Pair<'_, Rule>) -> Result<Value> {
        let key = unescape(
            pair.clone()
                .into_inner()
                .next()
                .expect("parameter has a name")
                .as_str(),
        );
        let value = self
            .parameters
            .get(&key)
            .ok_or_else(|| invalid(pair, &format!("missing parameter ${key}")))?;
        let footprint = self.footprints.get(key.as_str()).copied().unwrap_or(0);
        let expanded = self.expanded.get().saturating_add(footprint);
        if expanded > MAX_QUERY_BYTES {
            return Err(QueryError::LimitExceeded(format!(
                "expanded parameter values must not exceed {MAX_QUERY_BYTES} bytes"
            )));
        }
        self.expanded.set(expanded);
        Ok(value.clone())
    }
}

pub(crate) fn value(pair: Pair<'_, Rule>, bindings: &Bindings<'_>) -> Result<Value> {
    match pair.as_rule() {
        Rule::StringLiteral => string(pair).map(Value::String),
        Rule::BooleanLiteral => Ok(Value::Bool(pair.as_str().eq_ignore_ascii_case("true"))),
        Rule::NumberLiteral | Rule::UnsignedInteger => number(pair),
        Rule::TemporalLiteral => string(
            pair.into_inner()
                .next()
                .expect("temporal literal has a string"),
        )
        .map(Value::String),
        Rule::ListLiteral => pair.into_inner().map(|p| value(p, bindings)).collect(),
        Rule::Parameter => bindings.resolve(&pair),
        _ => Err(unexpected(&pair)),
    }
}

pub(crate) fn string(pair: Pair<'_, Rule>) -> Result<String> {
    let raw = pair.as_str();
    let mut chars = raw[1..raw.len() - 1].chars();
    let mut result = String::new();
    while let Some(c) = chars.next() {
        if c != '\\' {
            result.push(c);
            continue;
        }
        let escape = chars.next().expect("grammar validates escapes");
        match escape {
            '\\' | '\'' | '"' => result.push(escape),
            'n' | 'N' => result.push('\n'),
            'r' | 'R' => result.push('\r'),
            't' | 'T' => result.push('\t'),
            'b' | 'B' => result.push('\u{8}'),
            'f' | 'F' => result.push('\u{c}'),
            'u' | 'U' => {
                let digits = if escape == 'u' { 4 } else { 8 };
                let hex: String = chars.by_ref().take(digits).collect();
                let mut code = u32::from_str_radix(&hex, 16)
                    .map_err(|_| invalid(&pair, "invalid Unicode escape"))?;
                if (0xd800..=0xdbff).contains(&code) && escape == 'u' {
                    if chars.next() != Some('\\') || chars.next() != Some('u') {
                        return Err(invalid(&pair, "high surrogate requires a low surrogate"));
                    }
                    let hex: String = chars.by_ref().take(4).collect();
                    let low = u32::from_str_radix(&hex, 16)
                        .map_err(|_| invalid(&pair, "invalid low surrogate"))?;
                    if !(0xdc00..=0xdfff).contains(&low) {
                        return Err(invalid(&pair, "invalid low surrogate"));
                    }
                    code = 0x10000 + ((code - 0xd800) << 10) + low - 0xdc00;
                }
                result.push(
                    char::from_u32(code).ok_or_else(|| invalid(&pair, "invalid Unicode scalar"))?,
                );
            }
            _ => return Err(unexpected(&pair)),
        }
    }
    Ok(result)
}

fn number(pair: Pair<'_, Rule>) -> Result<Value> {
    let raw = pair.as_str();
    let unsigned = raw.trim_start_matches(['+', '-']);
    let radix = if unsigned.starts_with("0x") {
        Some(16)
    } else if unsigned.starts_with("0o") {
        Some(8)
    } else {
        None
    };
    if let Some(radix) = radix {
        let magnitude = u64::from_str_radix(&unsigned[2..], radix)
            .map_err(|_| invalid(&pair, "integer is out of range"))?;
        if raw.starts_with('-') {
            let signed = i64::try_from(-i128::from(magnitude))
                .map_err(|_| invalid(&pair, "integer is out of range"))?;
            return Ok(Value::from(signed));
        }
        return Ok(Value::from(magnitude));
    }
    let raw = raw.trim_start_matches('+');
    if raw.contains(['.', 'e', 'E']) {
        let number = raw
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .ok_or_else(|| invalid(&pair, "number must be finite and in range"))?;
        Ok(Value::Number(number))
    } else {
        raw.parse::<Number>()
            .map(Value::Number)
            .map_err(|_| invalid(&pair, "integer is out of range"))
    }
}
