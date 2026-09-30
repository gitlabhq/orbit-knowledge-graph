use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub enum Expression {
    Atom(String),
    List(Vec<Self>),
}

impl Expression {
    pub fn atom(value: impl ToString) -> Self {
        Self::Atom(value.to_string())
    }

    pub fn node(name: &str, children: impl IntoIterator<Item = Self>) -> Self {
        Self::List(std::iter::once(Self::atom(name)).chain(children).collect())
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Atom(atom) => {
                if atom.is_empty()
                    || atom
                        .chars()
                        .any(|c| c.is_whitespace() || matches!(c, '(' | ')' | '"' | '\\'))
                {
                    write!(
                        output,
                        "{}",
                        serde_json::to_string(atom).map_err(|_| fmt::Error)?
                    )
                } else {
                    output.write_str(atom)
                }
            }
            Self::List(children) => {
                output.write_str("(")?;
                for (index, child) in children.iter().enumerate() {
                    if index > 0 {
                        output.write_str(" ")?;
                    }
                    write!(output, "{child}")?;
                }
                output.write_str(")")
            }
        }
    }
}

pub fn parse(text: &str) -> Result<Expression, String> {
    fn expression(text: &str, position: &mut usize) -> Result<Expression, String> {
        while text
            .as_bytes()
            .get(*position)
            .is_some_and(u8::is_ascii_whitespace)
        {
            *position += 1;
        }
        let start = *position;
        match text.as_bytes().get(start) {
            Some(b'(') => {
                *position += 1;
                let mut children = Vec::new();
                loop {
                    while text
                        .as_bytes()
                        .get(*position)
                        .is_some_and(u8::is_ascii_whitespace)
                    {
                        *position += 1;
                    }
                    if text.as_bytes().get(*position) == Some(&b')') {
                        *position += 1;
                        return Ok(Expression::List(children));
                    }
                    children.push(expression(text, position)?);
                }
            }
            Some(b'"') => {
                *position += 1;
                let mut escaped = false;
                while let Some(byte) = text.as_bytes().get(*position) {
                    *position += 1;
                    if *byte == b'"' && !escaped {
                        return serde_json::from_str::<String>(&text[start..*position])
                            .map(Expression::Atom)
                            .map_err(|error| error.to_string());
                    }
                    escaped = *byte == b'\\' && !escaped;
                }
                Err("unterminated string".into())
            }
            Some(b')') | None => Err(format!("expected expression at byte {start}")),
            Some(_) => {
                while text
                    .as_bytes()
                    .get(*position)
                    .is_some_and(|byte| !byte.is_ascii_whitespace() && !matches!(byte, b'(' | b')'))
                {
                    *position += 1;
                }
                Ok(Expression::atom(&text[start..*position]))
            }
        }
    }
    let mut position = 0;
    let parsed = expression(text, &mut position)?;
    if !text[position..].trim().is_empty() {
        return Err("trailing input".into());
    }
    Ok(parsed)
}

fn matches(pattern: &Expression, actual: &Expression) -> bool {
    fn sequence(pattern: &[Expression], actual: &[Expression]) -> bool {
        match pattern.split_first() {
            None => actual.is_empty(),
            Some((Expression::Atom(atom), rest)) if atom == "..." => {
                (0..=actual.len()).any(|offset| sequence(rest, &actual[offset..]))
            }
            Some((first, rest)) => actual.split_first().is_some_and(|(value, remaining)| {
                matches(first, value) && sequence(rest, remaining)
            }),
        }
    }
    match (pattern, actual) {
        (Expression::Atom(atom), _) if atom == "_" => true,
        (Expression::Atom(left), Expression::Atom(right)) => left == right,
        (Expression::List(left), Expression::List(right)) => sequence(left, right),
        _ => false,
    }
}

pub fn contains(actual: &Expression, pattern: &Expression) -> bool {
    matches(pattern, actual)
        || match actual {
            Expression::List(children) => children.iter().any(|child| contains(child, pattern)),
            Expression::Atom(_) => false,
        }
}
