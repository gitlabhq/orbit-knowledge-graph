use compiler::planning::explain::SExpression;

pub fn parse(text: &str) -> Result<SExpression, String> {
    fn expression(text: &str, position: &mut usize) -> Result<SExpression, String> {
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
                        return Ok(SExpression::List(children));
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
                            .map(SExpression::Atom)
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
                Ok(SExpression::Atom(text[start..*position].into()))
            }
        }
    }

    let mut position = 0;
    let result = expression(text, &mut position)?;
    if !text[position..].trim().is_empty() {
        return Err("trailing input after expression".into());
    }
    Ok(result)
}

fn matches(pattern: &SExpression, actual: &SExpression) -> bool {
    fn sequence(pattern: &[SExpression], actual: &[SExpression]) -> bool {
        match pattern.split_first() {
            None => actual.is_empty(),
            Some((SExpression::Atom(atom), rest)) if atom == "..." => {
                (0..=actual.len()).any(|offset| sequence(rest, &actual[offset..]))
            }
            Some((first, rest)) => actual.split_first().is_some_and(|(value, remaining)| {
                matches(first, value) && sequence(rest, remaining)
            }),
        }
    }

    match (pattern, actual) {
        (SExpression::Atom(atom), _) if atom == "_" => true,
        (SExpression::Atom(left), SExpression::Atom(right)) => left == right,
        (SExpression::List(left), SExpression::List(right)) => sequence(left, right),
        _ => false,
    }
}

pub fn contains(actual: &SExpression, pattern: &SExpression) -> bool {
    matches(pattern, actual)
        || match actual {
            SExpression::List(children) => children.iter().any(|child| contains(child, pattern)),
            SExpression::Atom(_) => false,
        }
}
