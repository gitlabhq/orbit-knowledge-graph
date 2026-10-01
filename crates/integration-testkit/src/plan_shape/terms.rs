use std::collections::BTreeMap;

pub type Captures = BTreeMap<String, Term>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Term {
    Atom(String),
    Group(char, Vec<Self>),
}

pub fn parse(text: &str) -> Result<Vec<Term>, String> {
    fn sequence(
        chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
        end: Option<char>,
    ) -> Result<Vec<Term>, String> {
        let mut terms = Vec::new();
        while let Some(c) = chars.next() {
            match c {
                c if c.is_whitespace() => {}
                ')' | ']' => {
                    if Some(c) != end {
                        return Err(format!("unexpected '{c}'"));
                    }
                    return Ok(terms);
                }
                '(' | '[' => terms.push(Term::Group(
                    c,
                    sequence(chars, Some(if c == '(' { ')' } else { ']' }))?,
                )),
                '\'' | '"' => {
                    let mut value = String::from(c);
                    let mut escaped = false;
                    loop {
                        let next = chars.next().ok_or("unterminated string")?;
                        value.push(next);
                        if next == c && !escaped {
                            break;
                        }
                        escaped = next == '\\' && !escaped;
                    }
                    terms.push(Term::Atom(value));
                }
                c if c.is_alphanumeric() || matches!(c, '_' | '$') => {
                    let mut value = String::from(c);
                    while chars
                        .peek()
                        .is_some_and(|c| c.is_alphanumeric() || *c == '_')
                    {
                        value.push(chars.next().unwrap());
                    }
                    terms.push(Term::Atom(value));
                }
                '.' if chars.peek() == Some(&'.') => {
                    chars.next();
                    if chars.peek() == Some(&'.') {
                        chars.next();
                        terms.push(Term::Atom("...".into()));
                    } else {
                        terms.push(Term::Atom("..".into()));
                    }
                }
                c => terms.push(Term::Atom(c.to_string())),
            }
        }
        if end.is_some() {
            return Err("unclosed expression group".into());
        }
        Ok(terms)
    }
    sequence(&mut text.chars().peekable(), None)
}

pub fn match_text(pattern: &str, actual: &str, captures: &Captures) -> Option<Captures> {
    match_terms(pattern, actual, captures, false)
}

pub fn match_prefix(pattern: &str, actual: &str, captures: &Captures) -> Option<Captures> {
    match_terms(pattern, actual, captures, true)
}

fn match_terms(pattern: &str, actual: &str, captures: &Captures, prefix: bool) -> Option<Captures> {
    fn sequence(pattern: &[Term], actual: &[Term], captures: &mut Captures) -> bool {
        if pattern.len() != actual.len() {
            return false;
        }
        pattern
            .iter()
            .zip(actual)
            .all(|(pattern, actual)| match (pattern, actual) {
                (Term::Atom(name), actual) if name.starts_with('$') => {
                    if let Some(bound) = captures.get(name) {
                        bound == actual
                    } else {
                        captures.insert(name.clone(), actual.clone());
                        true
                    }
                }
                (Term::Atom(hole), _) if hole == "?" => true,
                (Term::Group(left, pattern), Term::Group(right, actual)) => {
                    left == right && sequence(pattern, actual, captures)
                }
                _ => pattern == actual,
            })
    }
    let pattern = parse(pattern).ok()?;
    let actual = parse(actual).ok()?;
    let actual = if prefix {
        actual.get(..pattern.len())?
    } else {
        &actual
    };
    let mut result = captures.clone();
    sequence(&pattern, actual, &mut result).then_some(result)
}

pub fn variables(text: &str) -> Result<Vec<String>, String> {
    fn collect(terms: &[Term], names: &mut Vec<String>) -> Result<(), String> {
        for term in terms {
            match term {
                Term::Atom(name) if name.starts_with('$') => {
                    if name.len() == 1 {
                        return Err("capture needs a name after '$'".into());
                    }
                    names.push(name.clone());
                }
                Term::Group(_, children) => collect(children, names)?,
                _ => {}
            }
        }
        Ok(())
    }
    let mut names = Vec::new();
    collect(&parse(text)?, &mut names)?;
    Ok(names)
}
