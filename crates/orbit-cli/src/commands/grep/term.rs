use anyhow::Result;

use grep_regex::{RegexMatcher, RegexMatcherBuilder};

use super::Options;

use super::rank::assignment;

pub(super) struct Term {
    pub(super) raw: String,
    regex: Option<(regex::Regex, regex::Regex)>,
    pub(super) assign: Option<regex::Regex>,
    /// `-F`: the raw text exactly, so `_`, `-`, and spaces are not optional.
    fixed: bool,
    text: Option<String>,
}

impl Term {
    pub(super) fn is_regex(&self) -> bool {
        self.regex.is_some()
    }

    pub(super) fn parse(raw: &str) -> Self {
        let pattern = raw.contains([
            '\\', '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '^', '$',
        ]);
        let regex = pattern
            .then(|| {
                let line = regex::Regex::new(&format!("(?i){raw}")).ok()?;
                let whole = regex::Regex::new(&format!("(?i)^(?:{raw})$")).ok()?;
                Some((line, whole))
            })
            .flatten();
        let assign = regex.is_none().then(|| assignment(raw)).flatten();
        let text = (pattern && regex.is_none()).then(|| unescape(raw));
        Self {
            raw: raw.to_string(),
            regex,
            assign,
            fixed: false,
            text,
        }
    }

    pub(super) fn literal(&self) -> Self {
        let text = unescape(&self.raw);
        Self {
            raw: self.raw.clone(),
            regex: None,
            assign: assignment(&text),
            fixed: false,
            text: Some(text),
        }
    }

    fn text(&self) -> &str {
        self.text.as_deref().unwrap_or(&self.raw)
    }

    pub(super) fn fixed(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            regex: None,
            assign: assignment(&self.raw),
            fixed: true,
            text: None,
        }
    }

    fn pattern(&self) -> String {
        match self.regex {
            Some(_) => self.raw.clone(),
            None if self.fixed || compact(self.text()).is_empty() => regex::escape(self.text()),
            None => compact(self.text())
                .chars()
                .map(|c| regex::escape(&c.to_string()))
                .collect::<Vec<_>>()
                .join("[-_\\t ]*"),
        }
    }

    pub(super) fn mentions(&self, name: &str) -> bool {
        match &self.regex {
            Some((line, _)) => line.is_match(name),
            None => {
                !compact(self.text()).is_empty() && compact(name).contains(&compact(self.text()))
            }
        }
    }

    pub(super) fn names(&self, name: &str) -> bool {
        match &self.regex {
            Some((_, whole)) => whole.is_match(name),
            None => !compact(name).is_empty() && compact(name) == compact(self.text()),
        }
    }
}

fn unescape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        match (c, chars.peek()) {
            ('\\', Some(next)) if !next.is_alphanumeric() => {
                out.push(*next);
                chars.next();
            }
            _ => out.push(c),
        }
    }
    out
}

fn compact(text: &str) -> String {
    text.chars()
        .filter(|c| !(c.is_whitespace() || *c == '_' || *c == '-'))
        .flat_map(char::to_lowercase)
        .collect()
}

pub(super) fn matcher(alternatives: &[Term], options: &Options) -> Result<RegexMatcher> {
    let pattern = alternatives
        .iter()
        .map(|term| format!("(?:{})", term.pattern()))
        .collect::<Vec<_>>()
        .join("|");
    Ok(RegexMatcherBuilder::new()
        .case_insensitive(true)
        .word(options.word)
        .line_terminator(Some(b'\n'))
        .build(&pattern)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use grep_matcher::Matcher;

    #[test]
    fn escaped_text_that_is_not_a_regex_matches_unescaped() {
        let options = Options::default();
        for query in [r"\.route(", r"foo\(bar"] {
            let found = matcher(&[Term::parse(query)], &options).unwrap();
            let line = query.replace('\\', "");
            assert!(
                found.is_match(format!("x{line}y").as_bytes()).unwrap(),
                "{query}"
            );
        }
        let both = [Term::parse("Router::new"), Term::parse(r"\.route(")];
        let found = matcher(&both, &options).unwrap();
        assert!(found.is_match(b"    .route(").unwrap());
        let fixed = matcher(&[Term::parse(r"\.route(").fixed()], &options).unwrap();
        assert!(!fixed.is_match(b"    .route(").unwrap());
        assert!(fixed.is_match(br"    \.route(").unwrap());
    }
}
