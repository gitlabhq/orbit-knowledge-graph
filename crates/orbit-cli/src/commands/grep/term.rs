use anyhow::Result;

use grep_regex::{RegexMatcher, RegexMatcherBuilder};

use super::Options;

use super::rank::assignment;

pub(super) struct Term {
    pub(super) raw: String,
    regex: Option<(regex::Regex, regex::Regex)>,
    pub(super) assign: Option<regex::Regex>,
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
        Self {
            raw: raw.to_string(),
            regex,
            assign,
        }
    }

    pub(super) fn literal(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            regex: None,
            assign: assignment(&self.raw),
        }
    }

    fn pattern(&self) -> String {
        match self.regex {
            Some(_) => self.raw.clone(),
            None => compact(&self.raw)
                .chars()
                .map(|c| regex::escape(&c.to_string()))
                .collect::<Vec<_>>()
                .join("[-_\\t ]*"),
        }
    }

    pub(super) fn names(&self, name: &str) -> bool {
        match &self.regex {
            Some((_, whole)) => whole.is_match(name),
            None => compact(name) == compact(&self.raw),
        }
    }
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
