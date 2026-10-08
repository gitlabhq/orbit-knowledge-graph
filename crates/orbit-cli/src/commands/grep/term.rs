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

#[cfg(test)]
mod tests {
    use grep_matcher::Matcher;

    use super::*;

    fn finds(term: &str, text: &str, options: &Options) -> bool {
        matcher(&[Term::parse(term)], options)
            .unwrap()
            .is_match(text.as_bytes())
            .unwrap()
    }

    #[test]
    fn regex_terms_match_like_ripgrep_and_bad_patterns_fall_back_to_literals() {
        let plain = Options::default();
        assert!(finds(r"\bCveDetail\b", "x := CveDetail{}", &plain));
        assert!(!finds(r"\bCveDetail\b", "CveDetails", &plain));
        assert!(finds(r"type .* struct\{\}", "type key struct{}", &plain));
        assert!(finds(r"route\(", ".route(\"/x\")", &plain));
        assert!(finds("^rand", "rand = \"0.10\"", &plain));
        assert!(!finds("^rand", "x.rand = 1", &plain));
        assert!(finds("mark_in_sync", "markInSync()", &plain));
        assert!(Term::parse("on.*Login").names("onLogin"));
        let literal = |raw: &str, text: &str| {
            matcher(&[Term::parse(raw).literal()], &plain)
                .unwrap()
                .is_match(text.as_bytes())
                .unwrap()
        };
        assert!(literal("route(", ".route(\"/x\")"));
        assert!(literal("????", "x = '????'"));
        let word = Options {
            word: true,
            ..Options::default()
        };
        assert!(finds("detail", "a detail here", &word));
        assert!(!finds("detail", "CveDetails", &word));
    }
}
