use std::collections::HashSet;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecallFilter {
    pub kinds: Vec<String>,
}

impl RecallFilter {
    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }
}

pub fn query_alternatives(query: &str) -> Result<Vec<String>, String> {
    let mut seen = HashSet::new();
    let alternatives: Vec<String> = split_top_level(query)
        .into_iter()
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty() && seen.insert(a.to_lowercase()))
        .collect();
    match alternatives.is_empty() {
        true => Err(format!("no usable search terms in query: {query:?}")),
        false => Ok(alternatives),
    }
}

fn split_top_level(query: &str) -> Vec<String> {
    let (mut parts, mut current, mut depth) = (Vec::new(), String::new(), 0i32);
    let mut chars = query.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                current.push(c);
                current.extend(chars.next());
                continue;
            }
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            '|' if depth <= 0 => {
                parts.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    parts.push(current);
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_only_on_top_level_unescaped_bars() {
        assert_eq!(
            query_alternatives(r"Router::new|route\(|func \(p\) (a|b)|a\|b|????").unwrap(),
            [
                "Router::new",
                r"route\(",
                r"func \(p\) (a|b)",
                r"a\|b",
                "????"
            ]
        );
    }

    #[test]
    fn alternatives_preserve_full_queries_and_first_spelling() {
        assert_eq!(
            query_alternatives(" markInSync | MARKINSYNC | prepare_multipart | who calls clone ")
                .unwrap(),
            ["markInSync", "prepare_multipart", "who calls clone"]
        );
    }

    #[test]
    fn only_empty_queries_are_rejected() {
        for query in ["", " ", "|", " | "] {
            assert!(query_alternatives(query).is_err(), "{query:?}");
        }
        assert_eq!(
            query_alternatives("clone||prepare|").unwrap(),
            ["clone", "prepare"]
        );
    }
}
