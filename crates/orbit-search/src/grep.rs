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
    let chars: Vec<char> = query.chars().collect();
    let paired = paired_brackets(&chars);
    let (mut parts, mut current, mut depth) = (Vec::new(), String::new(), 0i32);
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\\' => {
                current.push(c);
                current.extend(chars.get(i + 1));
                i += 2;
                continue;
            }
            '(' | '[' if paired.contains(&i) => depth += 1,
            ')' | ']' if paired.contains(&i) => depth -= 1,
            '|' if depth <= 0 => {
                parts.push(std::mem::take(&mut current));
                i += 1;
                continue;
            }
            _ => {}
        }
        current.push(c);
        i += 1;
    }
    parts.push(current);
    parts
}

fn paired_brackets(chars: &[char]) -> HashSet<usize> {
    let (mut open, mut paired) = (Vec::new(), HashSet::new());
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\\' => i += 1,
            '(' | '[' => open.push(i),
            ')' | ']' => {
                if let Some(start) = open.pop() {
                    paired.insert(start);
                    paired.insert(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    paired
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alternatives_preserve_full_queries_and_first_spelling() {
        assert_eq!(
            query_alternatives(" markInSync | MARKINSYNC | prepare_multipart | who calls clone ")
                .unwrap(),
            ["markInSync", "prepare_multipart", "who calls clone"]
        );
    }

    #[test]
    fn unbalanced_brackets_still_split_on_every_bar() {
        assert_eq!(
            query_alternatives(r"\.route(|Router::new").unwrap(),
            [r"\.route(", "Router::new"]
        );
        assert_eq!(query_alternatives("(a|b)|c").unwrap(), ["(a|b)", "c"]);
        assert_eq!(
            query_alternatives(r"(Router|App)::new|\.route(").unwrap(),
            ["(Router|App)::new", r"\.route("]
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
