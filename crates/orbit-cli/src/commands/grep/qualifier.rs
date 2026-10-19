use super::Options;

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Parsed {
    pub(super) query: Option<String>,
    pub(super) kinds: Vec<String>,
    pub(super) globs: Vec<String>,
    pub(super) types: Vec<String>,
    pub(super) targets: Vec<String>,
}

fn tokens(query: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for c in query.chars() {
        match c {
            '"' => quoted = !quoted,
            c if c.is_whitespace() && !quoted => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn glob_for(value: &str) -> String {
    match value.contains(['*', '?', '[']) {
        true => value.to_string(),
        false => format!("*{value}*"),
    }
}

pub(super) fn parse(query: &str) -> Option<Parsed> {
    let mut parsed = Parsed::default();
    let (mut terms, mut words) = (Vec::new(), Vec::new());
    let mut any = false;
    for token in tokens(query) {
        let Some((name, value)) = token.split_once(':') else {
            words.push(token);
            continue;
        };
        if value.is_empty() || value.starts_with(':') || value.starts_with('/') {
            words.push(token);
            continue;
        }
        match name.to_ascii_lowercase().as_str() {
            "is" => parsed.kinds.push(value.to_string()),
            "symbol" | "def" => {
                parsed.kinds.push("definition".to_string());
                terms.push(value.to_string());
            }
            "file" | "path" => parsed.globs.push(glob_for(value)),
            "repo" => parsed.targets.push(value.to_string()),
            _ => {
                words.push(token);
                continue;
            }
        }
        any = true;
    }
    if !any {
        return None;
    }
    if !words.is_empty() {
        terms.insert(0, words.join(" "));
    }
    parsed.query = match terms.is_empty() {
        true => Some(".".to_string()),
        false => Some(terms.join("|")),
    };
    Some(parsed)
}

/// Whether an argument is a qualifier such as `is:def` or `repo:g/p` rather than a path.
fn is_qualifier(word: &str) -> bool {
    word.split_once(':').is_some_and(|(name, value)| {
        !value.is_empty()
            && matches!(
                name.to_ascii_lowercase().as_str(),
                "is" | "symbol" | "def" | "file" | "path" | "repo"
            )
    })
}

/// Agents often write `orbit grep 'words' is:def`, with the qualifier outside the quotes. Such
/// arguments join the query unless a path by that name exists.
fn absorb_trailing(query: Option<String>, paths: &mut Vec<String>) -> Option<String> {
    let (qualifiers, rest): (Vec<String>, Vec<String>) = std::mem::take(paths)
        .into_iter()
        .partition(|path| is_qualifier(path) && !std::path::Path::new(path).exists());
    *paths = rest;
    if qualifiers.is_empty() {
        return query;
    }
    let mut words: Vec<String> = query.into_iter().collect();
    words.extend(qualifiers);
    Some(words.join(" "))
}

pub(super) fn apply(
    query: Option<String>,
    kinds: &mut Vec<String>,
    options: &mut Options,
    paths: &mut Vec<String>,
) -> Option<String> {
    if options.fixed {
        return query;
    }
    let query = absorb_trailing(query, paths);
    let Some(text) = query.as_deref() else {
        return query;
    };
    let Some(parsed) = parse(text) else {
        return query;
    };
    kinds.extend(parsed.kinds);
    options.globs.extend(parsed.globs);
    options.types.extend(parsed.types);
    paths.extend(parsed.targets);
    if parsed.query.as_deref() == Some(".") && kinds.is_empty() {
        options.output = super::Output::Files;
    }
    parsed.query
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(list: &[&str]) -> Vec<String> {
        list.iter().map(|item| item.to_string()).collect()
    }

    #[test]
    fn qualifiers_pick_kinds_files_and_targets() {
        let parsed = parse("is:mr rate limit path:src repo:g/p").unwrap();
        assert_eq!(parsed.query.as_deref(), Some("rate limit"));
        assert_eq!(parsed.kinds, strings(&["mr"]));
        assert_eq!(parsed.globs, strings(&["*src*"]));
        assert_eq!(parsed.targets, strings(&["g/p"]));
        assert_eq!(
            crate::remote::grep::scope_for("mrs"),
            Some("merge_requests")
        );
        assert_eq!(parse("type:go grep"), None);
        let mut paths = strings(&["is:def", "src/"]);
        assert_eq!(
            absorb_trailing(Some("x".into()), &mut paths).as_deref(),
            Some("x is:def")
        );
        assert_eq!(paths, strings(&["src/"]));
    }
}
