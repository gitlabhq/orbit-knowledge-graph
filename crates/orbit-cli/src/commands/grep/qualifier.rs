use super::Options;

const KINDS_BY_NAME: &[(&[&str], &str)] = &[
    (&["branch", "branches"], "Branch"),
    (
        &["mr", "mrs", "mergerequest", "merge_request", "pr"],
        "MergeRequest",
    ),
    (
        &["issue", "issues", "workitem", "work_item", "epic"],
        "WorkItem",
    ),
    (&["commit", "commits"], "Commit"),
    (&["comment", "comments", "note", "notes"], "Note"),
    (&["milestone", "milestones"], "Milestone"),
    (&["user", "users"], "User"),
    (&["project", "projects"], "Project"),
    (&["group", "groups"], "Group"),
    (
        &[
            "def",
            "defs",
            "definition",
            "definitions",
            "symbol",
            "symbols",
        ],
        "definition",
    ),
    (&["file", "files"], "File"),
    (
        &["dir", "dirs", "directory", "directories", "folder"],
        "Directory",
    ),
];
const KINDS: &[&str] = &["is", "type", "kind"];
const SYMBOLS: &[&str] = &["symbol", "def", "definition"];
const FILES: &[&str] = &["file", "path"];
const LANGUAGES: &[&str] = &["lang", "language"];
const EXTENSIONS: &[&str] = &["ext", "extension"];
const TARGETS: &[&str] = &["repo", "in"];
const ANY_LINE: &str = ".";

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

fn named(name: &str, names: &[&str]) -> bool {
    names.iter().any(|known| known.eq_ignore_ascii_case(name))
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
        if named(name, KINDS) {
            let kind = KINDS_BY_NAME
                .iter()
                .find(|(names, _)| named(value, names))
                .map_or(value, |(_, kind)| kind);
            parsed.kinds.push(kind.to_string());
        } else if named(name, SYMBOLS) {
            parsed.kinds.push("definition".to_string());
            terms.push(value.to_string());
        } else if named(name, FILES) {
            parsed.globs.push(glob_for(value));
        } else if named(name, EXTENSIONS) {
            parsed
                .globs
                .push(format!("*.{}", value.trim_start_matches('.')));
        } else if named(name, LANGUAGES) {
            parsed.types.push(value.to_string());
        } else if named(name, TARGETS) {
            parsed.targets.push(value.to_string());
        } else {
            words.push(token);
            continue;
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
        true => Some(ANY_LINE.to_string()),
        false => Some(terms.join("|")),
    };
    Some(parsed)
}

pub(super) fn apply(
    query: Option<String>,
    kinds: &mut Vec<String>,
    options: &mut Options,
    paths: &mut Vec<String>,
) -> Option<String> {
    let Some(text) = query.as_deref().filter(|_| !options.fixed) else {
        return query;
    };
    let Some(parsed) = parse(text) else {
        return query;
    };
    kinds.extend(parsed.kinds);
    options.globs.extend(parsed.globs);
    options.types.extend(parsed.types);
    paths.extend(parsed.targets);
    if parsed.query.as_deref() == Some(ANY_LINE) && kinds.is_empty() {
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
    fn is_picks_kinds_and_free_text_is_the_search() {
        let parsed = parse("is:mr rate limit").unwrap();
        assert_eq!(parsed.query.as_deref(), Some("rate limit"));
        assert_eq!(parsed.kinds, strings(&["MergeRequest"]));
        let parsed = parse("is:issue type:MR approval rules").unwrap();
        assert_eq!(parsed.query.as_deref(), Some("approval rules"));
        assert_eq!(parsed.kinds, strings(&["WorkItem", "MergeRequest"]));
        assert_eq!(
            parse("is:branch approval").unwrap().kinds,
            strings(&["Branch"])
        );
        assert_eq!(
            parse("is:def grep").unwrap().kinds,
            strings(&["definition"])
        );
        assert_eq!(parse("is:Class grep").unwrap().kinds, strings(&["Class"]));
        assert_eq!(parse("type:go grep").unwrap().kinds, strings(&["go"]));
    }

    #[test]
    fn filters_narrow_plain_words() {
        let parsed = parse("approval state file:mrutils lang:go repo:gitlab-org/cli").unwrap();
        assert_eq!(parsed.query.as_deref(), Some("approval state"));
        assert_eq!(parsed.globs, strings(&["*mrutils*"]));
        assert_eq!(parsed.types, strings(&["go"]));
        assert_eq!(parsed.targets, strings(&["gitlab-org/cli"]));
        let parsed = parse("symbol:PrintMRApprovalState file:*_test.go").unwrap();
        assert_eq!(parsed.query.as_deref(), Some("PrintMRApprovalState"));
        assert_eq!(parsed.kinds, strings(&["definition"]));
        assert_eq!(parsed.globs, strings(&["*_test.go"]));
    }

    #[test]
    fn a_file_qualifier_alone_lists_matching_files() {
        let parsed = parse("file:mrutils").unwrap();
        assert_eq!(parsed.query.as_deref(), Some(ANY_LINE));
    }

    #[test]
    fn unknown_names_and_urls_stay_literal() {
        assert_eq!(parse("std::io::Read"), None);
        assert_eq!(parse("https://gitlab.com/x"), None);
        assert_eq!(parse("key: value"), None);
        assert_eq!(parse("timeout:30"), None);
        assert_eq!(parse("mr:approval"), None);
    }
}
