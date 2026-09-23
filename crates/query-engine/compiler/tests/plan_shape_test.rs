//! Plan-shape fixtures: `tests/plan_shape/*.yaml`.
//!
//! ```yaml
//! name: "AUTHORED has FK: no edge scan"
//! input: {"query_type": "traversal", ...}
//! expect:                       # every pattern matches some subtree
//!   - (Join ON mr.author_id = u.id (_) (_))
//!   - (Project excerpt(mr.title) AS mr_title, ...)
//! reject:                       # no pattern matches anywhere
//!   - (Scan gl_edge ...)
//! plan: |                       # optional exact snapshot of the whole tree
//!   (Limit 11 ...)
//! ```
//!
//! Patterns are the plan text (`orbit-devtools compile --plan`) with holes:
//! `(_)` any child, `(...)` any remaining children, `_` any label, `...`
//! among items for "and possibly more", `head...` for a head prefix. Items
//! (predicates, columns) match as a set.

use compiler::passes::plan_v2::explain::PlanNode;
use std::path::PathBuf;
use std::sync::Arc;

// ── Pattern language ────────────────────────────────────────────────────────

/// Parse `(Label head items... children...)` written in the explain layout:
/// one node per `(`, items on their own lines or inline after the head,
/// `)` closing. Whitespace and line breaks are free.
fn parse_pattern(src: &str) -> PlanNode {
    let mut p = Pat {
        chars: src.chars().collect(),
        pos: 0,
    };
    let node = p.node();
    p.ws();
    assert!(p.pos == p.chars.len(), "trailing text in pattern: {src}");
    node
}

struct Pat {
    chars: Vec<char>,
    pos: usize,
}

impl Pat {
    fn ws(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn node(&mut self) -> PlanNode {
        self.ws();
        assert_eq!(self.peek(), Some('('), "pattern node must start with '('");
        self.pos += 1;
        let label = self.word();
        // Head: everything on the first line after the label, up to a
        // comma-list of items or a child.
        let mut head_and_items = String::new();
        let mut children = Vec::new();
        loop {
            self.ws();
            match self.peek() {
                None => panic!("unclosed pattern node ({label}"),
                Some(')') => {
                    self.pos += 1;
                    break;
                }
                Some('(') => children.push(self.node()),
                Some(_) => {
                    let text = self.until_break();
                    if !head_and_items.is_empty() {
                        head_and_items.push('\n');
                    }
                    head_and_items.push_str(&text);
                }
            }
        }
        let (head, items) = split_head(&label, &head_and_items);
        PlanNode {
            label,
            head,
            items,
            children,
        }
    }

    fn word(&mut self) -> String {
        self.ws();
        let start = self.pos;
        while self.pos < self.chars.len()
            && !self.chars[self.pos].is_whitespace()
            && !"()".contains(self.chars[self.pos])
        {
            self.pos += 1;
        }
        self.chars[start..self.pos].iter().collect()
    }

    /// Text up to end of line, a child `(`, or the closing `)`, respecting
    /// nested call parens and quotes.
    fn until_break(&mut self) -> String {
        let mut depth = 0;
        let mut quote: Option<char> = None;
        let start = self.pos;
        while let Some(c) = self.peek() {
            match (quote, c) {
                (Some(q), c) if c == q => quote = None,
                (Some(_), _) => {}
                (None, '\'' | '"') => quote = Some(c),
                // A child node's `(` follows whitespace; a call's `(` follows its name.
                (None, '(')
                    if depth == 0 && self.pos > 0 && self.chars[self.pos - 1].is_whitespace() =>
                {
                    break;
                }
                (None, '(') => depth += 1,
                (None, ')') if depth == 0 => break,
                (None, ')') => depth -= 1,
                (None, '\n') if depth == 0 => break,
                _ => {}
            }
            self.pos += 1;
        }
        self.chars[start..self.pos]
            .iter()
            .collect::<String>()
            .trim()
            .to_string()
    }
}

/// Heads that are not item lists: Scan/Limit/Union/Sort/Join/SemiJoin text.
/// For Filter/Project/Aggregate the text is the item list.
fn split_head(label: &str, text: &str) -> (String, Vec<String>) {
    match label {
        "Filter" | "Project" | "Aggregate" | "_" => (String::new(), split_items(text)),
        _ => (text.replace('\n', " ").trim().to_string(), vec![]),
    }
}

/// Split on commas and newlines outside parens/brackets/quotes.
fn split_items(text: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut cur = String::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    for c in text.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '"') => quote = Some(c),
            (None, '(' | '[') => depth += 1,
            (None, ')' | ']') => depth -= 1,
            (None, ',' | '\n') if depth == 0 => {
                if !cur.trim().is_empty() {
                    items.push(cur.trim().to_string());
                }
                cur.clear();
                continue;
            }
            _ => {}
        }
        cur.push(c);
    }
    if !cur.trim().is_empty() {
        items.push(cur.trim().to_string());
    }
    items
}

// ── Matching ────────────────────────────────────────────────────────────────

fn matches(pat: &PlanNode, node: &PlanNode) -> bool {
    if pat.label == "_" && pat.head.is_empty() && pat.items.is_empty() && pat.children.is_empty() {
        return true;
    }
    if pat.label != "_" && pat.label != node.label {
        return false;
    }
    if !head_matches(&pat.head, &node.head) {
        return false;
    }
    let open = pat.items.iter().any(|i| i == "...");
    let want: Vec<&String> = pat.items.iter().filter(|i| *i != "...").collect();
    if !want.iter().all(|w| node.items.contains(w)) {
        return false;
    }
    if !open && want.len() != node.items.len() {
        return false;
    }
    children_match(&pat.children, &node.children)
}

/// Exact, or `...`-suffixed prefix, or `_`.
fn head_matches(pat: &str, actual: &str) -> bool {
    if pat.is_empty() || pat == "_" {
        return true;
    }
    match pat.strip_suffix("...") {
        Some(prefix) => actual.starts_with(prefix.trim_end()),
        None => pat == actual,
    }
}

fn children_match(pats: &[PlanNode], nodes: &[PlanNode]) -> bool {
    match pats.split_first() {
        None => nodes.is_empty(),
        Some((p, rest)) if p.label == "..." => {
            (0..=nodes.len()).any(|k| children_match(rest, &nodes[k..]))
        }
        Some((p, rest)) => match nodes.split_first() {
            Some((n, nrest)) => matches(p, n) && children_match(rest, nrest),
            None => false,
        },
    }
}

fn matches_anywhere(pat: &PlanNode, node: &PlanNode) -> bool {
    matches(pat, node) || node.children.iter().any(|c| matches_anywhere(pat, c))
}

// ── Runner ──────────────────────────────────────────────────────────────────

fn scenario_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/plan_shape")
}

fn load_scenarios() -> Vec<(String, serde_json::Value)> {
    let mut scenarios = Vec::new();
    for entry in std::fs::read_dir(scenario_dir()).expect("plan_shape dir") {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "yaml") {
            let content = std::fs::read_to_string(&path).unwrap();
            let doc: serde_json::Value = serde_saphyr::from_str(&content)
                .unwrap_or_else(|e| panic!("parse {:?}: {e}", path));
            let name = doc["name"].as_str().unwrap_or("unnamed").to_string();
            scenarios.push((name, doc));
        }
    }
    scenarios.sort_by(|a, b| a.0.cmp(&b.0));
    scenarios
}

fn strings(v: &serde_json::Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn plan_shape_scenarios() {
    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology"));
    let ctx = compiler::types::SecurityContext::new(1, vec!["1/".into()]).unwrap();
    let mut failures = Vec::new();
    let show = std::env::var("SHOW_PLANS").is_ok();

    for (name, doc) in load_scenarios() {
        let json = match &doc["input"] {
            serde_json::Value::String(s) => s.clone(),
            v => serde_json::to_string(v).unwrap(),
        };
        let compiled = match compiler::compile(&json, compiler::Frontend::JsonDsl, &ontology, &ctx)
        {
            Ok(c) => c,
            Err(e) => {
                failures.push(format!("{name}: compile failed: {e}"));
                continue;
            }
        };
        let mut input = compiled.input.clone();
        let (_, op) = match compiler::passes::plan_v2::plan(&mut input, &ontology) {
            Ok(p) => p,
            Err(e) => {
                failures.push(format!("{name}: plan failed: {e}"));
                continue;
            }
        };
        let tree = op.to_node();
        let text = tree.render(0);
        if show {
            eprintln!("=== {name} ===\n{text}\n");
        }
        let mut fail = |what: String| failures.push(format!("{name}: {what}\n\nplan:\n{text}\n"));

        for pat in strings(&doc["expect"]) {
            if !matches_anywhere(&parse_pattern(&pat), &tree) {
                fail(format!("expected pattern not found:\n{pat}"));
            }
        }
        for pat in strings(&doc["reject"]) {
            if matches_anywhere(&parse_pattern(&pat), &tree) {
                fail(format!("rejected pattern found:\n{pat}"));
            }
        }
        if let Some(expected) = doc["plan"].as_str()
            && expected.trim() != text.trim()
        {
            fail(format!(
                "plan snapshot differs; expected:\n{}",
                expected.trim()
            ));
        }
        if doc["expect"].is_null() && doc["reject"].is_null() && doc["plan"].is_null() {
            fail("fixture has no expect, reject, or plan".into());
        }
    }

    if !failures.is_empty() {
        panic!(
            "\n{} scenario(s) failed:\n\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}

#[test]
fn pattern_language() {
    let plan = parse_pattern(
        "(Join ON a.x = b.y\n  (Filter a.p = ?, !deleted(a)\n    (Scan t AS a FINAL))\n  (Scan u AS b))",
    );
    assert!(matches(
        &parse_pattern("(Join ON a.x = b.y (_) (_))"),
        &plan
    ));
    assert!(matches(
        &parse_pattern("(Join ON a.x = b.y (Filter a.p = ?, ... (_)) (...))"),
        &plan
    ));
    assert!(!matches(
        &parse_pattern("(Join ON a.x = b.y (Filter a.p = ? (_)) (...))"),
        &plan
    ));
    assert!(matches(&parse_pattern("(Join ON a.x... (...))"), &plan));
    assert!(matches_anywhere(
        &parse_pattern("(Scan t AS a FINAL)"),
        &plan
    ));
    assert!(matches_anywhere(&parse_pattern("(Scan t ...)"), &plan));
    assert!(!matches_anywhere(
        &parse_pattern("(Scan gl_edge ...)"),
        &plan
    ));
}
