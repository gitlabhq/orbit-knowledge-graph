use query_engine::compiler::passes::plan_v2::explain::PlanNode;

pub fn parse(source: &str) -> PlanNode {
    let mut parser = Parser {
        chars: source.chars().collect(),
        position: 0,
    };
    let node = parser.node();
    parser.whitespace();
    assert_eq!(
        parser.position,
        parser.chars.len(),
        "trailing text in pattern: {source}"
    );
    node
}

pub fn matches_anywhere(pattern: &PlanNode, node: &PlanNode) -> bool {
    matches(pattern, node)
        || node
            .children
            .iter()
            .any(|child| matches_anywhere(pattern, child))
}

fn matches(pattern: &PlanNode, node: &PlanNode) -> bool {
    if pattern.label == "_"
        && pattern.head.is_empty()
        && pattern.items.is_empty()
        && pattern.children.is_empty()
    {
        return true;
    }
    if pattern.label != "_" && pattern.label != node.label {
        return false;
    }
    if !head_matches(&pattern.head, &node.head) {
        return false;
    }
    let open = pattern.items.iter().any(|item| item == "...");
    let expected: Vec<&String> = pattern
        .items
        .iter()
        .filter(|item| *item != "...")
        .collect();
    if !expected.iter().all(|item| node.items.contains(item)) {
        return false;
    }
    if !open && expected.len() != node.items.len() {
        return false;
    }
    children_match(&pattern.children, &node.children)
}

fn head_matches(pattern: &str, actual: &str) -> bool {
    if pattern.is_empty() || pattern == "_" {
        return true;
    }
    match pattern.strip_suffix("...") {
        Some(prefix) => actual.starts_with(prefix.trim_end()),
        None => pattern == actual,
    }
}

fn children_match(patterns: &[PlanNode], nodes: &[PlanNode]) -> bool {
    match patterns.split_first() {
        None => nodes.is_empty(),
        Some((pattern, rest)) if pattern.label == "..." => {
            (0..=nodes.len()).any(|index| children_match(rest, &nodes[index..]))
        }
        Some((pattern, rest)) => match nodes.split_first() {
            Some((node, nodes)) => matches(pattern, node) && children_match(rest, nodes),
            None => false,
        },
    }
}

struct Parser {
    chars: Vec<char>,
    position: usize,
}

impl Parser {
    fn node(&mut self) -> PlanNode {
        self.whitespace();
        assert_eq!(self.peek(), Some('('), "pattern node must start with '('");
        self.position += 1;
        let label = self.word();
        let mut text = String::new();
        let mut children = Vec::new();
        loop {
            self.whitespace();
            match self.peek() {
                None => panic!("unclosed pattern node ({label}"),
                Some(')') => {
                    self.position += 1;
                    break;
                }
                Some('(') => children.push(self.node()),
                Some(_) => {
                    let next = self.until_break();
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(&next);
                }
            }
        }
        let (head, items) = split_head(&label, &text);
        PlanNode {
            label,
            head,
            items,
            children,
        }
    }

    fn word(&mut self) -> String {
        self.whitespace();
        let start = self.position;
        while self.position < self.chars.len()
            && !self.chars[self.position].is_whitespace()
            && !"()".contains(self.chars[self.position])
        {
            self.position += 1;
        }
        self.chars[start..self.position].iter().collect()
    }

    fn until_break(&mut self) -> String {
        let mut depth = 0;
        let mut quote = None;
        let start = self.position;
        while let Some(character) = self.peek() {
            match (quote, character) {
                (Some(expected), character) if character == expected => quote = None,
                (Some(_), _) => {}
                (None, '\'' | '"') => quote = Some(character),
                (None, '(')
                    if depth == 0
                        && self.position > 0
                        && self.chars[self.position - 1].is_whitespace() =>
                {
                    break;
                }
                (None, '(') => depth += 1,
                (None, ')') if depth == 0 => break,
                (None, ')') => depth -= 1,
                (None, '\n') if depth == 0 => break,
                _ => {}
            }
            self.position += 1;
        }
        self.chars[start..self.position]
            .iter()
            .collect::<String>()
            .trim()
            .to_string()
    }

    fn whitespace(&mut self) {
        while self.position < self.chars.len() && self.chars[self.position].is_whitespace() {
            self.position += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.position).copied()
    }
}

fn split_head(label: &str, text: &str) -> (String, Vec<String>) {
    match label {
        "Filter" | "Project" | "Aggregate" | "_" => (String::new(), split_items(text)),
        _ => (text.replace('\n', " ").trim().to_string(), vec![]),
    }
}

fn split_items(text: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut quote = None;
    for character in text.chars() {
        match (quote, character) {
            (Some(expected), character) if character == expected => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '"') => quote = Some(character),
            (None, '(' | '[') => depth += 1,
            (None, ')' | ']') => depth -= 1,
            (None, ',' | '\n') if depth == 0 => {
                if !current.trim().is_empty() {
                    items.push(current.trim().to_string());
                }
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(character);
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns_match_subtrees_and_holes() {
        let plan = parse(
            "(Join ON a.x = b.y\n  (Filter a.p = ?, !deleted(a)\n    (Scan t AS a FINAL))\n  (Scan u AS b))",
        );
        assert!(matches(&parse("(Join ON a.x = b.y (_) (_))"), &plan));
        assert!(matches(
            &parse("(Join ON a.x = b.y (Filter a.p = ?, ... (_)) (...))"),
            &plan
        ));
        assert!(!matches(
            &parse("(Join ON a.x = b.y (Filter a.p = ? (_)) (...))"),
            &plan
        ));
        assert!(matches_anywhere(&parse("(Scan t AS a FINAL)"), &plan));
        assert!(matches_anywhere(&parse("(Scan t ...)"), &plan));
        assert!(!matches_anywhere(&parse("(Scan gl_edge ...)"), &plan));
    }
}
