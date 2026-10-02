use super::operator::Operator;
use super::terms::{self, Captures};
use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub struct Expression {
    pub label: Operator,
    pub head: String,
    pub items: Vec<String>,
    pub children: Vec<Self>,
}

impl Expression {
    pub fn node(label: Operator, head: impl Into<String>, children: Vec<Self>) -> Self {
        let text = head.into();
        let (head, items) = if matches!(
            label,
            Operator::Filter | Operator::Project | Operator::Aggregate | Operator::Hole
        ) {
            (String::new(), split_items(&text))
        } else {
            (text.trim().to_string(), vec![])
        };
        Self {
            label,
            head,
            items,
            children,
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn render(node: &Expression, out: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
            write!(out, "({}", node.label)?;
            if !node.head.is_empty() {
                write!(out, " {}", node.head)?;
            }
            if !node.items.is_empty() {
                write!(out, " {}", node.items.join(", "))?;
            }
            for child in &node.children {
                write!(out, "\n{}", "  ".repeat(depth + 1))?;
                render(child, out, depth + 1)?;
            }
            write!(out, ")")
        }
        render(self, out, 0)
    }
}

pub fn parse(text: &str) -> Result<Expression, String> {
    struct Parser<'a> {
        text: &'a str,
        position: usize,
    }
    impl Parser<'_> {
        fn peek(&self) -> Option<u8> {
            self.text.as_bytes().get(self.position).copied()
        }
        fn whitespace(&mut self) {
            while self.peek().is_some_and(|c| c.is_ascii_whitespace()) {
                self.position += 1;
            }
        }
        fn child(&self) -> bool {
            if self.peek() != Some(b'(') {
                return false;
            }
            let label = self.text[self.position + 1..]
                .split(|c: char| c.is_whitespace() || c == ')')
                .next()
                .unwrap_or("");
            Operator::parse(label).is_ok()
        }
        fn node(&mut self) -> Result<Expression, String> {
            self.whitespace();
            if self.peek() != Some(b'(') {
                return Err(format!("expected '(' at byte {}", self.position));
            }
            self.position += 1;
            let start = self.position;
            while self
                .peek()
                .is_some_and(|c| !c.is_ascii_whitespace() && !b"()".contains(&c))
            {
                self.position += 1;
            }
            let label = &self.text[start..self.position];
            let label = Operator::parse(label)?;
            let mut head = Vec::new();
            let mut children = Vec::new();
            loop {
                self.whitespace();
                match self.peek() {
                    None => return Err(format!("unclosed pattern ({label}")),
                    Some(b')') => {
                        self.position += 1;
                        break;
                    }
                    Some(b'(') if self.child() => children.push(self.node()?),
                    _ => {
                        let start = self.position;
                        let mut depth = 0;
                        let mut quote = None;
                        let mut escaped = false;
                        while let Some(c) = self.peek() {
                            if let Some(expected) = quote {
                                if !escaped && c == expected {
                                    quote = None;
                                }
                                escaped = c == b'\\' && !escaped;
                            } else {
                                match c {
                                    b'\'' | b'"' => quote = Some(c),
                                    b'(' if depth == 0
                                        && self.position > start
                                        && self.text.as_bytes()[self.position - 1]
                                            .is_ascii_whitespace() =>
                                    {
                                        if self.child() {
                                            break;
                                        }
                                        depth += 1;
                                    }
                                    b'(' | b'[' => depth += 1,
                                    b')' if depth == 0 => break,
                                    b')' | b']' => {
                                        if depth == 0 {
                                            return Err("unexpected closing bracket".into());
                                        }
                                        depth -= 1;
                                    }
                                    b'\n' if depth == 0 => break,
                                    _ => {}
                                }
                            }
                            self.position += 1;
                        }
                        if quote.is_some() || depth != 0 {
                            return Err("unclosed expression or string".into());
                        }
                        head.push(self.text[start..self.position].trim());
                    }
                }
            }
            if label == Operator::Sequence && (!head.is_empty() || !children.is_empty()) {
                return Err("(...) cannot have text or children".into());
            }
            if label == Operator::Hole && (!head.is_empty() || !children.is_empty()) {
                return Err("(_) is a subtree hole; it cannot have text or children".into());
            }
            let result = Expression::node(label, head.join("\n"), children);
            for text in std::iter::once(&result.head).chain(&result.items) {
                terms::variables(text)?;
                let tokens = terms::parse(text)?;
                if tokens.iter().enumerate().any(|(index, token)| {
                    matches!(token, terms::Term::Atom(value) if value == "...")
                        && (index + 1 != tokens.len()
                            || (!result.items.is_empty() && text != "..."))
                }) {
                    return Err(
                        "use '...' as a whole list item or at the end of an operator head".into(),
                    );
                }
            }
            if result.items.iter().filter(|item| *item == "...").count() > 1 {
                return Err("only one item-list '...' is allowed".into());
            }
            Ok(result)
        }
    }
    let mut parser = Parser { text, position: 0 };
    let result = parser.node()?;
    parser.whitespace();
    if parser.peek().is_some() {
        return Err("trailing input".into());
    }
    if result.label == Operator::Sequence {
        return Err("(...) is only valid in a child sequence".into());
    }
    Ok(result)
}

fn split_items(text: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    let mut quote = None;
    let mut escaped = false;
    for (index, c) in text.char_indices() {
        if let Some(expected) = quote {
            if !escaped && c == expected {
                quote = None;
            }
            escaped = c == '\\' && !escaped;
        } else {
            match c {
                '\'' | '"' => quote = Some(c),
                '(' | '[' => depth += 1,
                ')' | ']' => depth -= 1,
                ',' | '\n' if depth == 0 => {
                    if !text[start..index].trim().is_empty() {
                        items.push(text[start..index].trim().into());
                    }
                    start = index + c.len_utf8();
                }
                _ => {}
            }
        }
    }
    if !text[start..].trim().is_empty() {
        items.push(text[start..].trim().into());
    }
    items
}

pub fn matches(pattern: &Expression, actual: &Expression, captures: &Captures) -> Option<Captures> {
    if pattern.label == Operator::Hole
        && pattern.head.is_empty()
        && pattern.items.is_empty()
        && pattern.children.is_empty()
    {
        return Some(captures.clone());
    }
    if pattern.label != Operator::Hole && pattern.label != actual.label {
        return None;
    }
    let mut captures = captures.clone();
    if !pattern.head.is_empty() && pattern.head != "_" {
        captures = match pattern.head.strip_suffix("...") {
            Some(prefix) => terms::match_prefix(prefix.trim_end(), &actual.head, &captures)?,
            None => terms::match_text(&pattern.head, &actual.head, &captures)?,
        };
    }
    fn items(
        patterns: &[&String],
        nodes: &[String],
        captures: &Captures,
        open: bool,
    ) -> Option<Captures> {
        match patterns.split_first() {
            None => (open || nodes.is_empty()).then(|| captures.clone()),
            Some((first, rest)) => nodes.iter().enumerate().find_map(|(index, node)| {
                let captures = terms::match_text(first, node, captures)?;
                let mut remaining = nodes.to_vec();
                remaining.remove(index);
                items(rest, &remaining, &captures, open)
            }),
        }
    }
    captures = items(
        &pattern
            .items
            .iter()
            .filter(|item| *item != "...")
            .collect::<Vec<_>>(),
        &actual.items,
        &captures,
        pattern.items.iter().any(|item| item == "..."),
    )?;
    fn children(
        patterns: &[Expression],
        nodes: &[Expression],
        captures: &Captures,
    ) -> Option<Captures> {
        match patterns.split_first() {
            None => nodes.is_empty().then(|| captures.clone()),
            Some((first, rest)) if first.label == Operator::Sequence => {
                (0..=nodes.len()).find_map(|offset| children(rest, &nodes[offset..], captures))
            }
            Some((first, rest)) => nodes.split_first().and_then(|(node, remaining)| {
                children(rest, remaining, &matches(first, node, captures)?)
            }),
        }
    }
    children(&pattern.children, &actual.children, &captures)
}

pub fn find(actual: &Expression, pattern: &Expression, captures: &Captures) -> Vec<Captures> {
    matches(pattern, actual, captures)
        .into_iter()
        .chain(
            actual
                .children
                .iter()
                .flat_map(|child| find(child, pattern, captures)),
        )
        .collect()
}

pub fn variables(pattern: &Expression) -> Result<Vec<String>, String> {
    let mut names = terms::variables(&pattern.head)?;
    for item in &pattern.items {
        names.extend(terms::variables(item)?);
    }
    for child in &pattern.children {
        names.extend(variables(child)?);
    }
    Ok(names)
}

pub fn has_holes(pattern: &Expression) -> bool {
    fn holes(terms: &[terms::Term]) -> bool {
        terms.iter().any(|term| match term {
            terms::Term::Atom(value) => {
                matches!(value.as_str(), "?" | "_" | "...") || value.starts_with('$')
            }
            terms::Term::Group(_, children) => holes(children),
        })
    }
    matches!(pattern.label, Operator::Hole | Operator::Sequence)
        || std::iter::once(&pattern.head)
            .chain(&pattern.items)
            .any(|text| holes(&terms::parse(text).unwrap()))
        || pattern.children.iter().any(has_holes)
}

pub fn closest<'a>(actual: &'a Expression, pattern: &Expression) -> Option<&'a Expression> {
    if actual.label == pattern.label {
        return Some(actual);
    }
    actual
        .children
        .iter()
        .find_map(|child| closest(child, pattern))
}
