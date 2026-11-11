#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Ident(String),
    Punct(char),
    /// A string literal's source text between its quotes, and the line it opens on.
    Str(String, usize),
}

/// Rust tokens of `source`, comments dropped and char literals kept as punctuation.
pub fn lex(source: &str) -> Vec<Token> {
    let chars: Vec<char> = source.chars().collect();
    let (mut tokens, mut at, mut line) = (Vec::new(), 0, 1);
    while at < chars.len() {
        let c = chars[at];
        let next = chars.get(at + 1).copied();
        if c == '\n' {
            line += 1;
            at += 1;
        } else if c.is_whitespace() {
            at += 1;
        } else if c == '/' && next == Some('/') {
            while at < chars.len() && chars[at] != '\n' {
                at += 1;
            }
        } else if c == '/' && next == Some('*') {
            at = block_comment(&chars, at, &mut line);
        } else if let Some((hashes, open)) = raw_string_start(&chars, at) {
            let start = line;
            let (text, end) = raw_string(&chars, open, hashes, &mut line);
            tokens.push(Token::Str(text, start));
            at = end;
        } else if c == '"' || (matches!(c, 'b' | 'c') && next == Some('"')) {
            let start = line;
            let open = if c == '"' { at + 1 } else { at + 2 };
            let (text, end) = quoted(&chars, open, &mut line);
            tokens.push(Token::Str(text, start));
            at = end;
        } else if c == '\'' {
            at = quote(&chars, at, &mut tokens);
        } else if c == '_' || c.is_alphanumeric() {
            let start = at;
            while at < chars.len() && (chars[at] == '_' || chars[at].is_alphanumeric()) {
                at += 1;
            }
            tokens.push(Token::Ident(chars[start..at].iter().collect()));
        } else {
            tokens.push(Token::Punct(c));
            at += 1;
        }
    }
    tokens
}

fn block_comment(chars: &[char], mut at: usize, line: &mut usize) -> usize {
    let mut depth = 0;
    while at < chars.len() {
        match (chars[at], chars.get(at + 1)) {
            ('/', Some('*')) => (depth, at) = (depth + 1, at + 2),
            ('*', Some('/')) => (depth, at) = (depth - 1, at + 2),
            (c, _) => {
                *line += usize::from(c == '\n');
                at += 1;
            }
        }
        if depth == 0 {
            break;
        }
    }
    at
}

/// The `#` count and the index past the opening quote of a raw string starting at `at`.
fn raw_string_start(chars: &[char], at: usize) -> Option<(usize, usize)> {
    let previous = at.checked_sub(1).map(|index| chars[index]);
    if previous.is_some_and(|c| c == '_' || c.is_alphanumeric()) {
        return None;
    }
    let mut index = at;
    if matches!(chars[index], 'b' | 'c') {
        index += 1;
    }
    if chars.get(index) != Some(&'r') {
        return None;
    }
    index += 1;
    let hashes = chars[index..].iter().take_while(|c| **c == '#').count();
    (chars.get(index + hashes) == Some(&'"')).then_some((hashes, index + hashes + 1))
}

fn raw_string(chars: &[char], open: usize, hashes: usize, line: &mut usize) -> (String, usize) {
    let mut at = open;
    while at < chars.len() {
        let closes = chars[at] == '"'
            && chars[at + 1..]
                .iter()
                .take(hashes)
                .filter(|c| **c == '#')
                .count()
                == hashes;
        if closes {
            return (chars[open..at].iter().collect(), at + 1 + hashes);
        }
        *line += usize::from(chars[at] == '\n');
        at += 1;
    }
    panic!("unterminated raw string");
}

fn quoted(chars: &[char], open: usize, line: &mut usize) -> (String, usize) {
    let mut at = open;
    while at < chars.len() && chars[at] != '"' {
        let step = if chars[at] == '\\' { 2 } else { 1 };
        *line += chars[at..at + step].iter().filter(|c| **c == '\n').count();
        at += step;
    }
    (chars[open..at].iter().collect(), at + 1)
}

/// Skips a char literal or emits a lifetime's quote, returning the index past it.
fn quote(chars: &[char], at: usize, tokens: &mut Vec<Token>) -> usize {
    tokens.push(Token::Punct('\''));
    match chars.get(at + 1) {
        Some('\\') => {
            let mut end = at + 3;
            while chars.get(end).is_some_and(|c| *c != '\'') {
                end += 1;
            }
            end + 1
        }
        Some(_) if chars.get(at + 2) == Some(&'\'') => at + 3,
        _ => at + 1,
    }
}
