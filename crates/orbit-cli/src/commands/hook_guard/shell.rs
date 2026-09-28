const COMMAND_WRAPPERS: &[&str] = &[
    "command", "env", "nice", "nohup", "sudo", "time", "timeout", "xargs",
];

pub(super) type Stage = Vec<String>;

pub(super) fn split(command: &str) -> Option<Vec<Vec<Stage>>> {
    let mut lexer = Lexer::default();
    let mut chars = command.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('\n') | None => {}
                Some(escaped) => lexer.push(escaped),
            },
            '\'' => {
                lexer.in_word = true;
                chars
                    .by_ref()
                    .take_while(|&quoted| quoted != '\'')
                    .for_each(|quoted| lexer.push(quoted));
            }
            '"' => {
                lexer.in_word = true;
                while let Some(quoted) = chars.next() {
                    match quoted {
                        '"' => break,
                        '\\' if chars.peek().is_some_and(|n| "\"\\$`".contains(*n)) => {
                            lexer.push(chars.next().unwrap_or('\\'));
                        }
                        quoted => lexer.push(quoted),
                    }
                }
            }
            ' ' | '\t' | '\r' => lexer.end_word(),
            '|' if chars.peek() != Some(&'|') => {
                chars.next_if_eq(&'&');
                lexer.end_stage();
            }
            '|' | '&' | ';' | '\n' | '(' | ')' | '`' => {
                if (c == '|' || c == '&') && chars.peek() == Some(&c) {
                    chars.next();
                }
                lexer.end_statement();
            }
            '$' if chars.peek() == Some(&'(') => lexer.end_word(),
            '<' | '>' => {
                if c == '<' && chars.peek() == Some(&'<') {
                    return None;
                }
                lexer.redirect();
                while chars.next_if(|n| matches!(n, '>' | '&' | '|')).is_some() {}
                let mut attached = false;
                while chars
                    .next_if(|n| !n.is_whitespace() && !"|;&()`<>".contains(*n))
                    .is_some()
                {
                    attached = true;
                }
                lexer.skip_next = !attached;
            }
            c => lexer.push(c),
        }
    }
    lexer.end_statement();
    Some(lexer.statements)
}

pub(super) fn strip_wrappers(words: &[String]) -> (bool, &[String]) {
    let mut via_xargs = false;
    let mut in_wrapper = false;
    let mut start = 0;
    for word in words {
        let name = basename(word);
        if COMMAND_WRAPPERS.contains(&name) {
            via_xargs |= name == "xargs";
            in_wrapper = true;
        } else if !is_assignment(word)
            && !(in_wrapper && (word.starts_with('-') || is_duration(word)))
        {
            break;
        }
        start += 1;
    }
    (via_xargs, &words[start..])
}

pub(super) fn basename(token: &str) -> &str {
    token.rsplit('/').next().unwrap_or(token)
}

fn is_assignment(word: &str) -> bool {
    word.split_once('=').is_some_and(|(name, _)| {
        name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

fn is_duration(word: &str) -> bool {
    let digits = word.trim_end_matches(['s', 'm', 'h', 'd']);
    !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit() || c == '.')
}

#[derive(Default)]
struct Lexer {
    statements: Vec<Vec<Stage>>,
    stages: Vec<Stage>,
    words: Stage,
    word: String,
    in_word: bool,
    skip_next: bool,
}

impl Lexer {
    fn push(&mut self, c: char) {
        self.word.push(c);
        self.in_word = true;
    }

    fn redirect(&mut self) {
        if self.in_word && self.word.chars().all(|c| c.is_ascii_digit()) {
            self.word.clear();
            self.in_word = false;
        }
        self.end_word();
    }

    fn end_word(&mut self) {
        if std::mem::take(&mut self.in_word) {
            let word = std::mem::take(&mut self.word);
            if !std::mem::take(&mut self.skip_next) {
                self.words.push(word);
            }
        }
    }

    fn end_stage(&mut self) {
        self.end_word();
        self.skip_next = false;
        if !self.words.is_empty() {
            self.stages.push(std::mem::take(&mut self.words));
        }
    }

    fn end_statement(&mut self) {
        self.end_stage();
        if !self.stages.is_empty() {
            self.statements.push(std::mem::take(&mut self.stages));
        }
    }
}
