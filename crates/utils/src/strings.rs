use std::{borrow::Cow, fmt};

pub const fn ascii_alphanumeric_table(extra: &[u8]) -> [bool; 256] {
    let mut table = [false; 256];
    let mut byte = b'0';
    while byte <= b'9' {
        table[byte as usize] = true;
        byte += 1;
    }
    byte = b'A';
    while byte <= b'Z' {
        table[byte as usize] = true;
        byte += 1;
    }
    byte = b'a';
    while byte <= b'z' {
        table[byte as usize] = true;
        byte += 1;
    }
    let mut index = 0;
    while index < extra.len() {
        table[extra[index] as usize] = true;
        index += 1;
    }
    table
}

#[inline]
pub fn bytes_are_allowed(bytes: &[u8], allowed: &[bool; 256]) -> bool {
    bytes.iter().all(|byte| allowed[*byte as usize])
}

pub fn quote_escaped(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');

    let bytes = value.as_bytes();
    let mut start = 0;
    while start < bytes.len() {
        let special = next_escape_byte(&bytes[start..]).map_or(bytes.len(), |index| start + index);
        output.push_str(&value[start..special]);
        if special == bytes.len() {
            break;
        }
        match bytes[special] {
            b'\\' => output.push_str("\\\\"),
            b'"' => output.push_str("\\\""),
            b'\n' => output.push_str("\\n"),
            b'\r' => output.push_str("\\r"),
            b'\t' => output.push_str("\\t"),
            _ => {}
        }
        start = special + 1;
    }

    output.push('"');
    output
}

#[inline]
fn next_escape_byte(bytes: &[u8]) -> Option<usize> {
    let quoted = memchr::memchr3(b'\\', b'"', 0x7f, bytes);
    let bound = quoted.unwrap_or(bytes.len());
    let control = bytes[..bound].iter().position(|byte| *byte < 0x20);
    control.or(quoted)
}

#[inline]
pub fn char_count_if_exceeds(value: &str, limit: usize) -> Option<usize> {
    if value.len() <= limit {
        return None;
    }
    let count = value.chars().count();
    (count > limit).then_some(count)
}

pub fn truncate_chars<'a>(value: &'a str, limit: usize, suffix: &str) -> Cow<'a, str> {
    if char_count_if_exceeds(value, limit).is_none() {
        return Cow::Borrowed(value);
    }
    let take = limit.saturating_sub(suffix.chars().count());
    let mut truncated: String = value.chars().take(take).collect();
    truncated.push_str(suffix);
    Cow::Owned(truncated)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StrId(u32);

/// One large allocation instead of many individual `Box<str>` heap allocs;
/// an index `Vec` of `(offset, len)` pairs gives O(1) retrieval.
pub struct StringPool {
    // `String`, not `Vec<u8>`, so `get` slices in O(1) instead of revalidating
    // UTF-8 on every access (it is the hot accessor for every graph string).
    buf: String,
    /// (byte_offset, byte_len) into `buf` for each StrId.
    index: Vec<(u32, u32)>,
}

impl Default for StringPool {
    fn default() -> Self {
        Self::new()
    }
}

impl StringPool {
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            index: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: String::with_capacity(cap * 32),
            index: Vec::with_capacity(cap),
        }
    }

    pub fn alloc(&mut self, s: &str) -> StrId {
        let id = StrId(self.index.len() as u32);
        let offset = self.buf.len() as u32;
        self.buf.push_str(s);
        self.index.push((offset, s.len() as u32));
        id
    }

    #[inline]
    pub fn get(&self, id: StrId) -> &str {
        let (offset, len) = self.index[id.0 as usize];
        &self.buf[offset as usize..(offset + len) as usize]
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

impl fmt::Debug for StringPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StringPool")
            .field("strings", &self.index.len())
            .field("bytes", &self.buf.len())
            .finish()
    }
}

/// Allocated once, reused via `clear()`; avoids per-call `format!()` heap
/// allocations in hot paths.
pub struct ScratchBuf(String);

impl Default for ScratchBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScratchBuf {
    pub fn new() -> Self {
        Self(String::new())
    }

    #[inline]
    pub fn set_fmt(&mut self, args: fmt::Arguments<'_>) -> &str {
        self.0.clear();
        fmt::Write::write_fmt(&mut self.0, args).unwrap();
        &self.0
    }

    #[inline]
    pub fn clear(&mut self) {
        self.0.clear();
    }

    #[inline]
    pub fn push_str(&mut self, s: &str) {
        self.0.push_str(s);
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Write for ScratchBuf {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0.write_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN_BYTES: [bool; 256] = ascii_alphanumeric_table(b"_-:./@+");

    fn quote_escaped_reference(value: &str) -> String {
        let mut output = String::with_capacity(value.len() + 2);
        output.push('"');
        for character in value.chars() {
            match character {
                '\\' => output.push_str("\\\\"),
                '"' => output.push_str("\\\""),
                '\n' => output.push_str("\\n"),
                '\r' => output.push_str("\\r"),
                '\t' => output.push_str("\\t"),
                character if (character as u32) < 0x20 || character == '\u{7f}' => {}
                character => output.push(character),
            }
        }
        output.push('"');
        output
    }

    fn is_bare_token_reference(value: &str) -> bool {
        value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '_' | '-' | ':' | '.' | '/' | '@' | '+')
        })
    }

    fn truncate_reference(value: &str, limit: usize) -> Cow<'_, str> {
        if value.chars().count() <= limit {
            return Cow::Borrowed(value);
        }
        let head: String = value.chars().take(limit.saturating_sub(3)).collect();
        Cow::Owned(format!("{head}..."))
    }

    fn generated_strings(alphabet: &[char], max_len: usize) -> Vec<String> {
        let mut values = vec![String::new()];
        let mut level = vec![String::new()];
        for _ in 0..max_len {
            let mut next = Vec::with_capacity(level.len() * alphabet.len());
            for prefix in &level {
                for character in alphabet {
                    let mut value = prefix.clone();
                    value.push(*character);
                    next.push(value);
                }
            }
            values.extend(next.iter().cloned());
            level = next;
        }
        values
    }

    #[test]
    fn optimized_helpers_match_reference_implementations() {
        let alphabet = [
            'a', 'Z', '7', '_', '\\', '"', '\n', '\u{1f}', '\u{7f}', 'é', '🦀',
        ];
        let mut values = generated_strings(&alphabet, 4);
        values.extend([
            "\0\u{1}\u{8}\t\n\u{b}\u{c}\r\u{e}\u{1f}\u{7f}".to_string(),
            "_-:./@+".to_string(),
            "plain ASCII clean span".repeat(20),
            "日本語のきれいな範囲".repeat(20),
            format!("{}\\{}", "a".repeat(512), "b".repeat(512)),
            "\\\"\u{7f}".repeat(1024),
            "a".repeat(199),
            "a".repeat(200),
            "a".repeat(201),
            "🦀".repeat(199),
            "🦀".repeat(200),
            "🦀".repeat(201),
            "a".repeat(999),
            "a".repeat(1000),
            "a".repeat(1001),
        ]);

        for value in values {
            assert_eq!(quote_escaped(&value), quote_escaped_reference(&value));
            assert_eq!(
                bytes_are_allowed(value.as_bytes(), &TOKEN_BYTES),
                is_bare_token_reference(&value)
            );
            for limit in (0..=8).chain([199, 200, 201, 999, 1000, 1001]) {
                assert_eq!(
                    truncate_chars(&value, limit, "..."),
                    truncate_reference(&value, limit)
                );
                assert_eq!(
                    char_count_if_exceeds(&value, limit),
                    (value.chars().count() > limit).then(|| value.chars().count())
                );
            }
        }
    }

    #[test]
    fn lookup_table_rejects_every_non_ascii_byte() {
        assert!(bytes_are_allowed(b"Az09_-:./@+", &TOKEN_BYTES));
        assert!((0x80_u8..=u8::MAX).all(|byte| !TOKEN_BYTES[byte as usize]));
    }
}
