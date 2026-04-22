//! General-purpose string utilities: `StringPool`, `StrId`, `ScratchBuf`.

use std::fmt;

/// Index into [`StringPool`]. 4 bytes, Copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StrId(u32);

/// Pool of strings packed contiguously in a single `Vec<u8>` buffer.
///
/// An index `Vec` stores `(offset, len)` pairs for O(1) retrieval. One large
/// allocation instead of many individual `Box<str>` heap allocs.
///
/// No lifetime parameter, no global lock, no unsafe.
pub struct StringPool {
    /// Contiguous UTF-8 byte buffer. All strings packed end-to-end.
    buf: Vec<u8>,
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
            buf: Vec::new(),
            index: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap * 32),
            index: Vec::with_capacity(cap),
        }
    }

    /// Append a string to the pool. Returns an ID for later retrieval.
    pub fn alloc(&mut self, s: &str) -> StrId {
        let id = StrId(self.index.len() as u32);
        let offset = self.buf.len() as u32;
        self.buf.extend_from_slice(s.as_bytes());
        self.index.push((offset, s.len() as u32));
        id
    }

    /// Retrieve a string by ID.
    #[inline]
    pub fn get(&self, id: StrId) -> &str {
        let (offset, len) = self.index[id.0 as usize];
        let bytes = &self.buf[offset as usize..(offset + len) as usize];
        std::str::from_utf8(bytes).expect("StringPool: invalid UTF-8")
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

/// Reusable heap `String` for transient lookups.
///
/// Allocated once, reused via `clear()` + `write!()` or `push_str()`.
/// Avoids per-call `format!()` heap allocations in hot paths.
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

    /// Clear and write formatted content. Returns `&str` for immediate use.
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
