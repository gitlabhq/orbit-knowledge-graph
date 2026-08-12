use std::fmt;

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
