use rustc_hash::FxHashMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StrId(u32);

/// Interning string pool. Identical strings return the same `StrId`,
/// enabling O(1) equality checks via integer comparison.
pub struct StringPool {
    buf: String,
    index: Vec<(u32, u32)>,
    /// Maps (hash64, byte_len) → list of StrIds with that hash+len.
    /// On lookup, each candidate is verified by full string comparison.
    intern_map: FxHashMap<(u64, u32), Vec<StrId>>,
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
            intern_map: FxHashMap::default(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: String::with_capacity(cap * 32),
            index: Vec::with_capacity(cap),
            intern_map: FxHashMap::with_capacity_and_hasher(cap, Default::default()),
        }
    }

    /// Intern a string. Returns the same `StrId` for identical content.
    pub fn alloc(&mut self, s: &str) -> StrId {
        let key = (fxhash_str(s), s.len() as u32);

        if let Some(candidates) = self.intern_map.get(&key) {
            for &existing in candidates {
                if self.get(existing) == s {
                    return existing;
                }
            }
        }

        let id = StrId(self.index.len() as u32);
        let offset = self.buf.len() as u32;
        self.buf.push_str(s);
        self.index.push((offset, s.len() as u32));
        self.intern_map.entry(key).or_default().push(id);
        id
    }

    /// Look up a string's `StrId` without allocating. Returns `None` if the
    /// string has never been interned.
    pub fn find(&self, s: &str) -> Option<StrId> {
        let key = (fxhash_str(s), s.len() as u32);
        let candidates = self.intern_map.get(&key)?;
        candidates.iter().copied().find(|&id| self.get(id) == s)
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

fn fxhash_str(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = rustc_hash::FxHasher::default();
    s.hash(&mut h);
    h.finish()
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
