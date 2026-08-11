use std::fmt;

use smol_str::SmolStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StrId(u32);

/// Concurrent, append-only string interner.
///
/// `alloc` takes `&self`, enabling parallel interning from multiple threads.
/// `get` returns a cloned `SmolStr` (inline for strings <= 23 bytes, so no
/// heap allocation on the common path).
///
/// No deduplication: two calls with the same string produce distinct `StrId`s.
/// This is intentional; no caller relies on identity of `StrId` values.
pub struct StringPool {
    store: boxcar::Vec<SmolStr>,
}

impl Default for StringPool {
    fn default() -> Self {
        Self::new()
    }
}

impl StringPool {
    pub fn new() -> Self {
        Self {
            store: boxcar::Vec::new(),
        }
    }

    pub fn with_capacity(_cap: usize) -> Self {
        Self::new()
    }

    /// Intern a string. Takes `&self`, safe to call concurrently.
    pub fn alloc(&self, s: &str) -> StrId {
        let idx = self.store.push(SmolStr::new(s));
        StrId(idx as u32)
    }

    /// Resolve a handle to an owned `SmolStr`. For strings <= 23 bytes this
    /// is a stack copy with no heap allocation.
    #[inline]
    pub fn get(&self, id: StrId) -> SmolStr {
        self.store[id.0 as usize].clone()
    }

    pub fn len(&self) -> usize {
        self.store.count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Debug for StringPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StringPool")
            .field("strings", &self.len())
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

    #[test]
    fn basic_roundtrip() {
        let pool = StringPool::new();
        let a = pool.alloc("hello");
        let b = pool.alloc("world");
        assert_eq!(pool.get(a), "hello");
        assert_eq!(pool.get(b), "world");
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn empty_string() {
        let pool = StringPool::new();
        let id = pool.alloc("");
        assert_eq!(pool.get(id), "");
    }

    #[test]
    fn duplicates_produce_distinct_ids() {
        let pool = StringPool::new();
        let a = pool.alloc("same");
        let b = pool.alloc("same");
        assert_ne!(a, b);
        assert_eq!(pool.get(a), pool.get(b));
    }

    #[test]
    fn concurrent_alloc() {
        use std::sync::Arc;
        use std::thread;

        let pool = Arc::new(StringPool::new());
        let n = 10_000;
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let pool = Arc::clone(&pool);
                thread::spawn(move || {
                    let mut ids = Vec::with_capacity(n);
                    for i in 0..n {
                        let s = format!("t{t}-{i}");
                        ids.push((pool.alloc(&s), s));
                    }
                    ids
                })
            })
            .collect();

        let all: Vec<_> = threads
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        assert_eq!(pool.len(), 80_000);
        for (id, expected) in &all {
            assert_eq!(pool.get(*id).as_str(), expected.as_str());
        }
    }

    #[test]
    fn long_string() {
        let pool = StringPool::new();
        let long = "x".repeat(1000);
        let id = pool.alloc(&long);
        assert_eq!(pool.get(id), long);
    }
}
