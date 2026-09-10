use rustc_hash::FxHashMap;

#[derive(Default)]
pub struct Interner {
    map: FxHashMap<Box<str>, u32>,
    names: Vec<Box<str>>,
}

impl Interner {
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.map.get(s) {
            return i;
        }
        self.names.push(s.into());
        let i = self.names.len() as u32;
        self.map.insert(s.into(), i);
        i
    }

    pub fn resolve(&self, i: u32) -> &str {
        if i == 0 {
            ""
        } else {
            &self.names[i as usize - 1]
        }
    }

    pub fn lookup(&self, s: &str) -> u32 {
        self.map.get(s).copied().unwrap_or(0)
    }

    pub fn len(&self) -> u32 {
        self.names.len() as u32
    }
}

#[derive(Default)]
pub struct Lang {
    pub kinds: Interner,
    pub fields: Interner,
    pub syms: Interner,
}

impl Lang {
    pub fn new() -> Lang {
        Lang::default()
    }

    /// Register a kind name and return its ID. Use `is_synth_name()` to check
    /// whether a kind name represents a synthetic node.
    pub fn intern_kind(&mut self, s: &str) -> u16 {
        self.kinds.intern(s) as u16
    }

    /// Look up a kind by name without inserting. Returns 0 if not found.
    pub fn lookup_kind(&self, s: &str) -> u16 {
        self.kinds.lookup(s) as u16
    }

    pub fn intern_field(&mut self, s: &str) -> u16 {
        self.fields.intern(s) as u16
    }

    pub fn kind_name(&self, k: u16) -> &str {
        self.kinds.resolve(k as u32)
    }

    /// Whether a kind name represents a synthetic node (starts with `__`).
    pub fn is_synth_name(s: &str) -> bool {
        s.starts_with("__")
    }

    pub fn field_name(&self, f: u16) -> &str {
        self.fields.resolve(f as u32)
    }
}
