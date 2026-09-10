use rustc_hash::FxHashMap;

#[derive(Default, Clone)]
pub struct Interner {
    map: FxHashMap<Box<str>, u32>,
    names: Vec<Box<str>>,
}

impl Interner {
    pub fn get(&mut self, s: &str) -> u32 {
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

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
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
    pub fn kind(&mut self, s: &str) -> u16 {
        self.kinds.get(s) as u16
    }

    /// Look up a kind by name without inserting. Returns 0 if not found.
    pub fn kind_id(&self, s: &str) -> u16 {
        self.kinds.lookup(s) as u16
    }

    pub fn field(&mut self, s: &str) -> u16 {
        self.fields.get(s) as u16
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

    pub fn fork(&self) -> Lang {
        Lang {
            kinds: self.kinds.clone(),
            fields: self.fields.clone(),
            syms: Interner::default(),
        }
    }

    pub fn sym_remap_into(&self, target: &mut Lang) -> Vec<u32> {
        for i in 1..=self.kinds.len() {
            let s = self.kinds.resolve(i);
            if !s.is_empty() {
                target.kinds.get(s);
            }
        }
        for i in 1..=self.fields.len() {
            let s = self.fields.resolve(i);
            if !s.is_empty() {
                target.fields.get(s);
            }
        }
        self.sym_remap_syms_only(target)
    }

    pub fn sym_remap_syms_only(&self, target: &mut Lang) -> Vec<u32> {
        let mut remap = vec![0u32; self.syms.len() as usize + 1];
        for i in 1..=self.syms.len() {
            let s = self.syms.resolve(i);
            remap[i as usize] = target.syms.get(s);
        }
        remap
    }
}
