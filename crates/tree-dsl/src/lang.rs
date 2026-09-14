use rustc_hash::FxHashMap;

#[derive(Default, Clone)]
pub struct Interner {
    map: FxHashMap<Box<str>, u32>,
    names: Vec<Box<str>>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct InternerSnapshot {
    names: Vec<String>,
}

impl From<&Interner> for InternerSnapshot {
    fn from(i: &Interner) -> Self {
        InternerSnapshot {
            names: i.names.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl From<InternerSnapshot> for Interner {
    fn from(s: InternerSnapshot) -> Self {
        let mut interner = Interner::default();
        for name in &s.names {
            interner.intern(name);
        }
        interner
    }
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

    /// Merge another interner into self, returning a remap table.
    /// remap[old_id] = new_id in self's address space.
    pub fn merge(&mut self, other: &Interner) -> Vec<u32> {
        let mut remap = vec![0u32; other.names.len() + 1];
        for (i, name) in other.names.iter().enumerate() {
            let old_id = (i + 1) as u32;
            let new_id = self.intern(name);
            remap[old_id as usize] = new_id;
        }
        remap
    }
}

#[derive(Default, Clone)]
pub struct Lang {
    pub kinds: Interner,
    pub fields: Interner,
    pub syms: Interner,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LangSnapshot {
    pub kinds: InternerSnapshot,
    pub fields: InternerSnapshot,
    pub syms: InternerSnapshot,
}

impl From<&Lang> for LangSnapshot {
    fn from(l: &Lang) -> Self {
        LangSnapshot {
            kinds: InternerSnapshot::from(&l.kinds),
            fields: InternerSnapshot::from(&l.fields),
            syms: InternerSnapshot::from(&l.syms),
        }
    }
}

impl From<LangSnapshot> for Lang {
    fn from(s: LangSnapshot) -> Self {
        Lang {
            kinds: Interner::from(s.kinds),
            fields: Interner::from(s.fields),
            syms: Interner::from(s.syms),
        }
    }
}

impl Lang {
    pub fn new() -> Lang {
        Lang::default()
    }

    pub fn intern_kind(&mut self, s: &str) -> u16 {
        if let Ok(ck) = s.parse::<crate::canonical::Canonical>() {
            return ck as u16;
        }
        let id = self.kinds.intern(s) as u16;
        debug_assert!(
            id < crate::canonical::CANONICAL_BASE,
            "dynamic kind ID {id} collides with canonical range"
        );
        id
    }

    pub fn lookup_kind(&self, s: &str) -> u16 {
        if let Ok(ck) = s.parse::<crate::canonical::Canonical>() {
            return ck as u16;
        }
        self.kinds.lookup(s) as u16
    }

    pub fn intern_field(&mut self, s: &str) -> u16 {
        self.fields.intern(s) as u16
    }

    pub fn kind_name(&self, k: u16) -> &str {
        if crate::canonical::is_canonical(k) {
            use strum::IntoEnumIterator;
            for ck in crate::canonical::Canonical::iter() {
                if ck as u16 == k {
                    let s: &'static str = ck.into();
                    return s;
                }
            }
            "__unknown"
        } else {
            self.kinds.resolve(k as u32)
        }
    }

    pub fn field_name(&self, f: u16) -> &str {
        self.fields.resolve(f as u32)
    }

    /// Create a per-thread Lang that shares kinds/fields and pre-populated syms.
    /// Pre-populating syms ensures pattern-compiled literal IDs stay valid.
    pub fn thread_fork(&self) -> Lang {
        Lang {
            kinds: self.kinds.clone(),
            fields: self.fields.clone(),
            syms: self.syms.clone(),
        }
    }

    /// Merge a per-thread Lang's syms back, returning a sym remap table.
    pub fn thread_merge(&mut self, other: &Lang) -> Vec<u32> {
        self.syms.merge(&other.syms)
    }
}
