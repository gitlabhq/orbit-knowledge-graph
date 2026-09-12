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
}
