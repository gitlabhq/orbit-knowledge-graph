use lasso::{Key, Spur, ThreadedRodeo};

pub struct Interner {
    rodeo: ThreadedRodeo,
}

impl Default for Interner {
    fn default() -> Self {
        Self {
            rodeo: ThreadedRodeo::new(),
        }
    }
}

impl Clone for Interner {
    fn clone(&self) -> Self {
        let new = Self::default();
        let mut pairs: Vec<(usize, &str)> = self
            .rodeo
            .iter()
            .map(|(k, v)| (k.into_usize(), v))
            .collect();
        pairs.sort_by_key(|(k, _)| *k);
        for (_, s) in pairs {
            new.rodeo.get_or_intern(s);
        }
        new
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct InternerSnapshot {
    names: Vec<String>,
}

impl From<&Interner> for InternerSnapshot {
    fn from(i: &Interner) -> Self {
        let mut pairs: Vec<(usize, String)> = i
            .rodeo
            .iter()
            .map(|(k, v)| (k.into_usize(), v.to_string()))
            .collect();
        pairs.sort_by_key(|(k, _)| *k);
        InternerSnapshot {
            names: pairs.into_iter().map(|(_, v)| v).collect(),
        }
    }
}

impl From<InternerSnapshot> for Interner {
    fn from(s: InternerSnapshot) -> Self {
        let i = Interner::default();
        for name in &s.names {
            i.intern(name);
        }
        i
    }
}

impl Interner {
    pub fn intern(&self, s: &str) -> u32 {
        let spur = self.rodeo.get_or_intern(s);
        spur.into_usize() as u32 + 1
    }

    pub fn resolve(&self, i: u32) -> &str {
        if i == 0 {
            return "";
        }
        let spur = Spur::try_from_usize(i as usize - 1).expect("invalid spur");
        self.rodeo.resolve(&spur)
    }

    pub fn lookup(&self, s: &str) -> u32 {
        self.rodeo
            .get(s)
            .map(|spur| spur.into_usize() as u32 + 1)
            .unwrap_or(0)
    }

    pub fn len(&self) -> u32 {
        self.rodeo.len() as u32
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

    pub fn intern_kind(&self, s: &str) -> u16 {
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

    pub fn intern_field(&self, s: &str) -> u16 {
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
