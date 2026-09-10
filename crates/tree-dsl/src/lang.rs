use rustc_hash::FxHashMap;

pub const NONE: u32 = u32::MAX;
pub const SYNTH: u16 = 0x8000;
pub const DEAD: u16 = 1 << 0;
pub const NAMED: u16 = 1 << 1;
pub const LANG_FLAG: u16 = 1 << 8;

pub const K_MEMBER: u16 = SYNTH | 1;
pub const K_CALL: u16 = SYNTH | 2;
pub const K_IVAR: u16 = SYNTH | 3;
pub const F_OBJECT: u16 = 1;
pub const F_MEMBER: u16 = 2;
pub const F_CALLEE: u16 = 3;
pub const F_ARGS: u16 = 4;
pub const E_CALLS: u16 = 1;
pub const E_DEFINES: u16 = 2;
pub const E_IMPORTS: u16 = 3;
pub const E_TYPE_REF: u16 = 4;
pub const E_ALIAS: u16 = 5;
pub const E_RETURNS: u16 = 6;

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
        let mut l = Lang::default();
        assert_eq!(
            [l.kind("__member"), l.kind("__call"), l.kind("__ivar")],
            [K_MEMBER, K_CALL, K_IVAR]
        );
        assert_eq!(
            [
                l.field("object"),
                l.field("member"),
                l.field("callee"),
                l.field("args")
            ],
            [F_OBJECT, F_MEMBER, F_CALLEE, F_ARGS]
        );
        l
    }

    pub fn kind(&mut self, s: &str) -> u16 {
        let k = self.kinds.get(s) as u16;
        if s.starts_with("__") { k | SYNTH } else { k }
    }

    pub fn field(&mut self, s: &str) -> u16 {
        self.fields.get(s) as u16
    }

    pub fn kind_name(&self, k: u16) -> &str {
        self.kinds.resolve((k & !SYNTH) as u32)
    }

    pub fn field_name(&self, f: u16) -> &str {
        self.fields.resolve(f as u32)
    }

    /// Clone this Lang's kinds/fields but start with a fresh sym interner.
    pub fn fork(&self) -> Lang {
        Lang {
            kinds: self.kinds.clone(),
            fields: self.fields.clone(),
            syms: Interner::default(),
        }
    }

    /// Build a sym remap table from this Lang into `target`, and return it.
    /// Also merges kinds and fields into target so all IDs are valid.
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

    /// Build a sym remap table without merging kinds/fields (caller handles that).
    pub fn sym_remap_syms_only(&self, target: &mut Lang) -> Vec<u32> {
        let mut remap = vec![0u32; self.syms.len() as usize + 1];
        for i in 1..=self.syms.len() {
            let s = self.syms.resolve(i);
            remap[i as usize] = target.syms.get(s);
        }
        remap
    }
}
