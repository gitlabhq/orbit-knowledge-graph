use lasso::{Key, Spur, ThreadedRodeo};

pub struct Interner {
    pub(crate) rodeo: ThreadedRodeo,
    id: u64,
}

static NEXT_INTERNER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl Default for Interner {
    fn default() -> Self {
        Self {
            rodeo: ThreadedRodeo::new(),
            id: NEXT_INTERNER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl Interner {
    /// Distinct per interner, so caches keyed on interned ids never mix two.
    pub(crate) fn id(&self) -> u64 {
        self.id
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

    fn try_resolve(&self, i: u32) -> Option<&str> {
        let spur = Spur::try_from_usize((i as usize).checked_sub(1)?)?;
        self.rodeo.try_resolve(&spur)
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Default, Clone)]
pub struct Lang {
    pub kinds: Interner,
    pub fields: Interner,
    pub syms: Interner,
    pub edges: Interner,
    /// The canonical kinds pipeline.yaml declares, by name.
    pub vocabulary: Interner,
    /// The kinds the engine's own tree builders use, bound in pipeline.yaml.
    pub tree: std::sync::OnceLock<crate::tree::TreeKinds>,
    /// The attributes edges carry, by slot, as pipeline.yaml declares them.
    pub edge_attributes: std::sync::OnceLock<Vec<crate::edge::EdgeAttribute>>,
}

impl Lang {
    pub fn new() -> Lang {
        Lang::default()
    }

    pub fn intern_kind(&self, s: &str) -> u16 {
        self.kinds.intern(s) as u16
    }

    pub fn lookup_kind(&self, s: &str) -> u16 {
        self.kinds.lookup(s) as u16
    }

    pub fn in_vocabulary(&self, kind: u16) -> bool {
        self.kinds
            .try_resolve(u32::from(kind))
            .is_some_and(|name| self.vocabulary.lookup(name) != 0)
    }

    pub fn tree_kinds(&self) -> crate::tree::TreeKinds {
        self.tree.get().copied().unwrap_or_default()
    }

    pub fn intern_field(&self, s: &str) -> u16 {
        self.fields.intern(s) as u16
    }

    pub fn kind_name(&self, k: u16) -> &str {
        self.kinds.resolve(k as u32)
    }

    pub fn field_name(&self, f: u16) -> &str {
        self.fields.resolve(f as u32)
    }

    /// A declared edge kind by name.
    pub fn edge_kind(&self, name: &str) -> Option<crate::edge::EdgeKind> {
        match self.edges.lookup(name) {
            0 => None,
            id => Some(crate::edge::EdgeKind(id as u16)),
        }
    }

    pub fn edge_name(&self, kind: crate::edge::EdgeKind) -> &str {
        self.edges.resolve(u32::from(kind.0))
    }

    pub fn edge_attributes(&self) -> &[crate::edge::EdgeAttribute] {
        self.edge_attributes.get().map_or(&[], Vec::as_slice)
    }

    /// A declared edge attribute's slot by name.
    pub fn edge_attribute(&self, name: &str) -> Option<usize> {
        self.edge_attributes().iter().position(|a| a.name == name)
    }
}
