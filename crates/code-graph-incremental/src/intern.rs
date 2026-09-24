use lasso::ThreadedRodeo;

/// Ids are `spur + 1`, so 0 means "no symbol".
#[allow(dead_code)]
pub struct Interner {
    pub(crate) rodeo: ThreadedRodeo,
}

/// One language's interners: node kinds, field names, and symbols.
pub struct Lang {
    pub kinds: Interner,
    pub fields: Interner,
    pub syms: Interner,
}
