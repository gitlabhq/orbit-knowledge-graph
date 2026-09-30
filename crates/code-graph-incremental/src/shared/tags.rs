//! The only tag keys the linker and resolver branch on; every other tag a
//! language sets is passed through to export untouched.

use crate::intern::Lang;

#[derive(Clone, Copy)]
pub struct ReservedTags {
    pub callable: u32,
    pub scoped: u32,
    pub hoisted: u32,
    pub exports: u32,
    pub resolved_source: u32,
    pub visible_from: u32,
    pub implicit_self: u32,
}

impl ReservedTags {
    pub fn new(lang: &Lang) -> Self {
        let k = |s| lang.syms.intern(s);
        Self {
            callable: k("callable"),
            scoped: k("scoped"),
            hoisted: k("hoisted"),
            exports: k("exports"),
            resolved_source: k("resolved_source"),
            visible_from: k("visible_from"),
            implicit_self: k("implicit_self"),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn engine_interns_tag_keys_only_here() {
        for (name, src) in [
            ("linker.rs", include_str!("../linker.rs")),
            ("resolver.rs", include_str!("../resolver.rs")),
            ("tree/walk.rs", include_str!("../tree/walk.rs")),
        ] {
            for (i, line) in src.lines().enumerate() {
                assert!(
                    !line.contains("syms.intern(\""),
                    "{name}:{}: engine-read tag keys belong in tags.rs",
                    i + 1
                );
            }
        }
    }
}
