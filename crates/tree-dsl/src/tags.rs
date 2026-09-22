//! Tags the engine reads.
//!
//! Language YAML may set any tag it likes for its own later rules and for
//! `export.yaml`; the engine ignores those. The keys below are the only ones
//! the linker and resolver branch on, so adding one here is a deliberate
//! extension of the engine's contract with every language.

use crate::intern::Lang;

#[derive(Clone, Copy)]
pub struct ReservedTags {
    /// On a def or import: name references may resolve to it as a call target.
    pub callable: u32,
    /// On a def: it opens a lexical scope for the SSA walk.
    pub scoped: u32,
    /// On a scoped def: its direct child defs are visible before their
    /// definition, as with Elixir module functions.
    pub hoisted: u32,
    /// On any node: this file exports the named symbol. On a def it is an extra
    /// visible name; on a bare `__defname` it is a C-style prototype whose body
    /// an includer supplies.
    pub exports: u32,
    /// On an import: the source path after language-specific normalisation.
    pub resolved_source: u32,
    /// On a node: names from the given source path become visible in this
    /// file as if imported, as with Kotlin extension receivers.
    pub visible_from: u32,
    /// On a call whose callee is a bare method name (not a type): resolve as
    /// locals, then members of the enclosing class and its supertypes, then
    /// callable wildcard imports. Untagged bare callees resolve as names.
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
            ("linker.rs", include_str!("linker.rs")),
            ("resolver.rs", include_str!("resolver.rs")),
            ("tree/walk.rs", include_str!("tree/walk.rs")),
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
