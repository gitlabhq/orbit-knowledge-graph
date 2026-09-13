use crate::lang::Lang;
use crate::tree::{NONE, Tree};

pub const CANONICAL_BASE: u16 = 0xE000;

#[repr(u16)]
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter, strum::EnumString, strum::IntoStaticStr,
)]
pub enum Canonical {
    #[strum(serialize = "__def")]
    Def = CANONICAL_BASE,
    #[strum(serialize = "__defname")]
    DefName,
    #[strum(serialize = "__deftype")]
    DefType,
    #[strum(serialize = "__scope")]
    Scope,
    #[strum(serialize = "__return_type")]
    ReturnType,
    #[strum(serialize = "__supertype")]
    SuperType,
    #[strum(serialize = "__decorator")]
    Decorator,
    #[strum(serialize = "__self_method")]
    SelfMethod,
    #[strum(serialize = "__callable")]
    Callable,
    #[strum(serialize = "__visibility")]
    Visibility,
    #[strum(serialize = "__import")]
    Import,
    #[strum(serialize = "__import_type")]
    ImportType,
    #[strum(serialize = "__source")]
    Source,
    #[strum(serialize = "__source_path")]
    SourcePath,
    #[strum(serialize = "__name")]
    Name,
    #[strum(serialize = "__alias")]
    Alias,
    #[strum(serialize = "__call")]
    Call,
    #[strum(serialize = "__callee")]
    Callee,
    #[strum(serialize = "__args")]
    Args,
    #[strum(serialize = "__member")]
    Member,
    #[strum(serialize = "__object")]
    Object,
    #[strum(serialize = "__ivar")]
    Ivar,
    #[strum(serialize = "__binding")]
    Binding,
    #[strum(serialize = "__rhs")]
    Rhs,
    #[strum(serialize = "__branch")]
    Branch,
    #[strum(serialize = "__arm")]
    Arm,
    #[strum(serialize = "__loop")]
    Loop,
    #[strum(serialize = "__return")]
    Return,
}

impl From<Canonical> for u16 {
    fn from(ck: Canonical) -> u16 {
        ck as u16
    }
}

impl PartialEq<Canonical> for u16 {
    fn eq(&self, other: &Canonical) -> bool {
        *self == *other as u16
    }
}

pub fn is_canonical(kind: u16) -> bool {
    kind >= CANONICAL_BASE
}

pub fn classify_methods(tree: &mut Tree, lang: &mut Lang) {
    let func = lang.syms.intern("Function");
    let method = lang.syms.intern("Method");
    let assoc_fn = lang.syms.intern("AssociatedFunction");
    let class = lang.syms.intern("Class");
    let impl_ = lang.syms.intern("Impl");
    let trait_ = lang.syms.intern("Trait");

    for i in 0..tree.nodes.len() as u32 {
        if tree.kind(i) != Canonical::DefType || tree.sym(i) != func {
            continue;
        }
        let def = tree.nodes[i as usize].parent;
        if def == NONE {
            continue;
        }
        let mut p = tree.nodes[def as usize].parent;
        while p != NONE {
            if let Some(dt) = tree.cursor(p).child_sym(Canonical::DefType) {
                if dt == class {
                    tree.nodes[i as usize].sym = method;
                    break;
                }
                if dt == impl_ || dt == trait_ {
                    let has_self = tree.cursor(def).has(Canonical::SelfMethod);
                    tree.nodes[i as usize].sym = if has_self { method } else { assoc_fn };
                    break;
                }
            }
            p = tree.nodes[p as usize].parent;
        }
    }
}
