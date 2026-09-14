pub const CANONICAL_BASE: u16 = 0xE000;

#[repr(u16)]
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    strum::EnumIter,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumProperty,
)]
pub enum Canonical {
    // ── Structural ──
    #[strum(serialize = "__def")]
    Def = CANONICAL_BASE,
    #[strum(serialize = "__defname")]
    DefName,
    #[strum(serialize = "__return_type")]
    ReturnType,
    #[strum(serialize = "__supertype")]
    SuperType,
    #[strum(serialize = "__decorator")]
    Decorator,
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
    #[strum(serialize = "__cjs_require")]
    CjsRequire,
    #[strum(serialize = "__module_export")]
    ModuleExport,
    #[strum(serialize = "__default_export")]
    DefaultExport,

    // ── Config inlining ──
    #[strum(serialize = "__obj")]
    Obj,
    #[strum(serialize = "__arr")]
    Arr,
    #[strum(serialize = "__field")]
    ConfigField,
    #[strum(serialize = "__str")]
    Str,
    #[strum(serialize = "__num")]
    ConfigNum,
    #[strum(serialize = "__bool")]
    ConfigBool,

    // ── Def-type kinds ──
    #[strum(
        serialize = "__function",
        props(
            def_type = "true",
            callable = "true",
            scoped = "true",
            display = "Function"
        )
    )]
    Function,
    #[strum(
        serialize = "__method",
        props(
            def_type = "true",
            callable = "true",
            scoped = "true",
            display = "Method"
        )
    )]
    Method,
    #[strum(
        serialize = "__class",
        props(
            def_type = "true",
            callable = "true",
            scoped = "true",
            display = "Class"
        )
    )]
    Class,
    #[strum(
        serialize = "__struct",
        props(def_type = "true", scoped = "true", display = "Struct")
    )]
    Struct,
    #[strum(
        serialize = "__impl",
        props(def_type = "true", scoped = "true", display = "Impl")
    )]
    ImplBlock,
    #[strum(
        serialize = "__trait",
        props(def_type = "true", scoped = "true", display = "Trait")
    )]
    Trait,
    #[strum(
        serialize = "__interface",
        props(def_type = "true", display = "Interface")
    )]
    Interface,
    #[strum(
        serialize = "__enum",
        props(def_type = "true", scoped = "true", display = "Enum")
    )]
    Enum,
    #[strum(
        serialize = "__variable",
        props(def_type = "true", display = "Variable")
    )]
    Variable,
    #[strum(
        serialize = "__constant",
        props(def_type = "true", display = "Constant")
    )]
    Constant,
    #[strum(
        serialize = "__static_constant",
        props(def_type = "true", display = "Static")
    )]
    StaticConstant,
    #[strum(
        serialize = "__type_alias",
        props(def_type = "true", display = "TypeAlias")
    )]
    TypeAlias,
    #[strum(
        serialize = "__property",
        props(def_type = "true", display = "Property")
    )]
    Property,
    #[strum(
        serialize = "__lambda",
        props(def_type = "true", callable = "true", display = "Lambda")
    )]
    Lambda,
    #[strum(serialize = "__field_def", props(def_type = "true", display = "Field"))]
    FieldDef,
    #[strum(serialize = "__enum_variant", props(def_type = "true", callable = "true", display = "EnumVariant"))]
    EnumVariant,

    // ── Flavors ──
    #[strum(serialize = "__async", props(flavor = "true"))]
    Async,
    #[strum(serialize = "__static", props(flavor = "true"))]
    Static,
    #[strum(serialize = "__abstract", props(flavor = "true"))]
    Abstract,
    #[strum(serialize = "__generator", props(flavor = "true"))]
    Generator,
    #[strum(serialize = "__ssa_hint")]
    SsaHint,
    #[strum(serialize = "__self_method", props(flavor = "true"))]
    SelfMethod,
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

impl Canonical {
    pub fn is_def_type(self) -> bool {
        self.get_str("def_type") == Some("true")
    }
    pub fn is_callable(self) -> bool {
        self.get_str("callable") == Some("true")
    }
    pub fn is_scoped(self) -> bool {
        self.get_str("scoped") == Some("true")
    }
    pub fn is_flavor(self) -> bool {
        self.get_str("flavor") == Some("true")
    }
    pub fn display_name(self) -> &'static str {
        self.get_str("display").unwrap_or("")
    }
}

use strum::EnumProperty;

pub fn def_type_of(cursor: crate::tree::Cursor) -> Option<Canonical> {
    cursor.children().find_map(|c| {
        let ck = Canonical::try_from_u16(c.kind())?;
        ck.is_def_type().then_some(ck)
    })
}

pub fn has_def_type(cursor: crate::tree::Cursor) -> bool {
    def_type_of(cursor).is_some()
}

pub fn is_callable_def(cursor: crate::tree::Cursor) -> bool {
    def_type_of(cursor).is_some_and(|k| k.is_callable())
}

pub fn is_scoped_def(cursor: crate::tree::Cursor) -> bool {
    def_type_of(cursor).is_some_and(|k| k.is_scoped())
}

impl Canonical {
    pub fn try_from_u16(kind: u16) -> Option<Self> {
        if kind < CANONICAL_BASE {
            return None;
        }
        use strum::IntoEnumIterator;
        Self::iter().find(|c| *c as u16 == kind)
    }
}

pub fn is_def_type_kind(kind: u16) -> bool {
    Canonical::try_from_u16(kind).is_some_and(|k| k.is_def_type())
}
