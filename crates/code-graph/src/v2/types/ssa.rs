#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParseValue {
    LocalDef(u32),
    ImportRef(u32),
    Type(smol_str::SmolStr),
    Opaque,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArgType {
    Unknown,
    Named(smol_str::SmolStr),
}
