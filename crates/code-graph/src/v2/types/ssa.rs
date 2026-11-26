#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParseValue {
    LocalDef(u32),
    ImportRef(u32),
    Type(smol_str::SmolStr),
    Opaque,
}
