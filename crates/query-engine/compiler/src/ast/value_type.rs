#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueType {
    Bool,
    Int64,
    UInt64,
    Float64,
    String,
    Date,
    DateTime,
    Nullable(Box<ValueType>),
    List(Box<ValueType>),
    Record(Vec<ValueType>),
}
