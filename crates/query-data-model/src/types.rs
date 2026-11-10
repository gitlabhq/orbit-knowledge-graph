use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    String,
    Int,
    Float,
    Bool,
    Date,
    DateTime,
    Enum,
    Uuid,
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <&str>::deserialize(deserializer)?;
        match s {
            "string" => Ok(Self::String),
            "int64" | "int" | "integer" => Ok(Self::Int),
            "float64" | "float" | "double" => Ok(Self::Float),
            "boolean" | "bool" => Ok(Self::Bool),
            "date" => Ok(Self::Date),
            "timestamp" | "datetime" => Ok(Self::DateTime),
            "enum" => Ok(Self::Enum),
            "uuid" => Ok(Self::Uuid),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &[
                    "string",
                    "int64",
                    "int",
                    "integer",
                    "float64",
                    "float",
                    "double",
                    "boolean",
                    "bool",
                    "date",
                    "timestamp",
                    "datetime",
                    "enum",
                    "uuid",
                ],
            )),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::String => write!(f, "String"),
            DataType::Int => write!(f, "Int"),
            DataType::Float => write!(f, "Float"),
            DataType::Bool => write!(f, "Bool"),
            DataType::Date => write!(f, "Date"),
            DataType::DateTime => write!(f, "DateTime"),
            DataType::Enum => write!(f, "Enum"),
            DataType::Uuid => write!(f, "Uuid"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnumType {
    /// Integer values that map to string labels (requires CASE transformation).
    #[default]
    Int,
    /// Already stored as strings with constrained values (pass-through, no transformation).
    String,
}

/// Filter selectivity hint for the query planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldSelectivity {
    /// Few distinct values (enums, booleans). Filtering narrows weakly.
    Low,
    /// Many distinct values (IDs, strings, timestamps). Filtering narrows effectively.
    #[default]
    High,
}

impl FieldSelectivity {
    pub fn from_data_type(dt: DataType) -> Self {
        match dt {
            DataType::Enum | DataType::Bool => Self::Low,
            _ => Self::High,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraversalPathKind {
    Id,
    FullPath,
}

/// How a resolved `traversal_path` prefix relates to an edge variant's two
/// endpoints. `namespace_anchor` and `same_namespace` propagate the prefix
/// across the edge; `prune_to_source`/`prune_to_target` only confine the
/// edge's own scan to the named endpoint's namespace, without propagating, so
/// the other endpoint may be a global hub (User/Runner/Label).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeVariantScope {
    NamespaceAnchor,
    SameNamespace,
    PruneToSource,
    PruneToTarget,
}

impl EdgeVariantScope {
    #[must_use]
    pub fn is_scope_preserving(self) -> bool {
        matches!(self, Self::NamespaceAnchor | Self::SameNamespace)
    }

    #[must_use]
    pub fn propagates_to_source(self, relationship: &str, source: &str) -> bool {
        self.is_scope_preserving() && !(relationship == "CONTAINS" && source == "Group")
    }

    /// A `same_namespace` source can live in another namespace on prod
    /// (parent Group of CONTAINS, Package of HAS_VULNERABILITY).
    #[must_use]
    pub fn edge_path_is_target_path(self) -> bool {
        matches!(self, Self::SameNamespace | Self::PruneToTarget)
    }

    #[must_use]
    pub fn edge_path_is_source_path(self) -> bool {
        matches!(self, Self::PruneToSource)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Global,
    Namespace,
}

/// GitLab access levels. Discriminants match `Gitlab::Access` in Rails so
/// that YAML strings, JWT claim integers, and compiler-side comparisons all
/// agree. `SecurityManager` intentionally sits between `Reporter` and
/// `Developer` (25) to match the role's hybrid scope: broader than reporter
/// for security resources, narrower than developer for code changes.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredRole {
    Guest = 10,
    Reporter = 20,
    SecurityManager = 25,
    Developer = 30,
    Maintainer = 40,
    Owner = 50,
}

impl RequiredRole {
    pub fn as_access_level(self) -> u32 {
        self as u32
    }
}
