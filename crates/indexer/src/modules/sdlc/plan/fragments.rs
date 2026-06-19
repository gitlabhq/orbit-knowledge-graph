//! The renderable IR fragments of the SDLC ETL lowering, each paired with how
//! it turns into a SQL snippet.
//!
//! Keeping a fragment's variants and its `to_sql` in one place means the meaning
//! of a variant and its SQL live together, instead of being split between an
//! enum and a `match` over in `lower`. `lower` is then pure assembly: it stitches
//! these snippets into full `SELECT`s and never spells SQL for a variant itself.

use std::collections::BTreeMap;

/// A column projected by a node's transform (datalake row → node row).
pub(in crate::modules::sdlc) enum NodeColumn {
    /// Projected verbatim under its own name.
    Identity(String),
    /// Projected from `source` under a different `target` name.
    Rename { source: String, target: String },
    /// An integer enum decoded to its string label via a `CASE`. A non-nullable
    /// column maps NULL to `''`; a nullable one keeps NULL; unknowns fall to
    /// `'unknown'`.
    IntEnum {
        source: String,
        target: String,
        values: BTreeMap<i64, String>,
        nullable: bool,
    },
}

impl NodeColumn {
    pub(in crate::modules::sdlc) fn to_sql(&self) -> String {
        match self {
            NodeColumn::Identity(name) => name.clone(),
            NodeColumn::Rename { source, target } => format!("{source} AS {target}"),
            NodeColumn::IntEnum {
                source,
                target,
                values,
                nullable,
            } => {
                let cases: Vec<String> = values
                    .iter()
                    .map(|(key, value)| format!("WHEN {source} = {key} THEN '{value}'"))
                    .collect();
                let null_case = if *nullable {
                    format!("WHEN {source} IS NULL THEN NULL ")
                } else {
                    format!("WHEN {source} IS NULL THEN '' ")
                };
                format!(
                    "CASE {null_case}{} ELSE 'unknown' END AS {target}",
                    cases.join(" ")
                )
            }
        }
    }
}

/// The source/target id expression of an edge row.
pub(in crate::modules::sdlc) enum EdgeId {
    /// A bare id column: `owner_id`.
    Column(String),
    /// A delimited string column exploded into one id per element.
    Exploded { column: String, delimiter: String },
    /// One field pulled from each struct element of an array column.
    ArrayElement { column: String, field: String },
    /// Each element of a scalar array column, one id per element.
    ArrayUnnest { column: String },
}

impl EdgeId {
    pub(in crate::modules::sdlc) fn to_sql(&self) -> String {
        match self {
            EdgeId::Column(column) => column.clone(),
            EdgeId::Exploded { column, delimiter } => format!(
                "CAST(NULLIF(unnest(string_to_array({column}, '{delimiter}')), '') AS BIGINT)"
            ),
            EdgeId::ArrayElement { column, field } => format!("unnest({column})['{field}']"),
            EdgeId::ArrayUnnest { column } => format!("unnest({column})"),
        }
    }
}

/// The source/target node-kind expression of an edge row.
pub(in crate::modules::sdlc) enum EdgeKind {
    /// A fixed node kind: `'User'`.
    Literal(String),
    /// A kind read from a discriminator column, with raw Rails values rewritten
    /// to ontology names via a `CASE`. An empty mapping renders the bare column.
    Column {
        column: String,
        mapping: BTreeMap<String, String>,
    },
}

impl EdgeKind {
    pub(in crate::modules::sdlc) fn to_sql(&self) -> String {
        match self {
            EdgeKind::Literal(value) => format!("'{value}'"),
            EdgeKind::Column { column, mapping } if mapping.is_empty() => column.clone(),
            EdgeKind::Column { column, mapping } => {
                let cases: Vec<String> = mapping
                    .iter()
                    .map(|(from, to)| format!("WHEN {column} = '{from}' THEN '{to}'"))
                    .collect();
                format!("CASE {} ELSE {column} END", cases.join(" "))
            }
        }
    }
}

/// A predicate applied while transforming source rows into edge rows.
pub(in crate::modules::sdlc) enum EdgeFilter {
    IsNotNull(String),
    NotEmpty(String),
    ArrayNotEmpty(String),
    TypeIn { column: String, types: Vec<String> },
}

impl EdgeFilter {
    pub(in crate::modules::sdlc) fn to_sql(&self) -> String {
        match self {
            EdgeFilter::IsNotNull(column) => format!("({column} IS NOT NULL)"),
            EdgeFilter::NotEmpty(column) => format!("({column} != '')"),
            EdgeFilter::ArrayNotEmpty(column) => format!("(cardinality({column}) > 0)"),
            EdgeFilter::TypeIn { column, types } => {
                let list = types
                    .iter()
                    .map(|t| format!("'{t}'"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{column} IN ({list})")
            }
        }
    }
}

/// A column projected in the extract `SELECT`.
pub(in crate::modules::sdlc) enum ExtractColumn {
    /// Projected verbatim; may itself carry an `expr AS alias`.
    Bare(String),
    /// Cast to text on the way out (e.g. a UUID).
    ToString(String),
    /// A `date` column clamped to ClickHouse `Date32`'s range. Postgres `date`
    /// is wider (1900-01-01..2299-12-31); a single out-of-range row would poison
    /// the whole Arrow batch, so clamp here and let NULL propagate.
    DateClamp(String),
}

impl ExtractColumn {
    pub(in crate::modules::sdlc) fn to_sql(&self) -> String {
        match self {
            ExtractColumn::Bare(name) => name.clone(),
            ExtractColumn::ToString(name) => format!("toString({name}) AS {name}"),
            ExtractColumn::DateClamp(name) => format!(
                "if({name} >= toDate('1900-01-01') AND {name} <= toDate('2299-12-31'), {name}, NULL) AS {name}"
            ),
        }
    }

    /// The raw projection text, used to dedup columns before appending.
    pub(in crate::modules::sdlc) fn name(&self) -> &str {
        match self {
            ExtractColumn::Bare(name)
            | ExtractColumn::ToString(name)
            | ExtractColumn::DateClamp(name) => name,
        }
    }

    /// The output column name the enrichment CTE wrapper joins on: the part
    /// after `AS`, or the whole name.
    pub(in crate::modules::sdlc) fn alias(&self) -> &str {
        self.name()
            .rsplit_once(" AS ")
            .map_or(self.name(), |(_, alias)| alias.trim())
    }
}
