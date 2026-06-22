//! Marker templating for ETL extract SQL.
//!
//! Ontology owns which `{{marker}}` sites are legal and resolves the two whose
//! values are config — `{{watermark_column}}`/`{{deleted_column}}` — at load.
//! The runtime markers `{{filters}}`/`{{limit}}` are left for the indexer to
//! resolve per batch via [`QueryTemplate::render_runtime`].

use crate::OntologyError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Marker {
    WatermarkColumn,
    DeletedColumn,
    Filters,
    /// The whole `LIMIT n` clause, so it elides as a unit with no dangling keyword.
    Limit,
}

impl Marker {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "watermark_column" => Some(Self::WatermarkColumn),
            "deleted_column" => Some(Self::DeletedColumn),
            "filters" => Some(Self::Filters),
            "limit" => Some(Self::Limit),
            _ => None,
        }
    }

    fn token(self) -> &'static str {
        match self {
            Self::WatermarkColumn => "{{watermark_column}}",
            Self::DeletedColumn => "{{deleted_column}}",
            Self::Filters => "{{filters}}",
            Self::Limit => "{{limit}}",
        }
    }

    fn as_runtime(self) -> Option<RuntimeMarker> {
        match self {
            Self::Filters => Some(RuntimeMarker::Filters),
            Self::Limit => Some(RuntimeMarker::Limit),
            Self::WatermarkColumn | Self::DeletedColumn => None,
        }
    }
}

/// The markers resolved per batch by the indexer. The load-time markers are
/// already substituted by the time a template reaches [`QueryTemplate::render_runtime`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMarker {
    Filters,
    Limit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Text(String),
    Marker(Marker),
}

pub enum Resolve {
    Sub(String),
    Keep,
    Elide,
}

/// ETL SQL lexed into text and `{{marker}}` sites; the surrounding SQL stays
/// opaque, which is what lets a page-bounded CTE live in a plain `.sql` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTemplate {
    segments: Vec<Segment>,
    raw: String,
}

impl QueryTemplate {
    pub fn parse(context: &str, sql: &str) -> Result<Self, OntologyError> {
        let mut segments = Vec::new();
        let mut rest = sql;
        while let Some(start) = rest.find("{{") {
            let (lit, after) = rest.split_at(start);
            if !lit.is_empty() {
                segments.push(Segment::Text(lit.to_string()));
            }
            let after = &after[2..];
            let end = after.find("}}").ok_or_else(|| {
                OntologyError::Validation(format!("{context}: unterminated '{{{{' marker"))
            })?;
            let name = after[..end].trim();
            let marker = Marker::from_name(name).ok_or_else(|| {
                OntologyError::Validation(format!(
                    "{context}: unknown placeholder '{{{{{name}}}}}'"
                ))
            })?;
            segments.push(Segment::Marker(marker));
            rest = &after[end + 2..];
        }
        if !rest.is_empty() {
            segments.push(Segment::Text(rest.to_string()));
        }
        Ok(Self {
            segments,
            raw: sql.to_string(),
        })
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// A verbatim extract must drive its own paging, so both runtime markers
    /// are a construction invariant rather than a check left to the caller.
    pub fn parse_full(context: &str, sql: &str) -> Result<Self, OntologyError> {
        let template = Self::parse(context, sql)?;
        if !template.is_full_query() {
            return Err(OntologyError::Validation(format!(
                "{context}: must be a complete extract that drives its own paging \
                 with the {{{{filters}}}} and {{{{limit}}}} markers"
            )));
        }
        Ok(template)
    }

    fn is_full_query(&self) -> bool {
        let mut filters = false;
        let mut limit = false;
        for seg in &self.segments {
            if let Segment::Marker(marker) = seg {
                filters |= *marker == Marker::Filters;
                limit |= *marker == Marker::Limit;
            }
        }
        filters && limit
    }

    pub(crate) fn render(&self, mut resolve: impl FnMut(Marker) -> Resolve) -> String {
        let mut out = String::with_capacity(self.raw.len());
        for seg in &self.segments {
            match seg {
                Segment::Text(text) => out.push_str(text),
                Segment::Marker(marker) => match resolve(*marker) {
                    Resolve::Sub(sql) => out.push_str(&sql),
                    Resolve::Keep => out.push_str(marker.token()),
                    Resolve::Elide => {}
                },
            }
        }
        out
    }

    /// Resolve the runtime paging markers; any load-time marker passes through,
    /// since those are already resolved before a template reaches the indexer.
    pub fn render_runtime(&self, mut resolve: impl FnMut(RuntimeMarker) -> Resolve) -> String {
        self.render(|marker| match marker.as_runtime() {
            Some(runtime) => resolve(runtime),
            None => Resolve::Keep,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keep_paging(marker: Marker) -> Resolve {
        match marker {
            Marker::WatermarkColumn => Resolve::Sub("_siphon_watermark".into()),
            Marker::DeletedColumn => Resolve::Sub("_siphon_deleted".into()),
            Marker::Filters | Marker::Limit => Resolve::Keep,
        }
    }

    #[test]
    fn renders_column_markers_and_keeps_paging_markers() {
        let template = QueryTemplate::parse(
            "test",
            "SELECT {{watermark_column}} AS _version FROM t WHERE 1=1 {{filters}} {{limit}}",
        )
        .unwrap();
        let sql = template.render(keep_paging);
        assert_eq!(
            sql,
            "SELECT _siphon_watermark AS _version FROM t WHERE 1=1 {{filters}} {{limit}}"
        );
    }

    #[test]
    fn render_runtime_resolves_paging_and_passes_load_markers_through() {
        let template =
            QueryTemplate::parse("test", "SELECT {{watermark_column}} {{filters}} {{limit}}")
                .unwrap();
        let sql = template.render_runtime(|_| Resolve::Elide);
        assert_eq!(sql, "SELECT {{watermark_column}}  ");
    }

    #[test]
    fn parse_full_requires_both_paging_markers() {
        assert!(QueryTemplate::parse_full("test", "x {{filters}} y {{limit}}").is_ok());
        let err = QueryTemplate::parse_full("test", "x {{filters}} y").unwrap_err();
        assert!(
            err.to_string().contains("drives its own paging"),
            "got: {err}"
        );
    }

    #[test]
    fn unknown_marker_is_rejected_by_name() {
        let err = QueryTemplate::parse("test", "SELECT {{typo_column}} FROM t").unwrap_err();
        assert!(err.to_string().contains("typo_column"), "got: {err}");
    }

    #[test]
    fn unterminated_marker_is_rejected() {
        let err = QueryTemplate::parse("test", "SELECT {{filters FROM t").unwrap_err();
        assert!(err.to_string().contains("unterminated"), "got: {err}");
    }
}
