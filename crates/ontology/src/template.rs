//! Marker templating for ETL extract SQL.
//!
//! This is a grammar, not a resolver: ontology owns which `{{marker}}` sites
//! are legal in a `config/ontology/**/*.sql` file, but only resolves the
//! markers whose values are ontology config — `{{watermark_column}}` and
//! `{{deleted_column}}` (from `etl_settings`). The runtime paging markers
//! `{{filters}}` and `{{batch_size}}` are recognized here so an unknown marker
//! is rejected by name, but their *values* are the indexer extract phase's to
//! compute per batch, so ontology keeps them verbatim and passes them through.

use crate::OntologyError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Marker {
    WatermarkColumn,
    DeletedColumn,
    Filters,
    BatchSize,
}

impl Marker {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "watermark_column" => Some(Self::WatermarkColumn),
            "deleted_column" => Some(Self::DeletedColumn),
            "filters" => Some(Self::Filters),
            "batch_size" => Some(Self::BatchSize),
            _ => None,
        }
    }

    fn token(self) -> &'static str {
        match self {
            Self::WatermarkColumn => "{{watermark_column}}",
            Self::DeletedColumn => "{{deleted_column}}",
            Self::Filters => "{{filters}}",
            Self::BatchSize => "{{batch_size}}",
        }
    }
}

#[derive(Debug, Clone)]
enum Segment {
    Text(String),
    Marker(Marker),
}

/// How [`QueryTemplate::render`] resolves one marker site.
pub(crate) enum Resolve {
    Sub(String),
    Keep,
}

/// ETL SQL lexed into text and `{{marker}}` sites. Only marker boundaries are
/// parsed; the surrounding SQL is opaque, which is what lets a page-bounded
/// CTE live in a plain `.sql` file.
#[derive(Debug, Clone)]
pub(crate) struct QueryTemplate {
    segments: Vec<Segment>,
    len_hint: usize,
}

impl QueryTemplate {
    pub(crate) fn parse(context: &str, sql: &str) -> Result<Self, OntologyError> {
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
            len_hint: sql.len(),
        })
    }

    pub(crate) fn is_full_query(&self) -> bool {
        let mut filters = false;
        let mut batch_size = false;
        for seg in &self.segments {
            if let Segment::Marker(marker) = seg {
                filters |= *marker == Marker::Filters;
                batch_size |= *marker == Marker::BatchSize;
            }
        }
        filters && batch_size
    }

    pub(crate) fn render(&self, mut resolve: impl FnMut(Marker) -> Resolve) -> String {
        let mut out = String::with_capacity(self.len_hint);
        for seg in &self.segments {
            match seg {
                Segment::Text(text) => out.push_str(text),
                Segment::Marker(marker) => match resolve(*marker) {
                    Resolve::Sub(sql) => out.push_str(&sql),
                    Resolve::Keep => out.push_str(marker.token()),
                },
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keep_paging(marker: Marker) -> Resolve {
        match marker {
            Marker::WatermarkColumn => Resolve::Sub("_siphon_watermark".into()),
            Marker::DeletedColumn => Resolve::Sub("_siphon_deleted".into()),
            Marker::Filters | Marker::BatchSize => Resolve::Keep,
        }
    }

    #[test]
    fn renders_column_markers_and_keeps_paging_markers() {
        let template = QueryTemplate::parse(
            "test",
            "SELECT {{watermark_column}} AS _version FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .unwrap();
        let sql = template.render(keep_paging);
        assert_eq!(
            sql,
            "SELECT _siphon_watermark AS _version FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}"
        );
    }

    #[test]
    fn full_query_requires_both_paging_markers() {
        assert!(
            QueryTemplate::parse("test", "x {{filters}} y {{batch_size}}")
                .unwrap()
                .is_full_query()
        );
        assert!(
            !QueryTemplate::parse("test", "x {{filters}} y")
                .unwrap()
                .is_full_query()
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
