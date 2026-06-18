//! Centralized URI-length guard for datalake queries.
//!
//! Every batched datalake lookup turns its params into named ClickHouse
//! `param_*` settings that `clickhouse 0.15` percent-encodes into the request
//! URL's query string at dispatch (`Query::do_execute`). The `http` crate caps
//! a URI at [`MAX_REQUEST_URI_LEN`] bytes and rejects anything longer with an
//! opaque `uri too long` — the production stall in KG#881, where a batched
//! routes lookup overflowed the cap and re-failed every dispatch.
//!
//! This module measures the **encoded** URI a query will produce — the same two
//! encoders the client applies, in order — so a caller can reject an over-limit
//! request loudly *before* it reaches `hyper`/`http`:
//!
//! 1. **ClickHouse param serialization** (`clickhouse::sql::ser::ParamSerializer`):
//!    a top-level string is backslash-escaped (no surrounding quotes); an array
//!    becomes `['a','b',…]` with each element single-quoted and `\ ' \` \t \n`
//!    backslash-escaped; numbers/bools render as-is.
//! 2. **URL percent-encoding** (`url::form_urlencoded`, via `query_pairs_mut`):
//!    the serialized literal is percent-encoded into `&param_<name>=…`, exactly
//!    as `Query::do_execute` builds the dispatched URL.
//!
//! Measuring the encoded length (not the raw param bytes) is the point: `'`/`\`
//! are doubled by stage 1 *then* tripled by stage 2, so a naive raw-byte proxy
//! under-counts the real wire size (KG !1822 review, §3).

use serde_json::Value;
use url::Url;

/// Maximum request-URI length, in bytes, that the `http` crate accepts.
///
/// `http`'s `Uri::from_shared` rejects any URI whose serialized length exceeds
/// `MAX_LEN = (u16::MAX - 1) = 65534` with `ErrorKind::TooLong` ("uri too
/// long"). This is the single source of truth for the cap; the limit is the
/// length of the whole URI string (scheme + authority + path + query), which is
/// what we measure below.
pub const MAX_REQUEST_URI_LEN: usize = 65534;

/// Compute the byte length of the request URI that a fetch of `sql_params`
/// against `base_url`/`database` will serialize to on the wire.
///
/// This mirrors `clickhouse::query::Query::do_execute`: it appends
/// `default_format`, `database`, and one `param_<name>` pair per object entry to
/// the URL via the same `url` crate, then returns `url.as_str().len()`. The SQL
/// body is sent separately and does not count toward the URI cap, so it is not
/// included.
///
/// Non-object `sql_params` carry no `param_*` pairs, so only the base URL +
/// format/database scaffold is measured.
pub fn request_uri_len(base_url: &str, database: &str, sql_params: &Value) -> usize {
    let mut url = match Url::parse(base_url) {
        Ok(url) => url,
        // A malformed base URL is a configuration bug, not a per-query data
        // problem; fall back to the raw length so the guard never panics on the
        // data path. In practice `base_url` is the validated client URL.
        Err(_) => return base_url.len(),
    };

    {
        let mut pairs = url.query_pairs_mut();
        pairs.clear();
        pairs.append_pair("default_format", "ArrowStream");
        pairs.append_pair("database", database);

        if let Value::Object(map) = sql_params {
            let mut serialized = String::new();
            for (name, value) in map {
                serialized.clear();
                write_param(&value_into_param(value), &mut serialized);
                pairs.append_pair(&format!("param_{name}"), &serialized);
            }
        }
    }

    url.as_str().len()
}

/// Whether the request URI for `sql_params` would exceed [`MAX_REQUEST_URI_LEN`].
///
/// Returns `Some(len)` with the measured length when over the cap, `None` when
/// it fits (`len <= MAX_REQUEST_URI_LEN`). Strictly-over fails; a URI exactly at
/// the cap passes, matching `http`'s `s.len() > MAX_LEN` boundary.
pub fn request_uri_overflow(base_url: &str, database: &str, sql_params: &Value) -> Option<usize> {
    let len = request_uri_len(base_url, database, sql_params);
    (len > MAX_REQUEST_URI_LEN).then_some(len)
}

/// A `serde_json::Value` rendered the way `ParamSerializer` would, before
/// percent-encoding. Mirrors `clickhouse::sql::ser` for the JSON value domain
/// that flows through datalake params (the only `Value` variants used there).
enum ParamRepr {
    /// Top-level scalar/string: written unquoted, ClickHouse-escaped.
    Scalar(String),
    /// `\N`, the ClickHouse NULL param literal.
    Null,
    /// An array literal `[e0,e1,…]`; elements carry their own nested repr.
    Array(Vec<ParamRepr>),
    /// A nested array element string: single-quoted and ClickHouse-escaped.
    QuotedScalar(String),
}

fn value_into_param(value: &Value) -> ParamRepr {
    match value {
        Value::Null => ParamRepr::Null,
        Value::Bool(b) => ParamRepr::Scalar(b.to_string()),
        Value::Number(n) => ParamRepr::Scalar(n.to_string()),
        Value::String(s) => ParamRepr::Scalar(s.clone()),
        Value::Array(items) => ParamRepr::Array(items.iter().map(value_into_nested).collect()),
        // Objects are not valid ClickHouse params; `to_string()` matches the
        // fallback path in `bind_param` for unexpected shapes.
        Value::Object(_) => ParamRepr::Scalar(value.to_string()),
    }
}

/// Nested (inside-array) rendering: strings are single-quoted, unlike top level.
fn value_into_nested(value: &Value) -> ParamRepr {
    match value {
        Value::String(s) => ParamRepr::QuotedScalar(s.clone()),
        Value::Array(items) => ParamRepr::Array(items.iter().map(value_into_nested).collect()),
        other => value_into_param(other),
    }
}

fn write_param(repr: &ParamRepr, out: &mut String) {
    match repr {
        ParamRepr::Scalar(s) => escape(s, out),
        ParamRepr::Null => out.push_str(r"\N"),
        ParamRepr::QuotedScalar(s) => {
            out.push('\'');
            escape(s, out);
            out.push('\'');
        }
        ParamRepr::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_param(item, out);
            }
            out.push(']');
        }
    }
}

/// Backslash-escape the characters ClickHouse's string escaper doubles
/// (`clickhouse::sql::escape::escape`): `\ ' \` \t \n`. These doublings are what
/// defeat a naive 3x percent-encoding factor, so they must be reproduced here to
/// measure the true wire length.
fn escape(src: &str, out: &mut String) {
    const REPLACE: &[char] = &['\\', '\'', '`', '\t', '\n'];
    let mut rest = src;
    while let Some(idx) = rest.find(REPLACE) {
        let (before, after) = rest.split_at(idx);
        out.push_str(before);
        out.push('\\');
        out.push_str(&after[..1]);
        rest = &after[1..];
    }
    out.push_str(rest);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const BASE: &str = "http://clickhouse:8123";
    const DB: &str = "datalake";

    #[test]
    fn matches_the_http_crate_hard_cap() {
        assert_eq!(MAX_REQUEST_URI_LEN, 65534);
    }

    #[test]
    fn empty_params_measure_only_the_scaffold() {
        let len = request_uri_len(BASE, DB, &json!({}));
        let expected = "http://clickhouse:8123/?default_format=ArrowStream&database=datalake".len();
        assert_eq!(len, expected);
    }

    #[test]
    fn array_params_render_clickhouse_array_literals() {
        let mut out = String::new();
        write_param(
            &value_into_param(&json!(["group/project", "other/repo"])),
            &mut out,
        );
        assert_eq!(out, "['group/project','other/repo']");
    }

    #[test]
    fn escapes_quotes_and_backslashes_like_clickhouse() {
        let mut out = String::new();
        write_param(&value_into_param(&json!(["a'b", "c\\d"])), &mut out);
        assert_eq!(out, r"['a\'b','c\\d']");
    }

    // Pins the quoted-element escaping to clickhouse 0.15's own `escape::string`
    // fixture (`it_escapes_string`): the same input must produce byte-identical
    // output, or our wire-length measurement drifts from the real serializer.
    #[test]
    fn quoted_element_matches_clickhouse_escape_string_fixture() {
        let mut out = String::new();
        write_param(&value_into_nested(&json!(r"f\o'o '' b\'ar'")), &mut out);
        assert_eq!(out, r"'f\\o\'o \'\' b\\\'ar\''");
    }

    #[test]
    fn top_level_string_param_is_unquoted() {
        let mut out = String::new();
        write_param(&value_into_param(&json!("group/project")), &mut out);
        assert_eq!(out, "group/project");
    }

    #[test]
    fn null_renders_clickhouse_null_literal() {
        let mut out = String::new();
        write_param(&value_into_param(&Value::Null), &mut out);
        assert_eq!(out, r"\N");
    }

    #[test]
    fn quote_heavy_input_measures_larger_encoded_than_raw() {
        let raw_heavy: String = std::iter::repeat_n('\'', 500).collect();
        let plain: String = std::iter::repeat_n('a', 500).collect();
        let heavy_len = request_uri_len(BASE, DB, &json!({ "paths": [raw_heavy] }));
        let plain_len = request_uri_len(BASE, DB, &json!({ "paths": [plain] }));
        // Same raw byte count, but `'` is doubled then percent-encoded, so the
        // quote-heavy URI must be materially longer — the exact effect a
        // raw-byte proxy would miss.
        assert!(
            heavy_len > plain_len,
            "quote-heavy {heavy_len} should exceed plain {plain_len}"
        );
    }

    #[test]
    fn overflow_is_strictly_over_the_cap() {
        let small = request_uri_overflow(BASE, DB, &json!({ "paths": ["x"] }));
        assert_eq!(small, None);

        let huge_path: String = std::iter::repeat_n('a', 70_000).collect();
        let over = request_uri_overflow(BASE, DB, &json!({ "paths": [huge_path] }));
        assert!(matches!(over, Some(len) if len > MAX_REQUEST_URI_LEN));
    }
}
