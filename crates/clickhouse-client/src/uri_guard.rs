//! Centralized URI-length guard for datalake queries.
//!
//! Every batched datalake lookup turns its params into named ClickHouse
//! `param_*` settings that `clickhouse 0.15` percent-encodes into the request
//! URL at dispatch (`Query::do_execute`). The `http` crate caps a URI at
//! [`MAX_REQUEST_URI_LEN`] bytes and rejects anything longer with an opaque `uri
//! too long`: the production stall in KG#881, where a batched routes lookup
//! overflowed the cap and re-failed every dispatch.
//!
//! [`request_uri_len`] reproduces that URL so a caller can reject an over-limit
//! request before it reaches `hyper`/`http`. Two compounding encoders make the
//! **encoded** length, not the raw param bytes, the thing to measure:
//!
//! 1. ClickHouse param serialization (`clickhouse::sql::ser::ParamSerializer`):
//!    an array becomes `['a','b',...]`, with `'` and `\` backslash-doubled.
//! 2. URL percent-encoding (`url`, via `query_pairs_mut`), as `do_execute`
//!    builds the dispatched URL.
//!
//! `'` and `\` are doubled by stage 1 *then* tripled by stage 2 (6 bytes per raw
//! byte), so a naive raw-byte proxy undercounts the wire size (KG !1822 review,
//! §3). The measured URL also includes the client's compression flag and query
//! settings (`scaffold_pairs`), which `do_execute` appends and `http` counts.

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
/// Reproduces the URL `clickhouse::query::Query::do_execute` builds: it appends
/// `default_format`, `database`, every pair in `scaffold_pairs` (the client's
/// compression flag, baseline query settings, and any `session_settings` /
/// `roles`; see [`ArrowClickHouseClient::request_scaffold_pairs`]), then one
/// `param_<name>` pair per object entry, all via the same `url` crate, and
/// returns `url.as_str().len()`. Omitting `scaffold_pairs` would undercount the
/// real URI (the B1 gap: ~206 bytes of baseline settings plus the open-ended
/// session settings the client sends but a params-only measure ignores). The SQL
/// body is dispatched separately and does not count toward the URI cap.
///
/// [`ArrowClickHouseClient::request_scaffold_pairs`]: crate::ArrowClickHouseClient::request_scaffold_pairs
pub fn request_uri_len(
    base_url: &str,
    database: &str,
    scaffold_pairs: &[(String, String)],
    sql_params: &Value,
) -> usize {
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
        for (name, value) in scaffold_pairs {
            pairs.append_pair(name, value);
        }

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
pub fn request_uri_overflow(
    base_url: &str,
    database: &str,
    scaffold_pairs: &[(String, String)],
    sql_params: &Value,
) -> Option<usize> {
    let len = request_uri_len(base_url, database, scaffold_pairs, sql_params);
    (len > MAX_REQUEST_URI_LEN).then_some(len)
}

/// A `serde_json::Value` rendered the way `ParamSerializer` would, before
/// percent-encoding. Mirrors `clickhouse::sql::ser` for the JSON value domain
/// that flows through datalake params (the only `Value` variants used there).
enum ParamRepr {
    Scalar(String),
    Null,
    Array(Vec<ParamRepr>),
    /// Nested strings are single-quoted, unlike top-level [`ParamRepr::Scalar`].
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
///
/// This and the `write_param`/`value_into_*` replica mirror
/// `clickhouse::sql::ser::ParamSerializer`, which is not public, so it cannot be
/// round-tripped against directly. Mirrored from **clickhouse 0.15.1** (pinned
/// `clickhouse = "0.15"`); `quoted_element_matches_clickhouse_escape_string_fixture`
/// pins the output to that version's `escape::string` test. Re-verify the
/// `REPLACE` set and the array/quote framing against `sql/ser.rs` + `sql/escape.rs`
/// on any `clickhouse` bump.
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
    const NO_SCAFFOLD: &[(String, String)] = &[];

    fn baseline_scaffold() -> Vec<(String, String)> {
        let client = crate::ArrowClickHouseClient::new(
            BASE,
            DB,
            "default",
            None,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        );
        client.request_scaffold_pairs()
    }

    #[test]
    fn matches_the_http_crate_hard_cap() {
        assert_eq!(MAX_REQUEST_URI_LEN, 65534);
    }

    #[test]
    fn no_scaffold_measures_only_format_and_database() {
        let len = request_uri_len(BASE, DB, NO_SCAFFOLD, &json!({}));
        let expected = "http://clickhouse:8123/?default_format=ArrowStream&database=datalake".len();
        assert_eq!(len, expected);
    }

    // B1 regression: the client's compression flag and baseline query settings
    // are on the wire and `http` counts them, so the measured scaffold must
    // grow by exactly their encoded `&name=value` framing. Pins that the guard
    // includes them (omitting them is what let the guard under-report the URI).
    #[test]
    fn scaffold_pairs_add_the_client_settings_to_the_measure() {
        let base = request_uri_len(BASE, DB, NO_SCAFFOLD, &json!({}));
        let scaffold = baseline_scaffold();
        let with_settings = request_uri_len(BASE, DB, &scaffold, &json!({}));

        let expected_extra: usize = scaffold
            .iter()
            .map(|(k, v)| format!("&{k}={v}").len())
            .sum();
        assert_eq!(with_settings - base, expected_extra);
        assert!(
            scaffold.iter().any(|(k, _)| k == "compress"),
            "compression flag must be measured"
        );
        assert!(
            scaffold
                .iter()
                .any(|(k, _)| k == "output_format_arrow_string_as_string"),
            "baseline settings must be measured"
        );
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
        let heavy_len = request_uri_len(BASE, DB, NO_SCAFFOLD, &json!({ "paths": [raw_heavy] }));
        let plain_len = request_uri_len(BASE, DB, NO_SCAFFOLD, &json!({ "paths": [plain] }));
        // Same raw byte count, but `'` is doubled then percent-encoded, so the
        // quote-heavy URI must be materially longer, the exact effect a
        // raw-byte proxy would miss.
        assert!(
            heavy_len > plain_len,
            "quote-heavy {heavy_len} should exceed plain {plain_len}"
        );
    }

    #[test]
    fn overflow_is_strictly_over_the_cap() {
        let small = request_uri_overflow(BASE, DB, NO_SCAFFOLD, &json!({ "paths": ["x"] }));
        assert_eq!(small, None);

        let huge_path: String = std::iter::repeat_n('a', 70_000).collect();
        let over = request_uri_overflow(BASE, DB, NO_SCAFFOLD, &json!({ "paths": [huge_path] }));
        assert!(matches!(over, Some(len) if len > MAX_REQUEST_URI_LEN));
    }
}
