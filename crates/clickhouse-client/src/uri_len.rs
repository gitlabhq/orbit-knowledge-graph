//! Measures the request URI `clickhouse::Query::do_execute` would dispatch, so
//! a caller can reject an over-cap query before `http` rejects it with an opaque
//! "uri too long" and re-fails every retry (KG#881).
//!
//! Two encoders compound: ClickHouse param serialization (an array becomes
//! `['a','b']`, with `'` and `\` backslash-doubled) then `url` percent-encoding,
//! so `'`/`\` cost 6 bytes each and a raw-byte proxy undercounts the wire size.
//! Driven by `ArrowClickHouseClient`, which feeds its own `base_url`,
//! `database`, and scaffold pairs.
//!
//! The dispatch guard (`UriGuard::check()`) is automatic for every query, so an
//! over-cap dispatch always fails with `UriTooLong` rather than freezing. The
//! chunker (`chunk_to_fit`) is opt-in per caller: only a caller that splits a
//! large item list (e.g. `query_routes`) needs it, and it must pass the same
//! `dispatch_settings` the dispatch path appends so its budget matches the
//! guard's measurement.

use serde_json::Value;
use url::Url;

/// Maximum request-URI length, in bytes, that the `http` crate accepts.
///
/// `http`'s `Uri::from_shared` rejects any URI longer than `MAX_LEN =
/// u16::MAX - 1 = 65534` with `ErrorKind::TooLong`; the cap is the whole URI
/// string (scheme + authority + path + query).
pub const MAX_REQUEST_URI_LEN: usize = 65534;

/// Byte length of the request URI a fetch of `sql_params` against
/// `base_url`/`database` serializes to on the wire.
///
/// Reproduces the URL `do_execute` builds: `default_format`, `database`, every
/// `scaffold_pairs` entry, then one `param_<name>` pair per object entry, all
/// via the same `url` crate. The SQL body dispatches separately and does not
/// count toward the URI cap.
pub(crate) fn measure_uri(
    base_url: &str,
    database: &str,
    scaffold_pairs: &[(String, String)],
    sql_params: &Value,
) -> usize {
    let mut url = match Url::parse(base_url) {
        Ok(url) => url,
        // A malformed base URL is a configuration bug, not per-query data, so
        // never panic on the data path.
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

/// `Some(len)` when the URI for `sql_params` would exceed
/// [`MAX_REQUEST_URI_LEN`], `None` when it fits.
///
/// Strictly-over fails; a URI exactly at the cap passes, matching `http`'s
/// `s.len() > MAX_LEN` boundary.
pub(crate) fn overflow(
    base_url: &str,
    database: &str,
    scaffold_pairs: &[(String, String)],
    sql_params: &Value,
) -> Option<usize> {
    let len = measure_uri(base_url, database, scaffold_pairs, sql_params);
    (len > MAX_REQUEST_URI_LEN).then_some(len)
}

/// Split `items` into the fewest chunks whose serialized request URIs stay at
/// or under `max_uri_len` bytes.
///
/// `build_params` renders a sub-slice into the full `Value` the query sends
/// (including fixed params like `root_prefix`). Returns at least one chunk even
/// when the first item alone overflows; the dispatch-time `UriGuard::check()`
/// backstop then rejects that single-item overflow with `UriTooLong`.
pub(crate) fn chunk_to_fit<'a, T>(
    base_url: &str,
    database: &str,
    scaffold_pairs: &[(String, String)],
    items: &'a [T],
    build_params: impl Fn(&[T]) -> Value,
    max_uri_len: usize,
) -> Vec<&'a [T]> {
    if items.is_empty() {
        return Vec::new();
    }

    let full_len = measure_uri(base_url, database, scaffold_pairs, &build_params(items));
    if full_len <= max_uri_len {
        return vec![items];
    }

    let baseline_len = measure_uri(
        base_url,
        database,
        scaffold_pairs,
        &build_params(&items[..0]),
    );
    let budget = max_uri_len.saturating_sub(baseline_len);

    let max_item_cost = items
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let one_len = measure_uri(
                base_url,
                database,
                scaffold_pairs,
                &build_params(&items[i..i + 1]),
            );
            one_len.saturating_sub(baseline_len)
        })
        .max()
        .unwrap_or(1)
        .max(1);

    let mut chunk_size = (budget / max_item_cost).max(1);

    // The solo-measured cost misses the per-item array separator (`,` → `%2C`)
    // that each item after the first adds, so a later chunk of costlier items
    // can overflow even when the first-chunk probe passed. Shrink until all fit.
    while chunk_size > 1 {
        let all_fit = items.chunks(chunk_size).all(|chunk| {
            measure_uri(base_url, database, scaffold_pairs, &build_params(chunk)) <= max_uri_len
        });
        if all_fit {
            break;
        }
        chunk_size -= 1;
    }

    items.chunks(chunk_size).collect()
}

/// A `serde_json::Value` rendered the way `ParamSerializer` would, before
/// percent-encoding. Mirrors `clickhouse::sql::ser` for the JSON value domain
/// datalake params use.
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
/// (`clickhouse::sql::escape::escape`): `\ ' \` \t \n`. These doublings defeat a
/// naive 3x percent-encoding factor, so they must be reproduced to measure the
/// true wire length.
///
/// Mirrors the non-public `clickhouse::sql::ser::ParamSerializer` from
/// **clickhouse 0.15.1** (pinned `clickhouse = "0.15"`);
/// `quoted_element_matches_clickhouse_escape_string_fixture` pins the output to
/// that version's `escape::string` test. Re-verify the `REPLACE` set and the
/// array/quote framing against `sql/ser.rs` + `sql/escape.rs` on any
/// `clickhouse` bump.
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
        client.scaffold_pairs()
    }

    #[test]
    fn matches_the_http_crate_hard_cap() {
        assert_eq!(MAX_REQUEST_URI_LEN, 65534);
    }

    #[test]
    fn no_scaffold_measures_only_format_and_database() {
        let len = measure_uri(BASE, DB, NO_SCAFFOLD, &json!({}));
        let expected = "http://clickhouse:8123/?default_format=ArrowStream&database=datalake".len();
        assert_eq!(len, expected);
    }

    // The client's compression flag and baseline query settings are on the wire
    // and `http` counts them, so the measured scaffold must grow by exactly
    // their encoded `&name=value` framing; omitting them under-reports the URI.
    #[test]
    fn scaffold_pairs_add_the_client_settings_to_the_measure() {
        let base = measure_uri(BASE, DB, NO_SCAFFOLD, &json!({}));
        let scaffold = baseline_scaffold();
        let with_settings = measure_uri(BASE, DB, &scaffold, &json!({}));

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
        let heavy_len = measure_uri(BASE, DB, NO_SCAFFOLD, &json!({ "paths": [raw_heavy] }));
        let plain_len = measure_uri(BASE, DB, NO_SCAFFOLD, &json!({ "paths": [plain] }));
        // Same raw byte count, but `'` is doubled then percent-encoded, so the
        // quote-heavy URI must be materially longer.
        assert!(
            heavy_len > plain_len,
            "quote-heavy {heavy_len} should exceed plain {plain_len}"
        );
    }

    #[test]
    fn overflow_is_strictly_over_the_cap() {
        let small = overflow(BASE, DB, NO_SCAFFOLD, &json!({ "paths": ["x"] }));
        assert_eq!(small, None);

        let huge_path: String = std::iter::repeat_n('a', 70_000).collect();
        let over = overflow(BASE, DB, NO_SCAFFOLD, &json!({ "paths": [huge_path] }));
        assert!(matches!(over, Some(len) if len > MAX_REQUEST_URI_LEN));
    }

    fn routes_build_params(paths: &[&str]) -> Value {
        json!({ "root_prefix": "1/", "paths": paths })
    }

    #[test]
    fn chunk_params_returns_single_chunk_when_all_fit() {
        let paths = vec!["group/project-1", "group/project-2"];
        let chunks = chunk_to_fit(
            BASE,
            DB,
            NO_SCAFFOLD,
            &paths,
            routes_build_params,
            MAX_REQUEST_URI_LEN,
        );
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 2);
    }

    #[test]
    fn chunk_params_returns_empty_vec_for_empty_input() {
        let paths: Vec<&str> = Vec::new();
        let chunks = chunk_to_fit(
            BASE,
            DB,
            NO_SCAFFOLD,
            &paths,
            routes_build_params,
            MAX_REQUEST_URI_LEN,
        );
        assert!(chunks.is_empty());
    }

    #[test]
    fn chunk_params_splits_when_batch_overflows() {
        let path = "a".repeat(200);
        let paths: Vec<&str> = std::iter::repeat_n(path.as_str(), 1_000).collect();
        let chunks = chunk_to_fit(
            BASE,
            DB,
            NO_SCAFFOLD,
            &paths,
            routes_build_params,
            MAX_REQUEST_URI_LEN,
        );
        assert!(chunks.len() > 1);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 1_000);
        for chunk in &chunks {
            let uri_len = measure_uri(BASE, DB, NO_SCAFFOLD, &routes_build_params(chunk));
            assert!(
                uri_len <= MAX_REQUEST_URI_LEN,
                "chunk URI is {uri_len} bytes, over the {MAX_REQUEST_URI_LEN}-byte cap"
            );
        }
    }

    // `'` doubles to `\'` then percent-encodes to `%5C%27` — 6 bytes per raw
    // byte — so a 3×/char estimate would pack chunks that overflow on the wire.
    #[test]
    fn chunk_params_handles_adversarial_quote_heavy_paths() {
        let quote_heavy: String = std::iter::repeat_n('\'', 300).collect();
        let paths: Vec<&str> = std::iter::repeat_n(quote_heavy.as_str(), 200).collect();

        let chunks = chunk_to_fit(
            BASE,
            DB,
            NO_SCAFFOLD,
            &paths,
            routes_build_params,
            MAX_REQUEST_URI_LEN,
        );

        assert!(
            chunks.len() > 1,
            "adversarial input must require multiple chunks"
        );
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 200);

        for (i, chunk) in chunks.iter().enumerate() {
            let uri_len = measure_uri(BASE, DB, NO_SCAFFOLD, &routes_build_params(chunk));
            assert!(
                uri_len <= MAX_REQUEST_URI_LEN,
                "chunk {i} URI is {uri_len} bytes, over the {MAX_REQUEST_URI_LEN}-byte cap"
            );
        }
    }

    #[test]
    fn chunk_params_with_low_cap_produces_single_item_chunks() {
        let paths = vec!["group/project-1", "group/project-2", "group/project-3"];
        let tiny_cap = measure_uri(BASE, DB, NO_SCAFFOLD, &routes_build_params(&paths[..1])) + 1;

        let chunks = chunk_to_fit(BASE, DB, NO_SCAFFOLD, &paths, routes_build_params, tiny_cap);

        assert_eq!(chunks.len(), 3);
        for chunk in &chunks {
            assert_eq!(chunk.len(), 1);
        }
    }

    #[test]
    fn chunk_params_single_oversized_item_returns_one_chunk() {
        let huge_path: String = std::iter::repeat_n('a', 70_000).collect();
        let paths = vec![huge_path.as_str()];

        assert!(
            measure_uri(BASE, DB, NO_SCAFFOLD, &routes_build_params(&paths)) > MAX_REQUEST_URI_LEN,
            "precondition: the single item must overflow alone"
        );

        let chunks = chunk_to_fit(
            BASE,
            DB,
            NO_SCAFFOLD,
            &paths,
            routes_build_params,
            MAX_REQUEST_URI_LEN,
        );

        assert_eq!(chunks.len(), 1, "no smaller split is possible");
        assert_eq!(chunks[0].len(), 1);
    }

    // The solo-measured max_item_cost misses the array separator (`,` → `%2C`,
    // +3 bytes) each item after the first adds, so a cheap first item followed
    // by costlier items let chunk 0 fit but a later full-size chunk overflow.
    #[test]
    fn chunk_params_mixed_cost_items_all_chunks_fit() {
        let cheap: String = std::iter::repeat_n('\'', 361).collect();
        let costly: String = std::iter::repeat_n('\'', 362).collect();
        let mut paths: Vec<&str> = Vec::with_capacity(4001);
        paths.push(cheap.as_str());
        for _ in 0..4000 {
            paths.push(costly.as_str());
        }

        let chunks = chunk_to_fit(
            BASE,
            DB,
            NO_SCAFFOLD,
            &paths,
            routes_build_params,
            MAX_REQUEST_URI_LEN,
        );

        assert!(chunks.len() > 1);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 4001);

        for (i, chunk) in chunks.iter().enumerate() {
            let uri_len = measure_uri(BASE, DB, NO_SCAFFOLD, &routes_build_params(chunk));
            assert!(
                uri_len <= MAX_REQUEST_URI_LEN,
                "chunk {i} ({} items) URI is {uri_len} bytes, over the {MAX_REQUEST_URI_LEN}-byte cap",
                chunk.len(),
            );
        }
    }
}
