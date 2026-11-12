//! Overlaying one configuration layer onto another.

use serde_json::Value;

/// Merges `overlay` into `base`: maps recurse key by key, everything else
/// replaces outright.
///
/// Sequences replace rather than concatenate, so an overlay that sets a list
/// states the whole list. The Helm chart's own config validation deep-merges
/// its rendered overlay over `config/default.yaml` the same way.
pub fn merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                match base.get_mut(&key) {
                    Some(slot) => merge(slot, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (slot, other) => *slot = other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn merged(mut base: Value, overlay: Value) -> Value {
        merge(&mut base, overlay);
        base
    }

    #[test]
    fn maps_merge_key_by_key() {
        assert_eq!(
            merged(json!({"a": 1, "b": 2}), json!({"b": 3, "c": 4})),
            json!({"a": 1, "b": 3, "c": 4})
        );
    }

    #[test]
    fn nested_maps_keep_the_keys_the_overlay_omits() {
        assert_eq!(
            merged(
                json!({"graph": {"url": "http://base", "database": "base"}}),
                json!({"graph": {"database": "overlay"}}),
            ),
            json!({"graph": {"url": "http://base", "database": "overlay"}})
        );
    }

    #[test]
    fn sequences_replace_rather_than_concatenate() {
        assert_eq!(
            merged(json!({"modules": ["a", "b"]}), json!({"modules": ["c"]})),
            json!({"modules": ["c"]})
        );
    }

    #[test]
    fn an_explicit_null_replaces_a_value() {
        assert_eq!(
            merged(
                json!({"health_check_url": "http://base"}),
                json!({"health_check_url": null})
            ),
            json!({"health_check_url": null})
        );
    }

    #[test]
    fn a_scalar_overlaid_with_a_map_becomes_the_map() {
        assert_eq!(
            merged(json!({"tls": false}), json!({"tls": {"enabled": true}})),
            json!({"tls": {"enabled": true}})
        );
    }

    #[test]
    fn a_map_overlaid_with_a_scalar_becomes_the_scalar() {
        assert_eq!(
            merged(json!({"tls": {"enabled": true}}), json!({"tls": false})),
            json!({"tls": false})
        );
    }

    #[test]
    fn an_empty_overlay_changes_nothing() {
        assert_eq!(
            merged(json!({"a": {"b": 1}}), json!({})),
            json!({"a": {"b": 1}})
        );
    }

    #[test]
    fn layers_apply_in_order() {
        let mut document = json!({"query": {"default": {"use_query_cache": true, "ttl": 10}}});
        merge(
            &mut document,
            json!({"query": {"default": {"use_query_cache": false}}}),
        );
        merge(&mut document, json!({"query": {"default": {"ttl": 60}}}));

        assert_eq!(
            document,
            json!({"query": {"default": {"use_query_cache": false, "ttl": 60}}})
        );
    }
}
