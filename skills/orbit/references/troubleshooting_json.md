# Orbit skill troubleshooting: JSON queries

Errors specific to JSON DSL queries. For setup, exit codes, the named-query catalog, and the iteration budget, see [`troubleshooting.md`](troubleshooting.md).

## Empty result body

Usually the query matched no rows. Confirm with a known-good probe in `/tmp/q-min.json`:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "filters": {
        "full_path": {"starts_with": "gitlab-org/"}
      }
    }],
    "limit": 1
  }
}
```

```shell
glab orbit query --response-format raw --file /tmp/q-min.json
```

If this returns a row, the connection works and your other query has no matches.

## Validation errors (HTTP 400, exit 1)

The query did not match the DSL JSON Schema. Common causes:

- `node` (singular) instead of the `nodes` array. Wrap the selector: `"nodes": [{...}]`.
- `neighbors` or single-node `traversal` with more than one entry in `nodes`.
- Multi-node `traversal` without at least two nodes and one relationship.
- `aggregation` without any `aggregations` entries.
- `hops` upper bound or `max_depth` above 3, the server-enforced ceiling.
- `cursor.after` reused after the query changed. The token is bound to the exact query that issued it.
- `allowlist rejected` or `not valid under 'oneOf'` on a `columns` entry. The column is not in the entity's allowlist. Run `glab orbit ontology <Entity>` for the valid list.

Fix: validate against the live schema from `glab orbit dsl`. Full field reference in [`query_language.md`](query_language.md).
