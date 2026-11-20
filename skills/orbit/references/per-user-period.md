# Most MRs authored, merged, reviewed or approved per user in a period

Read this for "who authored, merged, reviewed or approved the most MRs in a
period" questions. For query syntax, see [`gql.md`](gql.md).

Use one `count()` aggregation grouped by user, ordered by the count. Do not list
rows and count them yourself: a listing stops at `LIMIT` and returns
`has_more: true`. Anchor on a project, and set the window explicitly:
`merged_at` with `state = 'merged'` for "merged last quarter". Use `created_at`
only when the question asks about opened MRs.

```gql orbit-query
MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 77960826})
WHERE mr.state = 'merged' AND mr.merged_at >= '2026-07-01' AND mr.merged_at < '2026-10-01'
RETURN u.username, u.user_type, count(mr) AS merged_mrs
ORDER BY merged_mrs DESC
LIMIT 100
```

Swap `AUTHORED` for `MERGED`, `REVIEWER` or `APPROVED` to rank mergers,
reviewers or approvers. `AUTHORED` means the user wrote the MR and `MERGED`
means the user merged it. `REVIEWER` means the user was assigned as a reviewer;
the edge carries no review state. `APPROVED` means the user approved the MR.
Leave out `mr.state = 'merged'` on `REVIEWER` and `APPROVED`: the `merged_at`
window already implies a merged MR, and the edge can hold a stale MR state.

```gql orbit-query
MATCH (u:User)-[:MERGED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 77960826})
WHERE mr.state = 'merged' AND mr.merged_at >= '2026-07-01' AND mr.merged_at < '2026-10-01'
RETURN u.username, u.user_type, count(mr) AS merged_by_user
ORDER BY merged_by_user DESC
LIMIT 100
```

```gql orbit-query
MATCH (u:User)-[:REVIEWER]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 77960826})
WHERE mr.merged_at >= '2026-07-01' AND mr.merged_at < '2026-10-01'
RETURN u.username, u.user_type, count(mr) AS reviewed_mrs
ORDER BY reviewed_mrs DESC
LIMIT 100
```

```gql orbit-query
MATCH (u:User)-[:APPROVED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 77960826})
WHERE mr.merged_at >= '2026-07-01' AND mr.merged_at < '2026-10-01'
RETURN u.username, u.user_type, count(mr) AS approved_mrs
ORDER BY approved_mrs DESC
LIMIT 100
```

## Bots

Do not filter on any `User` property other than `u.username` in `WHERE` for
`REVIEWER` or `APPROVED` yet, such as `u.user_type` or `u.state`.
Those properties are copied onto the edge, and the filter gives wrong counts,
positive or negated: `= 'human'` drops most rows and `NOT ... IN` lets bots
through. `AUTHORED` and `MERGED` come from the MR's own author and merge-user
columns, so a `WHERE u.user_type = 'human'` filter is exact there.

For `REVIEWER` and `APPROVED`, return `u.user_type` and drop bots (any value
other than `human`, such as `project_bot` or `duo_code_review_bot`) after the
query. Bot rows use up `LIMIT` slots, so ask for more rows than the answer
needs, then rank after dropping bots. If fewer humans remain than you need and
`has_more` is true, raise `LIMIT` (max 1000). Once
gitlab-org/orbit/knowledge-graph#1559 is fixed, switch to
`WHERE u.user_type = 'human'`.
