# Most reviews, approvals or merged MRs per user in a period

Read this for "who reviewed, approved or merged the most MRs in a period"
questions. For query syntax, see [`gql.md`](gql.md).

Use one `count()` aggregation grouped by user, ordered by the count. Do not list
rows and count them yourself: a listing stops at `LIMIT` and returns
`has_more: true`. Anchor on a project or group, and set the window explicitly:
`merged_at` with `state = 'merged'` for "merged last quarter". Use `created_at`
only when the question asks about opened MRs.

```gql orbit-query
MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 77960826})
WHERE mr.state = 'merged' AND mr.merged_at >= '2026-07-01' AND mr.merged_at < '2026-10-01'
RETURN u.username, u.user_type, count(mr) AS merged_mrs
ORDER BY merged_mrs DESC
LIMIT 100
```

Swap `AUTHORED` for `REVIEWER` or `APPROVED` to rank reviewers or approvers.
`REVIEWER` means the user was assigned as a reviewer; the edge carries no review
state. `APPROVED` means the user approved the MR.

```gql orbit-query
MATCH (u:User)-[:REVIEWER]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 77960826})
WHERE mr.state = 'merged' AND mr.merged_at >= '2026-07-01' AND mr.merged_at < '2026-10-01'
RETURN u.username, u.user_type, count(mr) AS reviewed_mrs
ORDER BY reviewed_mrs DESC
LIMIT 100
```

```gql orbit-query
MATCH (u:User)-[:APPROVED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 77960826})
WHERE mr.state = 'merged' AND mr.merged_at >= '2026-07-01' AND mr.merged_at < '2026-10-01'
RETURN u.username, u.user_type, count(mr) AS approved_mrs
ORDER BY approved_mrs DESC
LIMIT 100
```

## Bots

Do not filter on any `User` property in `WHERE` for `REVIEWER` or `APPROVED`
yet, such as `u.user_type`, `u.state` or `u.is_admin`. Those properties are
copied onto the edge, and the filter gives wrong counts, positive or negated:
`= 'human'` drops most rows and `NOT ... IN` lets bots through. Filtering by
`u.username` is safe.

Instead, return `u.user_type` and drop bots (any value other than `human`, such
as `project_bot` or `duo_code_review_bot`) after the query. Bot rows use up
`LIMIT` slots, so ask for more rows than the answer needs, then rank after
dropping bots. Once gitlab-org/orbit/knowledge-graph#1559 and
gitlab-org/orbit/knowledge-graph#1439 are fixed, switch to
`WHERE u.user_type = 'human'`.
