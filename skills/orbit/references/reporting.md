# Reporting Orbit results

Orbit answers are graph queries against ClickHouse, not an authoritative source
of truth. Always present results with their coverage caveats. See
[`SKILL.md`](../SKILL.md) for query construction.

## Rules

1. **Surface known coverage gaps inline.** If the query falls into one of the
   documented gap classes — historical file coverage (`HAS_LATEST_DIFF` vs
   `HAS_DIFF`), time-bounded aggregates — append a one-line caveat to the
   answer, not a buried footnote.
2. **Show the query.** Include the JSON request body (collapsed if long) so the
   user can audit the traversal.
3. **Do not invent a "Methodology" header that implies rigor the underlying
   data does not support.** A "Methodology" section is appropriate when the
   query itself is non-obvious; it is not a substitute for coverage caveats.

## Example

An answer to "how many pipelines ran for MR !235291?" should look like:

> Orbit returned 16 pipelines for MR !235291 (filtered by
> `source = "merge_request_event"`). This matches the MR Pipelines tab.

Not:

> **Methodology**
> I queried `MergeRequest --TRIGGERED--> Pipeline` and got 98 results,
> broken down by status: ...
