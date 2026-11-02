# Reporting Orbit results

Orbit answers are graph queries against ClickHouse, not an authoritative source
of truth. Always present results with their coverage caveats. See
[`SKILL.md`](../SKILL.md) for query construction.

## Rules

1. **Surface known coverage gaps inline.** Documented gap classes include
   historical file coverage (`HAS_LATEST_DIFF` vs `HAS_DIFF`) and time-bounded
   aggregates. When a query falls into one, append a one-line caveat to the
   answer, not a buried footnote.
2. **Treat CI recency as a coverage gap.** CI data lags by a few hours, so a
   pipeline or job from the last few hours may be missing or partly populated.
   Say it is not indexed yet. Never report it as 0 job-minutes.
3. **Drop rows Orbit lacks.** When a row has no data in the graph, leave it out
   of totals and averages and name it as a coverage gap. Do not count it as 0.
4. **Name what shaped the result.** Say in one clause which filter or
   traversal produced it. Give the full query only when asked.
5. **Do not invent a "Methodology" header that implies rigor the data
   lacks.** Use one only when the query itself is non-obvious. It is not a
   substitute for coverage caveats.

## Example

An answer to "how many pipelines ran for MR !235291?" should look like:

> Orbit returned 16 pipelines for MR !235291 (filtered by
> `source = "merge_request_event"`). This matches the MR Pipelines tab.

Not:

> **Methodology**
> I queried `MergeRequest --TRIGGERED--> Pipeline` and got 98 results,
> broken down by status: ...
