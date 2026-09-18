---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Find critical and high severity vulnerabilities across your organization and trace how they got there.
title: Trace security risk to its source
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

Follow these steps when you need a remediation shortlist rather than a full vulnerability
report.

- Time estimate: 15-30 minutes
- Level: Intermediate

## The challenge

Vulnerability reports show what was found, but not which projects carry the most risk or which
change introduced a finding.

## The approach

Find the open findings, measure the spread, then trace each one to its source.
Use GitLab Duo Chat or another agent connected to GitLab Orbit.
Replace `<my-org>` with your top-level group.

## Prerequisites

- A top-level group that GitLab Orbit Remote indexes.
- For an external agent, a connection through the [MCP server](../access/mcp.md) or the
  [`glab` CLI](../access/glab.md).

### Step 1: Find the open findings

Use your agent to collect the findings that still need work:

```plaintext
Using GitLab Orbit, find the critical and high severity vulnerabilities across
<my-org> that are still detected, and show me which projects they affect.
```

Expected outcome: The open critical and high severity findings, with their projects.

Find all critical and high vulnerabilities:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "columns": ["title", "severity", "state", "report_type"],
      "filters": {
        "severity": {"in": ["critical", "high"]},
        "state": "detected"
      }
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "limit": 50
}
```

### Step 2: Measure the spread

Use your agent to see where the risk concentrates:

```plaintext
Count the detected vulnerabilities by project and by severity, so I can see which
projects carry the most risk.
```

Expected outcome: Two counts, one by project and one by severity.

Count vulnerabilities by project:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "group_by": ["p"],
  "aggregations": [
    { "count": "v", "as": "vuln_count" }
  ],
  "aggregation_sort": "-vuln_count",
  "limit": 20
}
```

Count vulnerabilities by severity:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    }
  ],
  "group_by": ["v.severity"],
  "aggregations": [
    { "count": "v", "as": "vuln_count" }
  ],
  "aggregation_sort": "-vuln_count",
  "limit": 10
}
```

### Step 3: Trace and shortlist

Use your agent to find the origin of each finding:

```plaintext
For the highest risk findings, trace each one back to the scan and, where
possible, the merge request that introduced the change. Prioritize by severity
and give me a short remediation shortlist.
```

Expected outcome: A shortlist ordered by severity, with the origin of each finding that
GitLab Orbit can trace.

## Tips

- Do not sort by severity with `order_by`. Severity is stored as a string and `-v.severity` puts `critical` last.
- Filter with `{"in": ["critical", "high"]}` or group by `severity` instead, as the preceding queries do.
- Filter on `severity`, `state`, or `report_type` rather than `title`, which cannot be used with `contains`.
- Name a `report_type`, such as `sast` or `dependency_scanning`, to see the findings from one scanner.

## Verify

Ensure that:

- Only findings in the `detected` state are included.
- The affected projects are named by full path.
- Critical findings come ahead of high findings in the shortlist.
