---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Copy-paste Orbit queries for common use cases including blast radius analysis, onboarding, dependency mapping, pipeline health, and vulnerability tracing.
title: Cookbook
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

Ready-to-use queries for the most common Orbit use cases. All examples use the
REST API format. To run them via MCP, pass the JSON body to `query_graph`.

## Blast radius analysis

Answer: "What breaks if I change this?"

### Find all files that import a specific module

Replace `payments-service` with the module or library you want to trace.

```json
{
  "query_type": "traversal",
  "node": {
    "id": "sym",
    "entity": "ImportedSymbol",
    "columns": ["file_path", "import_path", "identifier_name"],
    "filters": {
      "import_path": {"op": "contains", "value": "payments-service"}
    }
  },
  "limit": 100
}
```

### Find all callers of a function across the codebase

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "def",
      "entity": "Definition",
      "filters": {"name": "process_payment", "definition_type": "function"}
    },
    {
      "id": "caller",
      "entity": "Definition",
      "columns": ["name", "file_path", "fqn"]
    }
  ],
  "relationships": [
    {"type": "CALLS", "from": "caller", "to": "def"}
  ],
  "limit": 50
}
```

### Find projects that depend on a shared library

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"op": "contains", "value": "shared-auth-lib"}}
    },
    {"id": "b", "entity": "Branch", "columns": ["name", "is_default"]},
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "ON_BRANCH", "from": "f", "to": "b"},
    {"type": "CONTAINS", "from": "p", "to": "b"}
  ],
  "limit": 100
}
```

## Onboarding and codebase exploration

Answer: "Help me understand this codebase."

### Map the top-level structure of a project

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"}
    },
    {
      "id": "b",
      "entity": "Branch",
      "filters": {"is_default": true}
    },
    {
      "id": "d",
      "entity": "Directory",
      "columns": ["path", "name"]
    }
  ],
  "relationships": [
    {"type": "CONTAINS", "from": "p", "to": "b"},
    {"type": "CONTAINS", "from": "b", "to": "d"}
  ],
  "limit": 50
}
```

### Find the most active contributors to a project

```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "u", "entity": "User", "columns": ["username", "name"]},
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"}
    },
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"}
    }
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"},
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "aggregations": [
    {"function": "count", "target": "mr", "group_by": "u", "alias": "merged_mrs"}
  ],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 10
}
```

### Find who owns a file based on recent review history

```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User", "columns": ["username", "name"]},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
    {"id": "diff", "entity": "MergeRequestDiff", "columns": ["id", "state"]},
    {
      "id": "diff_file",
      "entity": "MergeRequestDiffFile",
      "filters": {
        "new_path": {"op": "contains", "value": "app/services/auth"}
      }
    }
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"},
    {"type": "HAS_DIFF", "from": "mr", "to": "diff"},
    {"type": "HAS_FILE", "from": "diff", "to": "diff_file"}
  ],
  "limit": 25
}
```

## Dependency mapping

Answer: "How are our services connected?"

### Map imported definitions

```json
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "sym",
      "entity": "ImportedSymbol",
      "columns": ["import_path"],
      "filters": {
        "import_path": {"op": "contains", "value": "payments"}
      }
    },
    {"id": "def", "entity": "Definition", "columns": ["name", "fqn", "file_path"]}
  ],
  "relationships": [
    {"type": "IMPORTS", "from": "sym", "to": "def"}
  ],
  "aggregations": [
    {"function": "count", "target": "sym", "group_by": "def", "alias": "import_count"}
  ],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 20
}
```

### Find the shortest connection path between two projects

```json
{
  "query_type": "path_finding",
  "nodes": [
    {
      "id": "start",
      "entity": "Project",
      "filters": {"full_path": "my-org/service-a"}
    },
    {
      "id": "end",
      "entity": "Project",
      "filters": {"full_path": "my-org/service-b"}
    }
  ],
  "path": {
    "type": "shortest",
    "from": "start",
    "to": "end",
    "max_depth": 3,
    "rel_types": ["CONTAINS", "IN_PROJECT", "IMPORTS", "CALLS"]
  },
  "options": {"dynamic_columns": "*"}
}
```

## Pipeline health

Answer: "Where are our CI/CD problems?"

### Find projects with the most failed pipelines

```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "pl", "to": "p"}
  ],
  "aggregations": [
    {"function": "count", "target": "pl", "group_by": "p", "alias": "failed_count"}
  ],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 10
}
```

### Find failed jobs and their failure reasons

```json
{
  "query_type": "traversal",
  "node": {
    "id": "j",
    "entity": "Job",
    "columns": ["name", "status", "failure_reason"],
    "filters": {"status": "failed"}
  },
  "limit": 10
}
```

## Vulnerability tracing

Answer: "Where are our security risks, and how did they get there?"

### Find all critical and high vulnerabilities in a group

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "columns": ["title", "severity", "state", "report_type"],
      "filters": {
        "severity": {"op": "in", "value": ["critical", "high"]},
        "state": "detected"
      }
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "order_by": {"node": "v", "property": "severity", "direction": "DESC"},
  "limit": 50
}
```

### Find pipelines that detected a known finding

Use a finding ID from a vulnerability query, then trace it to the pipelines where
Orbit indexed that finding.

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "finding",
      "entity": "Finding",
      "node_ids": [12345],
      "columns": ["uuid", "name", "severity"]
    },
    {"id": "pl", "entity": "Pipeline", "columns": ["id", "ref", "sha", "status"]}
  ],
  "relationships": [
    {"type": "DETECTED_IN", "from": "finding", "to": "pl"}
  ],
  "limit": 10
}
```

### Count vulnerabilities by project, sorted by severity

```json
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
  "aggregations": [
    {"function": "count", "target": "v", "group_by": "p", "alias": "vuln_count"}
  ],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 20
}
```
