---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Full reference for all 28 Orbit node types across 6 domains, including properties and their types.
title: Schema reference
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

Orbit indexes 28 node types across 6 domains. Use these as entity names in your queries.

To fetch the live schema at any time:

```shell
glab orbit remote schema
```

## Core

| Node type | Description | Key properties |
|-----------|-------------|----------------|
| `Group` | GitLab group or subgroup | `id`, `full_path`, `name`, `visibility`, `traversal_path` |
| `Project` | GitLab project and repository | `id`, `full_path`, `name`, `visibility`, `archived`, `star_count` |
| `User` | GitLab user account | `id`, `username`, `email`, `name`, `state`, `is_admin` |
| `Note` | Comment or annotation on any GitLab object | `id`, `note`, `noteable_type`, `noteable_id`, `internal`, `confidential` |

## Source code

| Node type | Description | Key properties |
|-----------|-------------|----------------|
| `Branch` | Git branch | `id`, `project_id`, `name`, `is_default` |
| `Definition` | Function, class, method, or module definition | `id`, `file_path`, `fqn`, `name`, `definition_type`, `start_line`, `end_line`, `content` |
| `Directory` | Directory in a repository | `id`, `project_id`, `path`, `name` |
| `File` | Source code file | `id`, `path`, `name`, `extension`, `language`, `content` |
| `ImportedSymbol` | Import or cross-file symbol reference | `id`, `file_path`, `import_type`, `import_path`, `identifier_name` |

## Code review

| Node type | Description | Key properties |
|-----------|-------------|----------------|
| `MergeRequest` | Merge request | `id`, `iid`, `title`, `description`, `source_branch`, `target_branch`, `state`, `draft`, `squash` |
| `MergeRequestDiff` | Snapshot of changes in an MR | `id`, `merge_request_id`, `commits_count`, `files_count` |
| `MergeRequestDiffFile` | File changed in an MR diff | `id`, `new_path`, `old_path`, `new_file`, `renamed_file`, `deleted_file` |

## CI/CD

| Node type | Description | Key properties |
|-----------|-------------|----------------|
| `Pipeline` | CI/CD pipeline run | `id`, `sha`, `ref`, `status`, `source`, `duration`, `failure_reason` |
| `Stage` | Pipeline stage | `id`, `name`, `status`, `position` |
| `Job` | CI/CD job | `id`, `name`, `status`, `ref`, `allow_failure`, `environment`, `failure_reason` |
| `Deployment` | CI/CD deployment of a commit | `id`, `iid`, `status`, `ref`, `sha`, `environment_id` |
| `Environment` | CI/CD deployment target | `id`, `name`, `state`, `tier`, `external_url` |
| `JobMetadata` | Per-job runtime metadata | `id`, `build_id`, `interruptible`, `timeout`, `exit_code` |
| `Runner` | CI/CD runner | `id`, `runner_type`, `name`, `active`, `locked` |

## Planning

| Node type | Description | Key properties |
|-----------|-------------|----------------|
| `WorkItem` | Issue, epic, task, incident, or other work item | `id`, `iid`, `title`, `description`, `state`, `work_item_type`, `due_date`, `weight` |
| `Milestone` | Milestone | `id`, `title`, `state`, `due_date`, `start_date` |
| `Label` | Label for categorizing work | `id`, `title`, `color` |

## Security

| Node type | Description | Key properties |
|-----------|-------------|----------------|
| `Finding` | Security scan finding from `security_findings` | `id`, `uuid`, `name`, `description`, `severity`, `deduplicated` |
| `SecurityScan` | Security scan execution in a pipeline | `id`, `scan_type`, `status`, `latest` |
| `Vulnerability` | Confirmed or potential security vulnerability | `id`, `title`, `state`, `severity`, `report_type`, `resolved_on_default_branch` |
| `VulnerabilityIdentifier` | CVE, CWE, or other external reference | `id`, `external_type`, `external_id`, `name`, `url` |
| `VulnerabilityOccurrence` | Specific occurrence of a vulnerability (`Vulnerabilities::Finding` in Rails) | `id`, `uuid`, `severity`, `report_type`, `detection_method`, `cve`, `location` |
| `VulnerabilityScanner` | Security scanner | `id`, `external_id`, `name`, `vendor` |

## Notes

- Definition IDs are content-hashed integers scoped per project and branch. Two definitions
of the same symbol in different projects have different IDs even if the function name and
file path are identical.
- All entity IDs are returned as strings in query responses, even when the underlying value
is an integer. This prevents precision loss in JavaScript clients for values above
`Number.MAX_SAFE_INTEGER`.
- `content` fields on `Definition` and `File` nodes contain the full source text of the
definition or file. These fields are available for agent tools that need to hydrate file
content without making separate API calls to GitLab.
- All nodes include a `traversal_path` property used for authorization
filtering. Query results are automatically scoped to entities the requesting user can access.
