---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
title: Graph schema
---

{{< details >}}

- Tier: Ultimate
- Offering: GitLab.com

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.
- Enabled on GitLab.com in GitLab 18.XX.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.

The knowledge graph schema defines the objects Orbit indexes and
the relationships it tracks.

## Node types

A node represents an indexed object. Each node has a type, which defines its properties.
For example, the `User` node type has properties related to GitLab user accounts,
like a username, display name, and user ID.

### `core` domain

The `core` domain contains the foundational GitLab objects that other domains
reference.

| Node | Description |
|------|-------------|
| `User` | A GitLab user account. |
| `Group` | A group or subgroup. |
| `Project` | A project. |
| `Note` | A comment on an issue, merge request, or other object. |

### `plan` domain

The `plan` domain covers project planning and tracking.

| Node | Description |
|------|-------------|
| `WorkItem` | An issue, task, objective, or other work item. |
| `Milestone` | A GitLab milestone. |
| `Label` | A label applied to an issue, a merge request, or other object. |

### `code_review` domain

The `code_review` domain covers merge requests.

| Node | Description |
|------|-------------|
| `MergeRequest` | A merge request. |
| `MergeRequestDiff` | A diff associated with a merge request. |
| `MergeRequestDiffFile` | A file changed in a merge request diff. |

### `ci` domain

The `ci` domain covers CI/CD pipeline execution.

| Node | Description |
|------|-------------|
| `Pipeline` | A CI/CD pipeline run. |
| `Stage` | A stage in a pipeline. |
| `Job` | A job in a pipeline stage. |

### `security` domain

The `security` domain covers security scanning results.

| Node | Description |
|------|-------------|
| `Vulnerability` | A vulnerability identified in a project. |
| `VulnerabilityOccurrence` | A specific occurrence of a vulnerability. |
| `Finding` | A security scan finding that might be linked to a vulnerability. |
| `VulnerabilityScanner` | The scanner that produced a finding. |
| `VulnerabilityIdentifier` | An external identifier for a vulnerability, such as a CVE. |
| `SecurityScan` | A security scan run against a project. |

### `source_code` domain

The `source_code` domain covers the structure of your repository.

| Node | Description |
|------|-------------|
| `Branch` | A Git branch. |
| `Directory` | A directory in the repository. |
| `File` | A file in the repository. |
| `Definition` | A code definition such as a class, function, method, or module. |
| `ImportedSymbol` | An imported symbol or module reference in a source file. |

## Relationships

Relationshps define the edges that connect nodes.
Relationships have variants, which are combinations of source and target node type.
For example, the `Authored` relationship connects `User` nodes to `MergeRequest`, `Note`, `Vulnerability`, and `WorkItem` nodes.

### Available relationships

The following table lists the relationships available by default:

| Relationship | Variants (Source → Target) |
|---|---|
| `AUTHORED` | `User` → `MergeRequest` <br> `User` → `Note` <br> `User` → `Vulnerability` <br> `User` → `WorkItem` |
| `ASSIGNED` | `User` → `MergeRequest` <br> `User` → `WorkItem` |
| `REVIEWER` | `MergeRequest` → `User` |
| `MERGED_BY` | `MergeRequest` → `User` |
| `CREATOR` | `User` → `Project` |
| `OWNER` | `User` → `Group` |
| `MEMBER_OF` | `User` → `Group` <br> `User` → `Project` |
| `CONTAINS` | `Directory` → `Directory` <br> `Directory` → `File` <br> `Group` → `Group` <br> `Group` → `Project` <br> `User` → `Project` <br> `WorkItem` → `WorkItem` |
| `IN_PROJECT` | `Finding` → `Project` <br> `Job` → `Project` <br> `Label` → `Project` <br> `MergeRequest` → `Project` <br> `Milestone` → `Project` <br> `Note` → `Project` <br> `Pipeline` → `Project` <br> `SecurityScan` → `Project` <br> `Stage` → `Project` <br> `Vulnerability` → `Project` <br> `VulnerabilityIdentifier` → `Project` <br> `WorkItem` → `Project` |
| `IN_GROUP` | `Label` → `Group` <br> `Milestone` → `Group` <br> `WorkItem` → `Group` |
| `IN_MILESTONE` | `MergeRequest` → `Milestone` <br> `WorkItem` → `Milestone` |
| `HAS_LABEL` | `MergeRequest` → `Label` <br> `WorkItem` → `Label` |
| `HAS_NOTE` | `MergeRequest` → `Note` <br> `Vulnerability` → `Note` <br> `WorkItem` → `Note` |
| `HAS_DIFF` | `MergeRequest` → `MergeRequestDiff` |
| `HAS_FILE` | `MergeRequestDiff` → `MergeRequestDiffFile` |
| `HAS_STAGE` | `Pipeline` → `Stage` |
| `HAS_JOB` | `Stage` → `Job` |
| `HAS_FINDING` | `SecurityScan` → `Finding` <br> `Vulnerability` → `Finding` |
| `HAS_IDENTIFIER` | `Finding` → `VulnerabilityIdentifier` <br> `VulnerabilityOccurrence` → `VulnerabilityIdentifier` |
| `TRIGGERED` | `MergeRequest` → `Pipeline` <br> `User` → `Job` <br> `User` → `Pipeline` |
| `IN_PIPELINE` | `SecurityScan` → `Pipeline` |
| `RAN_BY` | `SecurityScan` → `Job` |
| `DETECTED_BY` | `Finding` → `VulnerabilityScanner` <br> `VulnerabilityOccurrence` → `VulnerabilityScanner` |
| `DETECTED_IN` | `Finding` → `Pipeline` |
| `OCCURRENCE_OF` | `VulnerabilityOccurrence` → `Vulnerability` |
| `SCANS` | `VulnerabilityScanner` → `Project` |
| `CONFIRMED_BY` | `User` → `Vulnerability` |
| `RESOLVED_BY` | `User` → `Vulnerability` |
| `DISMISSED_BY` | `User` → `Vulnerability` |
| `FIXES` | `MergeRequest` → `Vulnerability` |
| `CLOSES` | `MergeRequest` → `WorkItem` |
| `RELATED_TO` | `WorkItem` → `WorkItem` |
| `FROM_BRANCH` | `MergeRequest` → `Branch` |
| `TARGETS` | `MergeRequest` → `Branch` |
| `ON_BRANCH` | `Definition` → `Branch` <br> `Directory` → `Branch` <br> `File` → `Branch` <br> `ImportedSymbol` → `Branch` |
| `DEFINES` | `Definition` → `Definition` <br> `Definition` → `ImportedSymbol` <br> `File` → `Definition` |
| `IMPORTS` | `File` → `ImportedSymbol` <br> `ImportedSymbol` → `Definition` <br> `ImportedSymbol` → `File` <br> `ImportedSymbol` → `ImportedSymbol` |
| `CALLS` | `Definition` → `Definition` <br> `Definition` → `ImportedSymbol` <br> `File` → `Definition` <br> `File` → `ImportedSymbol` |