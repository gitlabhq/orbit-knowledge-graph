---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Understand the node types and relationships that make up the knowledge graph.
title: Knowledge graph schema
---

{{< details >}}

- Tier: Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

The knowledge graph schema defines the objects Orbit indexes and the relationships it tracks.

## Node types

A node represents an indexed object, like a GitLab user account or a source file.
Each node has a type that defines its properties, and a domain that groups it with related types.

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

Relationships are the edges that connect nodes.
Each relationship defines a directed connection between two node types.

Relationships can cross domain boundaries.
For example, an `IN_PROJECT` relationship can connect a `Job` node in the `ci` domain to a `Project` node in the `Core` domain.

The following relationships are available by default:

| Relationship       | Source node                | Target node                  |
|--------------------|----------------------------|------------------------------|
| `AUTHORED`         | `User`                     | `MergeRequest`               |
|                    | `User`                     | `Note`                       |
|                    | `User`                     | `Vulnerability`              |
|                    | `User`                     | `WorkItem`                   |
| `ASSIGNED`         | `User`                     | `MergeRequest`               |
|                    | `User`                     | `WorkItem`                   |
| `REVIEWER`         | `User`                     | `MergeRequest`               |
| `MERGED`           | `User`                     | `MergeRequest`               |
| `APPROVED`         | `User`                     | `MergeRequest`               |
| `CLOSED`           | `User`                     | `WorkItem`                   |
| `CREATOR`          | `User`                     | `Project`                    |
| `OWNER`            | `User`                     | `Group`                      |
| `MEMBER_OF`        | `User`                     | `Group`                      |
|                    | `User`                     | `Project`                    |
| `CONTAINS`         | `Branch`                   | `Directory`                  |
|                    | `Directory`                | `Directory`                  |
|                    | `Directory`                | `File`                       |
|                    | `Group`                    | `Group`                      |
|                    | `Group`                    | `Project`                    |
|                    | `User`                     | `Project`                    |
|                    | `WorkItem`                 | `WorkItem`                   |
| `IN_PROJECT`       | `Branch`                   | `Project`                    |
|                    | `Finding`                  | `Project`                    |
|                    | `Job`                      | `Project`                    |
|                    | `Label`                    | `Project`                    |
|                    | `MergeRequest`             | `Project`                    |
|                    | `Milestone`                | `Project`                    |
|                    | `Note`                     | `Project`                    |
|                    | `Pipeline`                 | `Project`                    |
|                    | `SecurityScan`             | `Project`                    |
|                    | `Stage`                    | `Project`                    |
|                    | `Vulnerability`            | `Project`                    |
|                    | `VulnerabilityIdentifier`  | `Project`                    |
|                    | `WorkItem`                 | `Project`                    |
| `IN_GROUP`         | `Label`                    | `Group`                      |
|                    | `Milestone`                | `Group`                      |
|                    | `WorkItem`                 | `Group`                      |
| `IN_MILESTONE`     | `MergeRequest`             | `Milestone`                  |
|                    | `WorkItem`                 | `Milestone`                  |
| `HAS_LABEL`        | `MergeRequest`             | `Label`                      |
|                    | `WorkItem`                 | `Label`                      |
| `HAS_NOTE`         | `MergeRequest`             | `Note`                       |
|                    | `Vulnerability`            | `Note`                       |
|                    | `WorkItem`                 | `Note`                       |
| `HAS_DIFF`         | `MergeRequest`             | `MergeRequestDiff`           |
| `HAS_FILE`         | `MergeRequestDiff`         | `MergeRequestDiffFile`       |
| `HAS_STAGE`        | `Pipeline`                 | `Stage`                      |
| `HAS_JOB`          | `Stage`                    | `Job`                        |
| `HAS_FINDING`      | `SecurityScan`             | `Finding`                    |
|                    | `Vulnerability`            | `Finding`                    |
| `HAS_IDENTIFIER`   | `Finding`                  | `VulnerabilityIdentifier`    |
|                    | `VulnerabilityOccurrence`  | `VulnerabilityIdentifier`    |
| `TRIGGERED`        | `MergeRequest`             | `Pipeline`                   |
|                    | `User`                     | `Job`                        |
|                    | `User`                     | `Pipeline`                   |
| `IN_PIPELINE`      | `SecurityScan`             | `Pipeline`                   |
| `RAN_BY`           | `SecurityScan`             | `Job`                        |
| `DETECTED_BY`      | `Finding`                  | `VulnerabilityScanner`       |
|                    | `VulnerabilityOccurrence`  | `VulnerabilityScanner`       |
| `DETECTED_IN`      | `Finding`                  | `Pipeline`                   |
| `OCCURRENCE`       | `VulnerabilityOccurrence`  | `Vulnerability`              |
| `SCANS`            | `VulnerabilityScanner`     | `Project`                    |
| `CONFIRMED_BY`     | `User`                     | `Vulnerability`              |
| `RESOLVED_BY`      | `User`                     | `Vulnerability`              |
| `DISMISSED_BY`     | `User`                     | `Vulnerability`              |
| `FIXES`            | `MergeRequest`             | `Vulnerability`              |
| `CLOSES`           | `MergeRequest`             | `WorkItem`                   |
| `RELATED_TO`       | `WorkItem`                 | `WorkItem`                   |
| `FROM_BRANCH`      | `MergeRequest`             | `Branch`                     |
| `TARGETS`          | `MergeRequest`             | `Branch`                     |
| `ON_BRANCH`        | `Definition`               | `Branch`                     |
|                    | `Directory`                | `Branch`                     |
|                    | `File`                     | `Branch`                     |
|                    | `ImportedSymbol`           | `Branch`                     |
| `DEFINES`          | `Definition`               | `Definition`                 |
|                    | `Definition`               | `ImportedSymbol`             |
|                    | `File`                     | `Definition`                 |
| `IMPORTS`          | `File`                     | `ImportedSymbol`             |
|                    | `ImportedSymbol`           | `Definition`                 |
|                    | `ImportedSymbol`           | `File`                       |
|                    | `ImportedSymbol`           | `ImportedSymbol`             |
| `CALLS`            | `Definition`               | `Definition`                 |
|                    | `Definition`               | `ImportedSymbol`             |
|                    | `File`                     | `Definition`                 |
|                    | `File`                     | `ImportedSymbol`             |
