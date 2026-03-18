---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
title: Graph schema
---

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
For example, a `HAS_FILE` relationship connects a `MergeRequest` node in the `code_review` domain to a `File` node in the `source_code` domain.

### Available relationships

The following table lists the relationships available by default:

| Relationship       | Source node                | Target node                  |
|--------------------|----------------------------|------------------------------|
| `AUTHORED`         | `User`                     | `MergeRequest`               |
|                    | `User`                     | `Note`                       |
|                    | `User`                     | `Vulnerability`              |
|                    | `User`                     | `WorkItem`                   |
| `ASSIGNED`         | `User`                     | `MergeRequest`               |
|                    | `User`                     | `WorkItem`                   |
| `REVIEWER`         | `MergeRequest`             | `User`                       |
| `MERGED_BY`        | `MergeRequest`             | `User`                       |
| `CREATOR`          | `User`                     | `Project`                    |
| `OWNER`            | `User`                     | `Group`                      |
| `MEMBER_OF`        | `User`                     | `Group`                      |
|                    | `User`                     | `Project`                    |
| `CONTAINS`         | `Directory`                | `Directory`                  |
|                    | `Directory`                | `File`                       |
|                    | `Group`                    | `Group`                      |
|                    | `Group`                    | `Project`                    |
|                    | `User`                     | `Project`                    |
|                    | `WorkItem`                 | `WorkItem`                   |
| `IN_PROJECT`       | `Finding`                  | `Project`                    |
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
| `OCCURRENCE_OF`    | `VulnerabilityOccurrence`  | `Vulnerability`              |
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
