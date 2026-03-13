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

The schema is organized into domains, which are logical groupings of related node types.
In each domain, there are nodes, which represent indexed objects, and relationships,
which define the edges between nodes.

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
