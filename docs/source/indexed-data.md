---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Learn what data you can index with GitLab Orbit Local and GitLab Orbit Remote.
title: Index data with GitLab Orbit
---

GitLab Orbit indexes data from multiple sources, including code repositories and SDLC data
rooted in a top-level group. After GitLab Orbit indexes the data, it builds a knowledge graph
that you can query to retrieve relationships, dependencies, and structural context across
your codebase.

Use GitLab Orbit Remote to index data from a top-level group and its subgroups and projects.

Use GitLab Orbit Local to index data from the working tree of any local repository.

## What data GitLab Orbit indexes

GitLab Orbit Local and GitLab Orbit Remote index different types of data. The following sections list what each feature indexes.

GitLab Orbit Remote and GitLab Orbit Local do not index:

- Binary files
- Branches other than the checked out branch (GitLab Orbit Local) or the default branch (GitLab Orbit Remote)

### Source code

| Code structure | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------|--------|
| Files and directories | {{< yes >}} | {{< yes >}} |
| Function, class, method, and module definitions | {{< yes >}} | {{< yes >}} |
| Import declarations | {{< yes >}} | {{< yes >}} |
| Cross-file symbol references | {{< yes >}} | {{< yes >}} |

### Groups, projects, and users

| Groups, projects, and users | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Groups | {{< no >}} | {{< yes >}} |
| Projects | {{< no >}} | {{< yes >}} |
| Users | {{< no >}} | {{< yes >}} |
| Notes and comments | {{< no >}} | {{< yes >}} |

### Code review

| Code review | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Merge requests | {{< no >}} | {{< yes >}} |
| Merge request diffs | {{< no >}} | {{< yes >}} |
| Changed files | {{< no >}} | {{< yes >}} |

### CI/CD pipelines

| CI/CD | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Pipelines | {{< no >}} | {{< yes >}} |
| Stages | {{< no >}} | {{< yes >}} |
| Jobs | {{< no >}} | {{< yes >}} |

### Code planning

| Code planning | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Issues | {{< no >}} | {{< yes >}} |
| Epics | {{< no >}} | {{< yes >}} |
| Tasks | {{< no >}} | {{< yes >}} |
| Incidents | {{< no >}} | {{< yes >}} |
| Milestones | {{< no >}} | {{< yes >}} |
| Labels | {{< no >}} | {{< yes >}} |

### Security

| Security | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Vulnerabilities | {{< no >}} | {{< yes >}} |
| Security findings | {{< no >}} | {{< yes >}} |
| Security scans | {{< no >}} | {{< yes >}} |
| Scanners | {{< no >}} | {{< yes >}} |
| CVE identifiers | {{< no >}} | {{< yes >}} |
| CWE identifiers | {{< no >}} | {{< yes >}} |

### Work scope

| Scope | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Working tree (checked-out code) | {{< yes >}} | {{< no >}} |
| Default branch only | {{< no >}} | {{< yes >}} |
| Multiple repositories | {{< yes >}} | {{< no >}} |
| Branch selection | {{< no >}} | {{< no >}} |

## Supported languages

GitLab Orbit Remote and Local index data for the following languages:

| Language | Definitions | Cross-file references |
|----------|-------------|----------------------|
| Ruby | {{< yes >}} | {{< yes >}} |
| Java | {{< yes >}} | {{< yes >}} |
| Kotlin | {{< yes >}} | {{< yes >}} |
| Python | {{< yes >}} | {{< yes >}} |
| TypeScript | {{< yes >}} | {{< yes >}} |
| JavaScript | {{< yes >}} | {{< yes >}} |
| Rust | {{< yes >}} | {{< yes >}} |
| Go | {{< yes >}} | {{< yes >}} |
| C# | {{< yes >}} | {{< yes >}} |
| C | {{< yes >}} | {{< yes >}} |
| C++ | {{< yes >}} | {{< yes >}} |
| PHP | {{< yes >}} | {{< yes >}} |
| Bash/Shell | {{< yes >}} | {{< no >}} |
