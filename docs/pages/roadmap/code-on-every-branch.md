---
title: "Code search, branches + commits, all languages"
description: Index code in all languages on active branches and recent commits, and search file content in queries.
theme: code
status: planned
start: FY27 Q4
end: FY28 Q1
tracking: https://gitlab.com/groups/gitlab-org/orbit/-/work_items/16
---

Agents work on branches, not only on the default branch.
Orbit will index the branch that an agent works on, and then recent commits.
Active branches come first, in FY27 Q4.
Orbit will also index file content, so one query can combine a text search with the code graph.

## Planned changes

- Index active branches first, and then recent commits.
- Index tags and release branches.
- Index non-code files, such as Markdown, YAML, and JSON.
- Index infrastructure code, such as Terraform.
- Add more languages, based on what our users ask for.
- Update the index incrementally. After a commit, Orbit indexes only the files that changed.
- Search file content with text and regular expressions.
- Limit a search to graph results, for example to the files that a merge request changed.
