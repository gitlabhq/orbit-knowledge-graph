---
title: Code on every branch, with code search
description: Index code on active branches and recent commits, and search file content inside graph queries.
theme: code
status: in-progress
start: FY27 Q3
end: FY28 Q1
tracking: https://gitlab.com/groups/gitlab-org/orbit/-/work_items/16
---

Agents work on branches, not only on the default branch.
Orbit will index the branch that an agent works on, and then recent commits.
Active branches come first, in FY27 Q4.
Orbit will also index file content, so one query can combine a text search with the code graph.

## Planned changes

- Index active branches first, and then recent commits.
- Update the index incrementally. After a commit, Orbit indexes only the files that changed.
- Search file content with text and regular expressions.
- Limit a search to graph results, for example to the files that a merge request changed.
