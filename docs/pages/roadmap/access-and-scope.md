---
title: Govern Orbit indexing and queries
description: Control what goes into the graph and who can query it.
theme: trust
status: exploring
start: FY27 Q4
end: FY28 Q1
---

Orbit checks GitLab permissions on every result.
Some teams also need to control what goes into the graph.
They also need to control who can use it.
We will explore how to give you this control.

## Ideas we are exploring

- Turn Orbit on for a subgroup or a project, not only a top-level group.
- Exclude subgroups and projects from the graph, not only top-level groups.
- Add a file to your repository, like a `.gitignore` file, that lists the files and paths to exclude.
- Limit Orbit to the users, roles, or service accounts that you pick.
