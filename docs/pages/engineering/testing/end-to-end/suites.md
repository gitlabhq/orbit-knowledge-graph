---
title: Suites
description: What the end-to-end suites check, and the rules for a new suite.
order: 3
authors:
  - bohdanpk
  - michaelangeloio
  - jgdoyon1
---

## What the suites cover

| Area | Question | What the suites do |
| ---- | -------- | ------------------ |
| Indexing | Does GitLab data reach the graph? | Create projects, work items, notes, epics, vulnerabilities, memberships, and project creators, and check each node and edge. |
| Code graph | Does code reach the graph? | Push small Ruby and Java repositories before Orbit is on, and check files, definitions, and imports. The backfill suite checks that turning on Orbit alone indexes the existing projects. |
| Lifecycle | Does the graph follow changes? | Delete a note and check that it leaves the graph. Turn Orbit off for a group and check that the data stays, then turn it on and check that new data arrives. |
| Authorization | Does each user see only their data? | Run the same queries as users with different roles. A private project stays hidden, a count includes only what the role can see, and `NOT` filters do not show hidden data. |
| Query API | Does each query shape work through GitLab? | Run neighbors, path finding, paging, relationship types, and each response format, check the schema and status endpoints, and query across top-level groups. |

The authorization suites run the real GitLab permission checks, so a mismatch between Orbit and the [security model](../../architecture/security/README.md) fails a test.
The API surface checks treat an endpoint that an older GitLab lacks as absent, but a server error still fails.
The MCP server is out of scope, because it needs an OAuth token that the stack cannot create yet.

## Rules for a new suite

A new suite is one Robot Framework file, and it joins the parallel pool with no registration.

- **Attach to the shared fixture.** The shared setup gives the suite the shared group and its own administrator user.
- **Seed your own data.** Create what the suite checks, with a random suffix in each name. Never read data from another suite.
- **Change the instance only in the smoke suite.** A feature flag or a license affects every suite.
- **Wait with a budget.** Poll the query API. Never use a fixed sleep.
- **Assert through the query API.** It is the system under test. A check in the database or the logs does not prove what a user sees.
