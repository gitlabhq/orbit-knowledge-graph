---
title: Orbit on GitLab.com Cells
description: Orbit and the Orbit API work for groups and projects on every GitLab.com cell.
theme: platform
status: planned
start: FY28 Q1
end: FY28 Q2
tracking: https://gitlab.com/gitlab-com/gl-infra/tenant-scale/cells-infrastructure/team/-/work_items/864
---

GitLab.com is moving to Cells. Each cell holds a set of organizations and their data.
Orbit will run with Cells, so that your group works with Orbit on any cell.

## Planned changes

- The Orbit API, GraphQL, MCP, and the Orbit CLI work the same on every cell.
- GitLab sends each query to the cell that holds your data.
- Queries that span more than one organization come later.
