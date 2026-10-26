---
title: GQL graph queries
description: Query the graph with GQL, a read-only query language that is similar to OpenCypher.
theme: agents
status: in-progress
start: FY27 Q3
end: FY27 Q3
tracking: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1311
---

Orbit queries use GQL.
Many agents and engineers already know this query style, so queries are easier to write and to read.
GQL becomes the Orbit query language on every deployment type with GitLab 19.5.

## Example

This query finds merge requests in a project whose latest diff changes a file.

```gql
MATCH (mr:MergeRequest)-[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(f:MergeRequestDiffFile {project_id: 278964, new_path: 'app/models/user.rb'})
RETURN mr.iid, mr.title, mr.state
LIMIT 10
```
