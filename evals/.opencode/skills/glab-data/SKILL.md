---
name: glab-data
description: Query GitLab data using the glab CLI. Covers REST API, GraphQL, merge requests, issues, projects, and users.
---

# glab Data Access Skill

Use the `glab` CLI to query data from a GitLab instance.

## REST API

```bash
# Users
glab api "/users?username=alice"

# Projects
glab api "/projects/278964"

# Merge requests (with filters)
glab api "/projects/278964/merge_requests?state=opened&per_page=100"

# Project members
glab api "/projects/278964/members/all"
```

## GraphQL

```bash
# User's MRs across projects
glab api graphql -f query='{user(username:"alice"){authoredMergeRequests(first:50,state:opened){nodes{iid title project{fullPath} createdAt}}}}'

# Project stats
glab api graphql -f query='{project(fullPath:"gitlab-org/gitlab"){name mergeRequests(state:opened){count} issues(state:opened){count}}}'
```

## Tips

- Use `jq` to parse output: `glab api "/projects/278964" | jq '.name'`
- Paginate with `per_page=100` and follow `x-next-page` header
- GraphQL is best for multi-hop queries (user -> MRs -> projects)
