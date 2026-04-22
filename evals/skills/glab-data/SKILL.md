# glab Data Access Skill

## Overview

Use the `glab` CLI and direct GitLab API calls to query data from a GitLab instance.

## glab CLI Commands

### Merge Requests

```bash
# List MRs for a project
glab mr list --repo owner/project

# Get MR details
glab mr view 123 --repo owner/project

# List with filters
glab mr list --repo owner/project --state opened --author alice
```

### Issues

```bash
# List issues
glab issue list --repo owner/project

# Get issue details
glab issue view 456 --repo owner/project
```

### Projects / Repos

```bash
# Search projects
glab api "/projects?search=knowledge-graph"

# Get project details
glab api "/projects/278964"
```

## REST API (via glab api or curl)

```bash
# List project MRs with filters
glab api "/projects/278964/merge_requests?state=opened&per_page=100"

# Get user info
glab api "/users?username=alice"

# List project members
glab api "/projects/278964/members/all"

# Pipelines
glab api "/projects/278964/pipelines?per_page=20"
```

## GraphQL (for complex queries)

```bash
# Find a user's MRs across projects
glab api graphql -f query='
{
  user(username: "alice") {
    authoredMergeRequests(first: 50, state: opened) {
      nodes {
        iid
        title
        project { fullPath }
        createdAt
      }
    }
  }
}'

# Get project with MR stats
glab api graphql -f query='
{
  project(fullPath: "gitlab-org/gitlab") {
    name
    mergeRequests(state: opened) {
      count
    }
    issues(state: opened) {
      count
    }
  }
}'
```

## Tips

- GraphQL is best for multi-hop queries (e.g., user -> MRs -> projects)
- REST pagination: use `per_page=100` and follow `x-next-page` header
- For large result sets, paginate and collect results
- Use `jq` to parse JSON output: `glab api "/projects/278964" | jq '.name'`
- Set `GITLAB_HOST` for non-default instances: `export GITLAB_HOST=staging.gitlab.com`
