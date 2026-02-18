---
name: gitlab-epic-management
description: Manage GitLab epics, issues, and merge requests via the glab CLI and GraphQL API. Use when the user wants to create issues, update epic descriptions, link issues to epics, fetch MRs, label issues, or perform any epic/issue management tasks on GitLab.
---

# GitLab epic and issue management

## Prerequisites

- `glab` CLI authenticated and configured
- Target project: `gitlab-org/orbit/knowledge-graph` (override with `-R`)
- All `glab api` and `glab api graphql` calls need `required_permissions: ["all"]` (TLS certs fail in sandbox)

## CLI quirks

### glab mr list

- No `--state` flag. Use `--merged`, `--closed`, or default (opened).
- Author filter: `--author=<username>` (not display name).
- Pagination: `-P <count>` for page size, `-p <page>` for page number.

### glab issue create

- Use `--yes` to skip interactive prompts.
- `--label` accepts comma-separated labels: `--label "knowledge graph,type::feature"`.
- Output contains the issue URL; parse IID with: `sed -n 's|.*issues/\([0-9]*\).*|\1|p'`
- macOS `grep` does not support `-P`. Use `sed` or `grep -oE` instead.

### glab api

- URL-encode project paths: `gitlab-org%2Forbit%2Fknowledge-graph`
- REST: `glab api -X POST "projects/<encoded>/issues" -f "title=..." -f "description=..."`
- GraphQL: `glab api graphql -f query='...'`

## Epics are work items

GitLab epics are managed through the Work Items GraphQL API, not the deprecated Epics REST API.

### Fetch epic content

```bash
glab api graphql -f query='
query {
  namespace(fullPath: "gitlab-org/rust") {
    workItem(iid: "33") {
      id
      title
      description
      state
      widgets {
        ... on WorkItemWidgetHierarchy {
          __typename
          children { nodes { id iid title state } }
        }
        ... on WorkItemWidgetLabels {
          labels { nodes { title } }
          __typename
        }
      }
    }
  }
}'
```

The `id` field returns the global Work Item ID (e.g., `gid://gitlab/WorkItem/178024097`). Save this for linking operations.

### Update epic description

```bash
glab api graphql -f query='
mutation {
  workItemUpdate(input: {
    id: "gid://gitlab/WorkItem/<EPIC_ID>"
    descriptionWidget: {
      description: "<new markdown>"
    }
  }) {
    workItem { id title }
    errors
  }
}'
```

### Link issue as child of epic

Issues are work items. The numeric ID is shared: `gid://gitlab/Issue/123` maps to `gid://gitlab/WorkItem/123`.

1. Get the issue's global ID:

```bash
glab api graphql -f query='
query {
  project(fullPath: "gitlab-org/orbit/knowledge-graph") {
    issue(iid: "<IID>") { id }
  }
}'
```

2. Convert `gid://gitlab/Issue/<N>` to `gid://gitlab/WorkItem/<N>` (same number).

3. Add as child:

```bash
glab api graphql -f query='
mutation {
  workItemUpdate(input: {
    id: "gid://gitlab/WorkItem/<EPIC_ID>"
    hierarchyWidget: {
      childrenIds: ["gid://gitlab/WorkItem/<ISSUE_ID>"]
    }
  }) {
    workItem { id }
    errors
  }
}'
```

## Labeling

### Standard labels for this project

Issues and MRs in `gitlab-org/orbit/knowledge-graph` use:

- `knowledge graph` - project identifier
- `devops::analytics` - devops stage
- `section::analytics` - section
- `type::feature` / `type::bug` / `type::maintenance` - issue type

### Apply labels

At creation: `--label "knowledge graph,devops::analytics,section::analytics,type::feature"`

After creation:

```bash
glab api -X PUT "projects/gitlab-org%2Forbit%2Fknowledge-graph/issues/<IID>" \
  -f 'labels=knowledge graph,devops::analytics,section::analytics,type::feature'
```

### Search for labels

```bash
glab api "projects/gitlab-org%2Forbit%2Fknowledge-graph/labels?per_page=100&search=<term>" \
  | jq '.[].name'
```

## Workflows

### Workflow: fetch MRs for a user

```bash
# Merged MRs (paginated)
glab mr list -R gitlab-org/orbit/knowledge-graph --author=<user> --merged -P 100

# Full MR detail (JSON)
glab mr view <IID> -R gitlab-org/orbit/knowledge-graph --output json

# MR comments
glab api "projects/gitlab-org%2Forbit%2Fknowledge-graph/merge_requests/<IID>/notes" \
  | jq '.[].body'
```

### Workflow: bulk-create issues from markdown

1. **Draft issues** in a markdown file. Each issue is a section:

```markdown
## Issue title

### Problem to solve

Description of the problem.

### Proposed solution

Description of the solution.

Related MRs: !42, !43
```

2. **Create a shell script** that loops over each issue. Key elements:

```bash
EPIC_ID="gid://gitlab/WorkItem/<ID>"
REPO="gitlab-org/orbit/knowledge-graph"
LABELS="knowledge graph,devops::analytics,section::analytics,type::feature"

create_and_link() {
  local title="$1"
  local desc="$2"

  # Jitter to avoid rate limits
  sleep $(( (RANDOM % 5) + 1 ))

  # Create issue
  OUTPUT=$(glab issue create \
    --title "$title" \
    --description "$desc" \
    --label "$LABELS" \
    -R "$REPO" \
    --yes 2>&1)

  IID=$(echo "$OUTPUT" | sed -n 's|.*issues/\([0-9]*\).*|\1|p')

  # Get global ID
  GLOBAL_ID=$(glab api graphql -f query="
    query {
      project(fullPath: \"$REPO\") {
        issue(iid: \"$IID\") { id }
      }
    }" 2>&1 | jq -r '.data.project.issue.id')

  WORK_ITEM_ID=$(echo "$GLOBAL_ID" | sed 's|gid://gitlab/Issue/|gid://gitlab/WorkItem/|')

  # Link to epic
  glab api graphql -f query="
    mutation {
      workItemUpdate(input: {
        id: \"$EPIC_ID\"
        hierarchyWidget: {
          childrenIds: [\"$WORK_ITEM_ID\"]
        }
      }) {
        errors
      }
    }" 2>&1
}
```

3. **Call `create_and_link`** for each issue with its title and description (use heredocs for multi-line descriptions).

4. **Clean up** the script after execution.

### Workflow: create sub-epics and reorganize hierarchy

Epics live in a **namespace** (group), issues live in a **project**. Sub-epics are work items of type Epic created in the namespace with a `parentId`.

#### 1. Get the Epic work item type ID

```bash
glab api graphql -f query='
query {
  namespace(fullPath: "gitlab-org/rust") {
    workItemTypes { nodes { id name } }
  }
}' | jq '.data.namespace.workItemTypes.nodes[] | select(.name == "Epic")'
# Returns: gid://gitlab/WorkItems::Type/8
```

#### 2. Create a sub-epic under a parent epic

```bash
glab api graphql -f query='
mutation {
  workItemCreate(input: {
    namespacePath: "gitlab-org/rust"
    title: "Sub-Epic Title"
    workItemTypeId: "gid://gitlab/WorkItems::Type/8"
    descriptionWidget: {
      description: "Description in markdown"
    }
    hierarchyWidget: {
      parentId: "gid://gitlab/WorkItem/<PARENT_EPIC_ID>"
    }
  }) {
    workItem { id iid title }
    errors
  }
}'
```

#### 3. Reparent issues from one epic to another (move to sub-epic)

Setting `parentId` on a work item moves it — the old parent relationship is replaced automatically. No need to remove from old parent first.

```bash
glab api graphql -f query='
mutation {
  workItemUpdate(input: {
    id: "gid://gitlab/WorkItem/<ISSUE_GLOBAL_ID>"
    hierarchyWidget: {
      parentId: "gid://gitlab/WorkItem/<NEW_PARENT_EPIC_ID>"
    }
  }) {
    workItem { iid title }
    errors
  }
}'
```

Batch multiple reparent operations in a single GraphQL call using aliases:

```bash
glab api graphql -f query='
mutation {
  a: workItemUpdate(input: {
    id: "gid://gitlab/WorkItem/111"
    hierarchyWidget: { parentId: "gid://gitlab/WorkItem/<SUB_EPIC_ID>" }
  }) { workItem { iid title } errors }
  b: workItemUpdate(input: {
    id: "gid://gitlab/WorkItem/222"
    hierarchyWidget: { parentId: "gid://gitlab/WorkItem/<SUB_EPIC_ID>" }
  }) { workItem { iid title } errors }
}'
```

#### 4. Fetch full hierarchy (epic → sub-epics → issues)

```bash
glab api graphql -f query='
query {
  namespace(fullPath: "gitlab-org/rust") {
    workItem(iid: "<EPIC_IID>") {
      title
      widgets {
        ... on WorkItemWidgetHierarchy {
          __typename
          children {
            nodes {
              iid title state
              widgets {
                ... on WorkItemWidgetHierarchy {
                  __typename
                  children { nodes { iid title state } }
                }
              }
            }
          }
        }
      }
    }
  }
}'
```

#### 5. Batch-fetch work item details by global ID

Use GraphQL aliases to fetch multiple work items in one call:

```bash
glab api graphql -f query='
query {
  a: workItem(id: "gid://gitlab/WorkItem/111") { iid title state description }
  b: workItem(id: "gid://gitlab/WorkItem/222") { iid title state description }
}'
```

Note: fetching work items by IID via `namespace(fullPath: ...) { workItem(iid: ...) }` only works for epics in that namespace. For project issues that are children of a namespace epic, use the global ID approach above.

### Workflow: bulk-create issues and link to an epic

When creating multiple issues, use `glab issue create` individually (not a shell script loop with heredocs — heredoc `$()` substitution breaks on descriptions with backticks, parentheses, or special characters). Create each issue with a separate `glab issue create` call:

```bash
glab issue create \
  --title "[PREP] Issue title" \
  --description "Description without backticks or special chars" \
  --label "knowledge graph,devops::analytics,section::analytics,type::maintenance" \
  -R gitlab-org/orbit/knowledge-graph \
  --yes
```

Then fetch global IDs and link in a second pass.

### Gotchas for shell scripting with global IDs

- **zsh interprets `//` in strings as math expressions.** Do not use `gid://gitlab/...` strings in zsh array loops or `for` variable assignments. Use single GraphQL calls with aliases instead.
- **Heredocs inside `$()` command substitution break on special characters.** Descriptions with backticks, parentheses, or `$` will cause syntax errors. Pass descriptions directly via `--description` flag instead.

### Workflow: close issues as completed

```bash
glab issue close <IID> -R gitlab-org/orbit/knowledge-graph
```

### Workflow: link MRs to issues

Edit the MR description to include `Closes #<IID>` or `Related to #<IID>`:

```bash
# Get current description
DESC=$(glab api "projects/gitlab-org%2Forbit%2Fknowledge-graph/merge_requests/<MR_IID>" \
  | jq -r '.description')

# Append closing reference
glab api -X PUT "projects/gitlab-org%2Forbit%2Fknowledge-graph/merge_requests/<MR_IID>" \
  -f "description=${DESC}

Closes #<ISSUE_IID>"
```
