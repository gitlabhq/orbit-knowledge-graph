---
name: related-repositories
description: List related repositories and their key paths
allowed-tools: Read, Bash(git *)
---

# Related Repositories

Registry of related repositories. If a repository is not cloned locally, clone it using `git clone <git_url> <local_path>`.

## Repositories

### GitLab Knowledge Graph (orbit)
- **git_url:** git@gitlab.com:gitlab-org/orbit/knowledge-graph.git
- **project_url:** https://gitlab.com/gitlab-org/orbit/knowledge-graph
- **local_path:** ~/gitlab/orbit/knowledge-graph
- **description:** GitLab Knowledge Graph (orbit) Primary repository

### GitLab Handbook
- **git_url:** git@gitlab.com:gitlab-com/content-sites/handbook.git
- **project_url:** https://gitlab.com/gitlab-com/content-sites/handbook
- **local_path:** ~/gitlab/handbook
- **description:** Repository containing architectural design documents
- **key_paths:**
  - `content/handbook/engineering/architecture/design-documents/gitlab_knowledge_graph` - GitLab Knowledge Graph (orbit) design documents
  - `content/handbook/engineering/architecture/design-documents/data_insights_platform` - Data Insights Platform design document

### gitlab-zoekt-indexer
- **git_url:** git@gitlab.com:gitlab-org/gitlab-zoekt-indexer.git
- **project_url:** https://gitlab.com/gitlab-org/gitlab-zoekt-indexer
- **local_path:** ~/gitlab/gdk/gitlab-zoekt-indexer
- **description:** Zoekt code search indexer for GitLab
- **key_paths:**
  - `internal/gitaly` - Gitaly client implementation and related code

### Siphon
- **git_url:** git@gitlab.com:gitlab-org/analytics-section/siphon.git
- **project_url:** https://gitlab.com/gitlab-org/analytics-section/siphon
- **local_path:** ~/gitlab/siphon
- **description:** CDC (Change Data Capture) stream project
- **key_paths:**
  - `cmd` - Main application entry points
  - `pkg` - Core packages

### Gitaly
- **git_url:** git@gitlab.com:gitlab-org/gitaly.git
- **project_url:** https://gitlab.com/gitlab-org/gitaly
- **local_path:** ~/gitlab/gdk/gitaly
- **description:** Git RPC service for GitLab

### GitLab
- **git_url:** git@gitlab.com:gitlab-org/gitlab.git
- **project_url:** https://gitlab.com/gitlab-org/gitlab
- **local_path:** ~/gitlab/gdk/gitlab
- **description:** Primary GitLab project
- **key_paths:**
  - `doc` - GitLab documentation
