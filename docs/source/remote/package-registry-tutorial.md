---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use GitLab Orbit to answer package registry questions that the REST API can't answer in one call.
title: 'Tutorial: Answer package registry questions with GitLab Orbit'
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

Package registry questions that span a group take many REST calls, or have no REST answer at all.
GitLab Orbit indexes packages, package files, declared dependencies, and the pipelines that built
them as a graph. One query answers each question.

This tutorial shows four questions. Each one is hard or impossible with the REST API.

To answer package registry questions with GitLab Orbit:

1. [Run your first query](#step-1-run-your-first-query) to list your newest packages.
1. [Find packages that depend on a library](#step-2-find-packages-that-depend-on-a-library).
1. [Trace a package to the pipeline that built it](#step-3-trace-a-package-to-the-pipeline-that-built-it).
1. [Find large package files](#step-4-find-large-package-files).
1. [Count packages per project](#step-5-count-packages-per-project).
1. [Ask GitLab Duo instead](#step-6-ask-gitlab-duo-instead).

## Before you begin

- You have a top-level group on GitLab.com with Premium or Ultimate. GitLab Orbit is turned on for
  the group. For more information, see [Get started with GitLab Orbit Remote](getting-started.md).
- You have the Reporter role or higher on the projects you query. Results include only packages you
  can read. GitLab Orbit checks the `read_package` permission on every row. For more information,
  see [security](security.md).
- Your group has packages in the GitLab package registry.
- You have the GitLab CLI (`glab`) 1.117 or later, signed in with `glab auth login`. Or you have a
  personal access token with the `read_api` scope for the REST API.

During the beta, GitLab Orbit queries don't use GitLab Credits.

## Step 1: Run your first query

Start with a simple question: which five npm packages in your group are the newest?

The sample output on this page comes from the public `gitlab-org` group on GitLab.com. The queries
use `my-group/` in its place. This first sample output uses `gitlab-org/editor-extensions/`.

Save this JSON to `newest-packages.json`. Replace `my-group/` with your top-level group path.

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {
        "id": "pkg",
        "entity": "Package",
        "columns": [
          "name",
          "version",
          "package_type",
          "created_at"
        ],
        "filters": {
          "package_type": "npm"
        }
      },
      {
        "id": "p",
        "entity": "Project",
        "columns": [
          "full_path"
        ],
        "filters": {
          "full_path": {
            "starts_with": "my-group/"
          }
        }
      }
    ],
    "relationships": [
      {
        "type": "IN_PROJECT",
        "from": "pkg",
        "to": "p"
      }
    ],
    "order_by": "-pkg.created_at",
    "limit": 5
  },
  "response_format": "llm"
}
```

Run the query with the GitLab CLI:

```shell
glab orbit query --file newest-packages.json
```

Or run it with the REST API:

```shell
curl --request POST \
  --header "PRIVATE-TOKEN: <your_access_token>" \
  --header "Content-Type: application/json" \
  --data @newest-packages.json \
  "https://gitlab.com/api/v4/orbit/query"
```

The output looks like this:

```plaintext
@header
query_type:traversal
nodes:6
edges:5
has_more:true
@nodes
Package(5):
70580412 name=@gitlab-org/gitlab-lsp package_type=npm version=9.25.0 created_at=2026-09-25T12:57:01Z
71018659 name=@gitlab-org/gitlab-lsp package_type=npm version=9.26.0 created_at=2026-09-30T14:38:08Z
71173978 name=@gitlab-org/gitlab-lsp package_type=npm version=9.27.0 created_at=2026-10-01T18:20:10Z
71834859 name=@gitlab-org/gitlab-lsp package_type=npm version=9.28.0 created_at=2026-10-08T11:19:30Z
71850309 name=@gitlab-org/gitlab-lsp package_type=npm version=9.28.1 created_at=2026-10-08T13:20:25Z
Project(1):
46519181 full_path=gitlab-org/editor-extensions/gitlab-lsp
@edges
IN_PROJECT(5):
Package:70580412 --> Project:46519181
...
```

Each part of the query does one job:

| Part | What it does |
|------|--------------|
| `nodes` | The entities to match. Here, `Package` and `Project`. |
| `relationships` | How the nodes connect. Here, `IN_PROJECT`. |
| `filters` | Narrows the match. `package_type` is `npm`. `full_path` with `starts_with` limits results to your group and its subgroups. Keep the trailing slash. |
| `columns` | The fields to return. |
| `order_by` | The sort order. `-pkg.created_at` means newest first. |
| `limit` | The maximum number of rows. |

- **Output format**: `response_format: "llm"` returns compact text, grouped by node type. It lists nodes by ID, not in `order_by` order. Use `"raw"` for JSON in sort order that you can pipe to `jq`.
- **Later steps**: Use the same two commands for every step. Change only the file.

## Step 2: Find packages that depend on a library

Which packages in your group depend on `lodash`? This helps when a library has a security issue.

Today with REST: there's no REST endpoint for a package's declared dependencies. GraphQL
`dependencyLinks` works one package at a time. You list every package and make one call each.

Save this JSON to `dependents.json` and run the same command.

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {
        "id": "pkg",
        "entity": "Package",
        "columns": [
          "name",
          "version",
          "package_type"
        ]
      },
      {
        "id": "d",
        "entity": "Dependency",
        "columns": [
          "name",
          "version_pattern"
        ],
        "filters": {
          "name": "lodash"
        }
      },
      {
        "id": "p",
        "entity": "Project",
        "columns": [
          "full_path"
        ],
        "filters": {
          "full_path": {
            "starts_with": "my-group/"
          }
        }
      }
    ],
    "relationships": [
      {
        "type": "DECLARES_DEPENDENCY",
        "from": "pkg",
        "to": "d"
      },
      {
        "type": "IN_PROJECT",
        "from": "pkg",
        "to": "p"
      }
    ],
    "limit": 100
  },
  "response_format": "llm"
}
```

```shell
glab orbit query --file dependents.json
```

The output, trimmed, looks like this:

```plaintext
@nodes
Dependency(3):
16965799 name=lodash version_pattern="^4.17.21"
72437722 name=lodash version_pattern="^4.17.23"
3166809 name=lodash version_pattern="^4.17.14"
Package(4):
110384 name=@gitlab-org/gitlab-ui package_type=npm version=1.0.0
47331128 name=@gitlab-org/gitlab-lsp package_type=npm version=8.22.0
48717922 name=@gitlab-org/gitlab-lsp package_type=npm version=8.35.0
60763772 name=@gitlab-org/gitlab-lsp package_type=npm version=8.98.0
@edges
DECLARES_DEPENDENCY(4):
Package:110384 --> Dependency:3166809
Package:47331128 --> Dependency:16965799
Package:48717922 --> Dependency:16965799
Package:60763772 --> Dependency:72437722
...
```

- **Version range**: `version_pattern` shows the range each package asked for, for example `^4.17.21`.
- **Formats**: Dependencies are recorded for formats that declare them in their metadata, such as npm and NuGet.

## Step 3: Trace a package to the pipeline that built it

Which pipeline built `@my-group/my-package` version `1.4.2`, and who started it?

Today with REST: call `GET /groups/:id/packages?package_name=...&package_version=...` to find the
package ID. Then call `GET /projects/:id/packages/:package_id/pipelines` for that one package.

Save this JSON to `built-by.json` and run the same command. Replace the package name and version
with your own.

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {
        "id": "pkg",
        "entity": "Package",
        "columns": [
          "name",
          "version"
        ],
        "filters": {
          "name": "@my-group/my-package",
          "version": "1.4.2"
        }
      },
      {
        "id": "pl",
        "entity": "Pipeline",
        "columns": [
          "iid",
          "ref",
          "sha",
          "tag",
          "status",
          "source",
          "created_at"
        ]
      },
      {
        "id": "u",
        "entity": "User",
        "columns": [
          "username"
        ]
      }
    ],
    "relationships": [
      {
        "type": "BUILT_BY",
        "from": "pkg",
        "to": "pl"
      },
      {
        "type": "TRIGGERED",
        "from": "u",
        "to": "pl"
      }
    ],
    "limit": 10
  },
  "response_format": "llm"
}
```

```shell
glab orbit query --file built-by.json
```

The sample output is for `@gitlab-org/gitlab-lsp` 8.98.0. It shows the pipeline IID, ref, commit
SHA, status, source, and who triggered it. The user ID and username are replaced with placeholders.

```plaintext
@nodes
Package(1):
60763772 name=@gitlab-org/gitlab-lsp version=8.98.0
Pipeline(1):
2551838748 iid=20602 status=success ref=main sha=e639dee9b530bb2b3ae04e839e756f3bf1052d7d source=push tag=false created_at=2026-05-25T22:35:22Z
User(1):
<user_id> username=<username>
@edges
BUILT_BY(1):
Package:60763772 --> Pipeline:2551838748
TRIGGERED(1):
User:<user_id> --> Pipeline:2551838748
```

- **CI/CD jobs only**: Only packages published from a CI/CD job have a `BUILT_BY` pipeline.
- **No user**: A pipeline with no triggering user drops out of the results. To keep it, remove the `u` node and the `TRIGGERED` relationship.

## Step 4: Find large package files

Which package files are over 100 MiB (104857600 bytes)? Largest first.

Today with REST: the API has no size filter. You list packages, call `package_files` for each one
with paging, and sort the results yourself.

Save this JSON to `big-files.json` and run the same command.

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {
        "id": "pkg",
        "entity": "Package",
        "columns": [
          "name",
          "version",
          "package_type"
        ]
      },
      {
        "id": "f",
        "entity": "PackageFile",
        "columns": [
          "file_name",
          "size"
        ],
        "filters": {
          "size": {
            "gt": 104857600
          }
        }
      },
      {
        "id": "p",
        "entity": "Project",
        "columns": [
          "full_path"
        ],
        "filters": {
          "full_path": {
            "starts_with": "my-group/"
          }
        }
      }
    ],
    "relationships": [
      {
        "type": "HAS_PACKAGE_FILE",
        "from": "pkg",
        "to": "f"
      },
      {
        "type": "IN_PROJECT",
        "from": "f",
        "to": "p"
      }
    ],
    "order_by": "-f.size",
    "limit": 100
  },
  "response_format": "llm"
}
```

```shell
glab orbit query --file big-files.json
```

The output looks like this:

```plaintext
@nodes
Package(1):
35674987 name=DeepScaleR-1.5B-Preview package_type=ml_model version=0.0.1
PackageFile(2):
176987614 file_name=model-00002-of-00002.safetensors size=2111719976
176991757 file_name=model-00001-of-00002.safetensors size=4996670464
Project(1):
61238492 full_path=gitlab-org/modelops/mlops/mlops
@edges
HAS_PACKAGE_FILE(2):
Package:35674987 --> PackageFile:176987614
Package:35674987 --> PackageFile:176991757
...
```

- **Size unit**: `size` is in bytes. In the sample, the largest file is about 5 GB.
- **Sort order**: The `llm` output lists nodes by ID, so the 5 GB file appears second. Use `"raw"` to get rows in `-f.size` order.

## Step 5: Count packages per project

How many npm packages does each project in your group have?

Today with REST: page through `GET /groups/:id/packages?package_type=npm` and count the results
yourself.

This is an `aggregation` query. It uses `group_by` on the project and `count` on the packages. It
sorts with `aggregation_sort`. Save this JSON to `counts.json` and run the same command.

```json orbit-query
{
  "query": {
    "query_type": "aggregation",
    "nodes": [
      {
        "id": "pkg",
        "entity": "Package",
        "filters": {
          "package_type": "npm"
        }
      },
      {
        "id": "p",
        "entity": "Project",
        "columns": [
          "full_path"
        ],
        "filters": {
          "full_path": {
            "starts_with": "my-group/"
          }
        }
      }
    ],
    "relationships": [
      {
        "type": "IN_PROJECT",
        "from": "pkg",
        "to": "p"
      }
    ],
    "group_by": [
      "p"
    ],
    "aggregations": [
      {
        "count": "pkg",
        "as": "packages"
      }
    ],
    "aggregation_sort": "-packages",
    "limit": 20
  },
  "response_format": "llm"
}
```

```shell
glab orbit query --file counts.json
```

The output shows the top five projects:

```plaintext
@header
query_type:aggregation
group_by:p(node:Project)
aggregations:packages(count:pkg)
@nodes
Project(5):
46519181 full_path=gitlab-org/editor-extensions/gitlab-lsp
39903947 full_path=gitlab-org/modelops/applied-ml/code-suggestions/ai-assist
16683102 full_path=gitlab-org/security-products/security-report-schemas
17421565 full_path=gitlab-org/ci-cd/package-stage/feature-testing/new-packages-list
29589722 full_path=gitlab-org/ci-cd/package-stage/feature-testing/ux-scorecard-metadata
@rows
p=Project:46519181 packages=347
p=Project:39903947 packages=48
p=Project:16683102 packages=26
p=Project:17421565 packages=25
p=Project:29589722 packages=12
```

- **Versions, not names**: The count is package versions, not distinct package names. `347` for `gitlab-lsp` means 347 versions.
- **Other formats**: Change `package_type` to count another format, for example `maven`, `pypi`, or `generic`.

## Step 6: Ask GitLab Duo instead

GitLab Duo Agent Platform has GitLab Orbit built in. You can ask the same questions in plain
language in GitLab Duo Chat. The agent writes and runs the query for you. For more information, see
[Use GitLab Orbit with GitLab Duo](access/duo.md).

For example:

```plaintext
Using GitLab Orbit, which packages in my-group declare a dependency on lodash, and which version range does each one ask for?
```

MCP clients such as Claude Code connect through [MCP](access/mcp.md).

## Next steps

- [Cookbook](cookbook.md): copy-paste prompts.
- [Query language](queries/query-language.md): the full query language.
- [Schema reference](schema.md): all node types.
