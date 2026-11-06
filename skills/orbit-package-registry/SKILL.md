---
name: orbit-package-registry
description: Use Orbit graph queries to answer questions about GitLab Package Registry packages and Container Registry repositories across projects and groups. It covers package versions and their files, declared dependencies, known vulnerabilities, and the pipelines that built them. Load it with the `orbit` skill. Skip it for one project's package list or for publishing, which `glab` and the package manager handle.
version: 0.1.0
license: MIT
compatibility: Requires the GitLab Orbit CLI (directly or through glab) and network access to the GitLab instance for commands that call the GitLab Orbit API.
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, package-registry, container-registry, dependencies, vulnerabilities, sbom
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit-package-registry
---

# Package Registry and Container Registry in GitLab Orbit

Use these rules and recipes with the `orbit` skill, which covers query syntax,
schema discovery, and the iteration budget. Run each recipe with
`glab orbit query "<query>"`, because the recipes contain single-quoted strings.

## Rules

- Package and container questions use `Package`, `PackageFile`, `Dependency`, and `ContainerRepository`. This is the free-tier Package Registry and Container Registry. Artifact Registry data is not in Orbit, so do not report these results as Artifact Registry packages.
- `Package` holds one row per package version.
- To count packages the way GitLab lists them, filter `p.status IN ['default', 'error', 'deprecated']`. Without it, counts include `hidden` packages, packages still `processing`, and packages `pending_destruction`.
- To scope packages to a group, join to the project and filter its path: `MATCH (p:Package)-[:IN_PROJECT]->(pr:Project) WHERE pr.full_path STARTS WITH 'gitlab-org/'`. Walking `Group` and `CONTAINS` to a fixed depth misses deeper subgroups.
- `PackageFile` rows are the stored files of a package version, such as one `.tgz` or `.jar`. They are not the files inside the archive. Read them with `-[:HAS_PACKAGE_FILE]->(f:PackageFile)`.
- `BUILT_BY` links a package or file to the CI/CD pipeline that built it. `DECLARES_DEPENDENCY` links a package to the dependencies it declares, which is not proof the dependency is used at run time.
- A missing `HAS_VULNERABILITY` link is not proof a package is safe. The link comes from SBOM data, matched by package name and version.
- Each `Vulnerability` is one record in one project: the project whose SBOM matched the package, not the package's own project. One advisory found in 29 projects is 29 records. Count advisories with `count(DISTINCT i.external_id)` on the primary identifier, as in [the recipe](#packages-with-known-vulnerabilities).
- Reading a vulnerability needs the Security Manager role or higher on the project that holds it. Without it, the query returns no rows, which looks the same as no known vulnerabilities.
- Vulnerabilities in a package's project are not that package's vulnerabilities. Only a `HAS_VULNERABILITY` link ties a vulnerability to a package version.
- `ContainerRepository` has no tags. Read the tag count from the Container Registry API with the repository `id`, and no `/api/v4/` prefix: `glab api "registry/repositories/<id>?tags_count=true"`.
- A response with `has_more: true` or `truncated: true` is not the full result. Count with an aggregate query, or page with `SKIP`, before you say how many there are or that you found them all.

## Recipes

### Packages in a project, by type

```gql orbit-query
MATCH (p:Package)-[:IN_PROJECT]->(pr:Project {full_path: 'gitlab-org/editor-extensions/gitlab-lsp'})
WHERE p.status IN ['default', 'error', 'deprecated']
RETURN p.package_type, count(p) AS packages
ORDER BY packages DESC
LIMIT 10
```

### Projects in a group that publish the most packages

To count one format, add a type filter, such as `(p:Package {package_type: 'npm'})`.

```gql orbit-query
MATCH (p:Package)-[:IN_PROJECT]->(pr:Project)
WHERE pr.full_path STARTS WITH 'gitlab-org/' AND p.status IN ['default', 'error', 'deprecated']
RETURN pr.full_path, count(p) AS packages
ORDER BY packages DESC
LIMIT 10
```

### Packages that declare a dependency

Count them with one aggregate query rather than a sample.

```gql orbit-query
MATCH (p:Package)-[:DECLARES_DEPENDENCY]->(d:Dependency {name: 'lodash'}), (p)-[:IN_PROJECT]->(pr:Project)
WHERE pr.full_path STARTS WITH 'gitlab-org/' AND p.status IN ['default', 'error', 'deprecated']
RETURN pr.full_path, count(DISTINCT p) AS dependents
ORDER BY dependents DESC
LIMIT 10
```

### Pipeline that built a package

```gql orbit-query
MATCH (p:Package {name: 'com/gitlab/javafuzz/javafuzz', version: '1.26'})-[:BUILT_BY]->(pl:Pipeline)
RETURN pl.id, pl.status, pl.ref
LIMIT 10
```

For one file of the package, match `PackageFile` on its `package_id`:

```gql orbit-query
MATCH (f:PackageFile {package_id: 4092779})-[:BUILT_BY]->(pl:Pipeline)
RETURN f.file_name, pl.id
LIMIT 10
```

### Files of a package version

```gql orbit-query
MATCH (p:Package {name: '@gitlab-org/duo-workflow-service', version: '0.1.2'})-[:HAS_PACKAGE_FILE]->(f:PackageFile)
RETURN f.file_name, f.size
LIMIT 50
```

### Packages with known vulnerabilities

`advisories` counts distinct primary identifiers. `affected_projects` counts
the projects whose SBOM matched the package. Say with every result that a
missing link is not proof that a package is safe.

```gql orbit-query
MATCH (p:Package)-[:HAS_VULNERABILITY]->(v:Vulnerability)-[:HAS_FINDING]->(o:VulnerabilityOccurrence)-[:HAS_IDENTIFIER]->(i:VulnerabilityIdentifier), (p)-[:IN_PROJECT]->(pr:Project)
WHERE pr.full_path STARTS WITH 'gitlab-org/'
RETURN p.name, p.version, count(DISTINCT i.external_id) AS advisories, count(DISTINCT v.project_id) AS affected_projects
ORDER BY advisories DESC
LIMIT 20
```

Advisories by severity for one package version:

```gql orbit-query
MATCH (p:Package {name: 'commons-io/commons-io', version: '2.3'})-[:HAS_VULNERABILITY]->(v:Vulnerability)-[:HAS_FINDING]->(o:VulnerabilityOccurrence)-[:HAS_IDENTIFIER]->(i:VulnerabilityIdentifier)
RETURN v.severity, count(DISTINCT i.external_id) AS advisories
ORDER BY advisories DESC
LIMIT 10
```

### Container repositories in a project

```gql orbit-query
MATCH (c:ContainerRepository)-[:IN_PROJECT]->(pr:Project {full_path: 'gitlab-org/gitlab'})
RETURN count(c) AS repositories
```

Read tags from the Container Registry API with the repository `id`. Pass the
path without an `/api/v4/` prefix, because `glab api` adds it:

```shell
glab api "registry/repositories/<repository-id>?tags_count=true"
glab api "projects/<project-id>/registry/repositories/<repository-id>/tags"
```
