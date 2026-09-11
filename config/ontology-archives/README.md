# Ontology archives

The server embeds every `v<N>.tar.gz` in this directory and validates that each
archive loads at build time. The current archive must also match the current
ontology sources. Schema bumps retain previous archives.

The dispatcher can publish a bundled archive when the active schema's catalog
entry is missing. It never replaces an existing entry. Missing versions without
a bundled archive still block migration.

## Legacy upgrade support

Schemas 93 through 95 predate archive publication. Their archives are bundled so a
server on any of them can upgrade directly, without manually seeding the catalog or
deploying an intermediate release. Each holds the unmodified ontology YAML and SQL
templates from the exact release below, keyed by relative path under
`config/ontology/`, and no current ontology files were substituted:

| Archive        | Release    | Commit                                     | Sources |
| -------------- | ---------- | ------------------------------------------ | ------- |
| `v93.tar.gz`   | `v0.115.0` | `4cd41155c4bd87c59f2f1665ce2443e630e1f784` | 104     |
| `v94.tar.gz`   | `v0.118.0` | `be1137efc42bd09ab2120e8e024719e041adb37e` | 104     |
| `v95.tar.gz`   | `v0.118.1` | `53544c54efa727be390cd28faba0460597eaa792` | 104     |

Each was produced with `OntologyArchive::from_sources(N, sources)` over that commit's
`config/ontology/` files, plus the format-1 manifest.

To support another legacy starting schema, recover its sources from the exact
release, generate and commit its archive, document its provenance here, and test
the direct upgrade. Never relabel the current ontology as a historical version.
