# Ontology archives

The server embeds every `v<N>.tar.gz` in this directory and validates that each
archive loads at build time. The current archive must also match the current
ontology sources. Schema bumps retain previous archives.

The dispatcher can publish a bundled archive when the active schema's catalog
entry is missing. It never replaces an existing entry. Missing versions without
a bundled archive still block migration.

## Legacy upgrade support

`v93.tar.gz` contains the unmodified ontology YAML and SQL templates from release
`v0.115.0`, commit `4cd41155c4bd87c59f2f1665ce2443e630e1f784`.
That release predates archive publication. It is the supported legacy starting
schema; bundling it allows a direct upgrade without manually seeding the catalog
or deploying an intermediate release.

The archive was produced with `OntologyArchive::from_sources(93, sources)`, where
`sources` contains every `.yaml` and `.sql.j2` file under that commit's
`config/ontology/`, keyed by relative path. The archive contains 104 source files
and the format-1 manifest. No current ontology files were substituted.

To support another legacy starting schema, recover its sources from the exact
release, generate and commit its archive, document its provenance here, and test
the direct upgrade. Never relabel the current ontology as a historical version.
