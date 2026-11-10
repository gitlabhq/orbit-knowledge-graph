# Model archives

Each `v<N>.tar.gz` holds the data model packages (`config/model/`) of schema
version N. `orbit-migrations` embeds every archive, and the server embeds the
current one and loads queries from the archive of the active version. `mise schema:bump` writes the
archive of the new version and retains the previous ones.

The dispatcher can publish a bundled archive when the active schema's catalog
entry is missing. It never replaces an existing entry. Missing versions without
a bundled archive still block migration.

## Legacy upgrade support

Versions v93 through v118 predate the checked-in packages. Each was converted from
the ontology archive of the same version, and their source fragments were added in
a second conversion that left every other package file and the generated DDL
unchanged.

Schemas 93 through 95 also predate archive publication. Their ontology archives
held the unmodified ontology YAML and SQL templates from the exact release below,
so a server on any of them can upgrade directly:

| Archive        | Release    | Commit                                     |
| -------------- | ---------- | ------------------------------------------ |
| `v93.tar.gz`   | `v0.115.0` | `4cd41155c4bd87c59f2f1665ce2443e630e1f784` |
| `v94.tar.gz`   | `v0.118.0` | `be1137efc42bd09ab2120e8e024719e041adb37e` |
| `v95.tar.gz`   | `v0.118.1` | `53544c54efa727be390cd28faba0460597eaa792` |

To support another legacy starting schema, recover its sources from the exact
release, convert and commit its archive, document its provenance here, and test
the direct upgrade. Never relabel the current packages as a historical version.
