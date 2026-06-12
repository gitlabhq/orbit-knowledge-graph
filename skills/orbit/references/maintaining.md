# Maintaining the Orbit skill

For agents and contributors editing this skill itself, not for answering Orbit
queries. See [`SKILL.md`](../SKILL.md) for usage.

## Contributing

If Orbit guidance, recipes, or helper behavior is inaccurate, update this skill
in `gitlab-org/orbit/knowledge-graph` rather than working around it silently.
Keep `SKILL.md`, `references/`, and `scripts/` in sync, and use `opencode run`
for meaningful behavior changes.

## Syncing the query-language reference

`references/query_language.md` is synced from
`docs/source/remote/queries/query-language.md`. Edit the upstream file, then run:

```bash
mise run skill:sync:orbit
```

The lefthook `orbit-skill-docs-sync` job fails the commit if the two files
drift. Do **not** edit `references/query_language.md` directly.

## Version bumps

Bump the `version` field in `SKILL.md` frontmatter on every change under
`skills/orbit/`. The lefthook `skill-version-bump` job enforces this.
