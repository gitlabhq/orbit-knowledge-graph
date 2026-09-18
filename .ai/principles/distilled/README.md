# Documentation principles

The `documentation*.md` files in this directory are the distilled GitLab documentation
principles, synced from
[the same path](https://gitlab.com/gitlab-org/gitlab/-/tree/master/.ai/principles/distilled)
in `gitlab-org/gitlab`.
They are the standard for GitLab product documentation, and they govern the pages under
`docs/source/`.
Read them before writing or editing a page there.
The repository's own `mise lint:docs` task checks almost none of these rules, so a clean
lint run is not evidence that a page follows them.

| File | Covers |
|---|---|
| [`documentation.md`](documentation.md) | Voice, tone, grammar, formatting, tables, and links |
| [`documentation-topics.md`](documentation-topics.md) | The Concept, Task, Reference, and Troubleshooting topic model, and topic titles |
| [`documentation-feature-lifecycle.md`](documentation-feature-lifecycle.md) | Feature flags, availability details, and Experiment and Beta status |
| [`documentation-api.md`](documentation-api.md) | REST and GraphQL reference pages only |

## How the sync works

A daily scheduled pipeline runs the `doc-principles-sync` job
(`.gitlab/ci/doc-principles-sync.yml`), which refetches the files and opens a merge request
when any of them changed upstream.
Only `documentation*.md` is synced.
The distilled directory also holds backend, frontend, database, Ruby, and Vue principles
that do not apply to this repository.

The sync adds and updates files, and never deletes them, so a file removed here stays
removed until someone restores it.

This directory mirrors the upstream path deliberately, and it sits outside `docs/`.
Everything under `docs/` is both prose-linted and mounted as content by the Hugo docs
build, and these files fail on both counts.
They are written upstream, so they cannot be changed to satisfy local rules, and they
document Hugo shortcodes that the docs build would try to expand.
Only the link checker reaches this directory, and it skips it.

Do not edit the `documentation*.md` files.
Each one carries a `source_checksum` and a "do not edit manually" banner from the
generator that produces it.
Changes belong upstream in `gitlab-org/gitlab`.
This README is not synced, so it can be edited freely.

## Schedule setup

The job runs only on a pipeline schedule that marks itself.
It uses the Orbit automation bot's Vault token through the `.automation-bot` template, so
no new token is needed:

1. Create a daily pipeline schedule (**Project** > **CI/CD** > **Schedules**) on `main`
   with the variable `SCHEDULE_ONLY` set to `doc-principles-sync`.
   That marker makes the schedule run only this job (see `.skip-on-solo-schedule` in
   `.gitlab-ci.yml`).
1. To rehearse it, set `DRY_RUN` to `true` on the schedule and run it once.
   The job then logs the diff and stops before it pushes anything.
   Remove that variable to go live.

To rehearse the fetch locally, run `mise run docs:principles:sync`.
