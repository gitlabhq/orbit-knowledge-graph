---
name: fetch-doc-guidelines
description: >
  Fetch the canonical documentation style guidelines from the GitLab monolith
  before writing or editing any file under docs/. Use whenever a task involves
  creating or modifying documentation in the docs/ directory.
allowed-tools: get_repository_file
---

# Fetch documentation guidelines

## When to use

Before writing or editing any file under `docs/` in this repository.

## Steps

1. Call `get_repository_file` with:
   - `project_id`: `gitlab-org/gitlab`
   - `ref`: `master`
   - `file_path`: `.ai/principles/distilled/documentation.md`
2. Apply all guidelines from the returned content to your writing.
3. Proceed with the documentation task.
