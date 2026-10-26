---
title: Build checks
description: The checks that run when Orbit compiles, before any test runs.
order: 1
authors:
  - michaelangeloio
---

Build checks verify the inputs that the code depends on but that
the compiler cannot type-check. A failed check stops the build on a laptop and
in CI.

## Contracts

- Vendored schemas and vendored lists from GitLab are valid and agree with
  their pinned versions.
- The public query schema agrees with the query limits in the code.
- The local prompts load, the Orbit skills agree with the command-line tool,
  and the agent guide that lists the workspace is complete.
- The crate that owns an input owns its check.
