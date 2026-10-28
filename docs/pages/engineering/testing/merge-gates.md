---
title: Merge gates
description: The checks and reviews that run next to the tests.
order: 8
authors:
  - michaelangeloio
---

Every merge request runs static analysis, schema checks, freshness
checks, and version checks next to the tests. Review automation and code
owners add a second check for security, performance, and billing.

## Contracts

- Build checks, unit tests, and both integration layers run on every merge
  request and every commit to the main branch, and they block a merge.
- In CI, every test run continues after a failure, retries a failed test,
  and publishes a result for each test.
- Static analysis, formatting, dependency audits, and secret detection run on
  every merge request.
- Schema checks validate the ontology, the named queries, the migration
  ledger, and the scenario files.
- Freshness checks fail when a generated file does not match its source.
- A contract check compares the redaction settings in the ontology with the GitLab permission policies of the pinned GitLab commit. It runs on main and on merge requests that change the ontology, the pins, or the check itself, and it blocks a merge.
- Version checks fail when a change to the query language, the response
  format, the agent skills, or the prompts does not bump its version.
- Documentation checks run style, Markdown, and link checks on the user
  docs, and a prose check on the text that agents read.
- Merge request titles follow the conventional commit format.
- Local hooks check the commit message, secrets, formatting, and lints before
  a commit, and static analysis before a push. CI does not depend on them.
- Review automation checks each change for security, performance, logging
  safety, and the billing boundary. A person can start a deeper automated
  review of performance, security, or docs.
- Billing code is in scope for SOX. A check keeps it behind one boundary, and
  changes to it need approval from the Orbit team.
