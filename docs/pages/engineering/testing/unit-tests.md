---
title: Unit tests
description: Tests for pure logic and repository consistency.
order: 2
authors:
  - michaelangeloio
---

Unit tests cover pure logic, such as parsing and name mapping. They
need no external services.

## Contracts

- A unit test sits next to the code it tests.
- Repository-consistency checks run as unit tests, so the fastest test run
  catches drift. Examples are the named queries, the migration ledger, and
  the bundled ontology archives.
