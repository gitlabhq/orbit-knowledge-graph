---
title: Integration with real infrastructure
description: Tests that run Orbit against a real graph store and a real message broker.
order: 4
authors:
  - michaelangeloio
  - michaelusa
---

These tests run Orbit against a real graph store and a real message
broker in containers. Query scenarios are the main proof of query
correctness.

## Isolation

- One graph store serves the whole run. The harness creates each schema once
  and clones it into isolated databases.
- Query tests create only the graph schema. Indexer tests also create the
  source schema.
- Tests that only read share one database. Tests that write get their own
  clone.
- The harness compacts the seed data before the tests read it, so every read
  is deterministic.
- CI splits the tests into parallel lanes. A check fails if a test is in no
  lane or in two lanes.

## Query scenarios

- A scenario runs the full query path, from compilation to the formatted
  response, and checks the result.
- A scenario can give a query in both query languages. When it does, both
  must return the same result.
- The runner checks response schemas and page metadata. Repeated queries
  must return the same order, properties, and relationships.
- A scenario can also check the generated query and the indexes that the
  graph store uses.

## Indexer scenarios

- A scenario gives the source rows to seed, the steps to run, and the nodes,
  relationships, or messages to expect.
- A schema check validates every scenario file.

## Message broker

- Tests check acknowledgements, redelivery, keep-alives for long work, and
  the rule of one task in progress for each subject.
- Tests check delivery limits, the clean-up of stuck messages, the dead-letter
  queue, and locks.
- Handler logic uses an in-memory broker, so most handler tests need no
  broker.
- A suite makes a certificate authority at run time and proves that the
  broker rejects a client with no certificate or the wrong one.

## Analytics events

- A test sends a query event to a local event collector and checks that the
  collector accepts it with the Orbit contexts.
- A check compares the vendored event schemas with the public registry.

## Query corpus and executable docs

- The corpus runs a curated set of real queries, every named query, and the
  marked query examples. Each one goes through the production query stages.
- The corpus fails on a query in an unmarked JSON or shell code block, and on
  an unclosed query or shell code block.
