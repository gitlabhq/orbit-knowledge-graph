---
title: Integration without infrastructure
description: Tests that run real Orbit code with no external services.
order: 3
authors:
  - michaelangeloio
  - michaelusa
---

These tests run the query compiler, the code-graph engine, and the
command-line tool with no external services. They are the fastest layer that
runs the real code path.

## Query compiler

- Compiler tests check the generated queries for each database dialect,
  ontology loading, virtual columns, and named queries.
- A mock of the GitLab internal API serves file content and merge request
  diffs.
- Plan checks give a query and the plan that the compiler must pick. The
  plan names the relationship table to scan, where to narrow, and how to load
  node properties. Plan checks do not measure latency, and they do not replace
  query scenarios.
- Every query scenario also runs here against an in-process local database,
  so a broken scenario fails without containers.

## Code-graph engine

- A suite declares some source files inline. The harness indexes them with
  the real engine and checks the graph with graph queries.
- A suite fails if the engine writes a relationship that the graph schema
  does not declare.
- Each language and framework has its own suites. Incremental suites can add,
  change, and remove files in steps.
- A new suite file becomes a test with no registration.

## Command-line tool

- Tests run the real binary against temporary repositories and local graph
  databases.
- They cover concurrent readers and writers, repeated indexing, worktrees,
  nested repositories, and files the parser cannot read.
- They also cover setup outside a repository, the bundled skills, and the
  MCP server.
- Release pipelines install the release archives and run a smoke test.
