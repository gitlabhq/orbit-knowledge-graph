---
title: Querying
description: How Orbit turns a graph question into one bounded, authorized query.
order: 5
authors:
  - michaelusa
---

Querying turns a graph question into one bounded, authorized query. The pages
in this section follow a query from the caller to ClickHouse:

- [Query lifecycle](query-lifecycle.md) follows one query through GitLab,
  the query service, the relational store, redaction, and hydration.
- [Orbit Query Engine](query-engine.md) shows how the compiler turns any query
  language into secure SQL for any backend.
