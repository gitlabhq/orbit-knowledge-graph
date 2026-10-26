---
title: Indexing
description: How GitLab data and code become the Orbit graph, and how the graph changes shape.
order: 2
authors:
  - jgdoyon1
---

Indexing turns GitLab data into the Orbit graph and keeps it up to date. The
pages in this section follow that work from the data to the graph:

- [SDLC indexing](sdlc-indexing.md) builds the graph from replicated GitLab
  data.
- [Code indexing](code-indexing.md) builds the code graph from repositories.
- [Schema evolution](schema-evolution.md) moves the graph to a new schema with
  no downtime.
- [Resiliency](resiliency.md) explains how all of them recover from crashes,
  retries, and outages.
