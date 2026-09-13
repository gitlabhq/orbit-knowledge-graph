## Orbit Code Indexing v2.0

This document discusses the next generation of code indexing for Orbit.

Orbit has reached critical mass on Code Indexing on ClickHouse in .com production today, with 12+ billion rows in the `code_edges` table alone. Additionally, users have marked Orbit missing both file content search and branches/commits as a limitation in Orbit usage. A separate datastore is required to meet both scaling requirements and user needs. As part of our [original strategy](https://gitlab.com/gitlab-com/content-sites/handbook/-/blob/9c2b62bb3cec6ef86ea672c2614d7208c2287171/content/handbook/engineering/architecture/design-documents/gitlab_knowledge_graph/indexing/code_indexing.md#a-future-strategy), we will:
- Migrate Orbit’s code indexing features to a Object Storage. 
- Build a stateless code indexing engine on top of Object Storage that can handle both branches and commits.
- Add support for content search (text search, regex search) and code graph capabilities (find related files, find related code, find related definitions, etc.).

We've broken down the document into the following sections:
- [Motivation](./motivation.md)
- [Functional Requirements](./functional_requirements.md)
- [Architecture](./architecture.md)
- [Branch & Commit Indexing](./branch_and_commit_indexing.md)
- [Content Search](./content_search.md)
- [Code Graph](./code_graph.md)
