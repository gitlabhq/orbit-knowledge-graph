---
title: "ADR-001: FFI vs Dedicated Process Integration"
owning-stage: "~devops::create"
toc_hide: true
---

## Context

The initial implementation used Foreign Function Interface (FFI) to integrate the
Rust-based Knowledge Graph indexing functionality within the Go-based `gitlab-zoekt-indexer`
service. This approach was chosen to simplify deployment within Omnibus and reduce
operational complexity. Exposing knowledge graph for querying was planned to be done
directly from `gitlab-zoekt` service (not through FFI).

However, after committing to the [segmentation strategy](../../selfmanaged_segmentation/_index.md),
the Omnibus constraints no longer apply to the Knowledge Graph service.

## Problem

The FFI approach presented several challenges:

1. **Querying Complexity**: Knowledge Graph querying now requires additional
   query building, and result type mapping, which would require FFI usage
   also for query pre-processing and post-processing
2. **Separation of Concerns**: The Knowledge Graph's increasing complexity for both
   indexing and querying makes will make it difficult to use from zoekt-indexer
3. **Independent Upgrades**: The FFI model requires two upgrade maintenance points
   for any API changes - both the Knowledge Graph repo and the Zoekt indexer repo
4. **Observability**: FFI makes it difficult to monitor and debug the Knowledge Graph
   components independently
5. **Safety Concerns**: Unsafe FFI code poses potential security and stability risks
6. **Bindings Library Dependency Issues**: Because Knowledge Graph static
   library is quite big, there were issues with distributing this as part of Go
   module. We also hit an issue with go-kuzu module which uses dynamically
   linked library which is then missing in zoekt-indexer binary -
   [issue 100](https://gitlab.com/gitlab-org/gitlab-zoekt-indexer/-/issues/100),
   we would have to build static library for go-kuzu.

## Decision

Move away from FFI-based integration and adopt a dedicated process model where:

- Knowledge Graph functionality is encapsulated within its own long-running processes in a separate stateful set
- Services are deployed as "sidecar" containers within the same Kubernetes pod as Zoekt container
- The existing `gitlab-zoekt-indexer` service acts as a proxy which handles
  authentication, processing incoming requests, fetching repository using Gitaly
  and forwarding requests to the Knowledge Graph Service
- Knowledge graph indexing and querying requests are forwarded to dedicated GKG
  service

## Consequences

### Benefits

- Fault Isolation: KG crashes don't affect the Go service
- Independent build lifecycle: Building a new version of KG won't requiring building and releasing a new `gitlab-zoekt` binary
- Better Observability: Dedicated processes expose their own health, metrics, and logs
- Conventional API semantics: HTTP/gRPC provides schema validation and versioning in a way that is common in the GitLab architecture which may make it easier to spot and resolve compatibility issues
- Testability: Black-box testing directly against KG services

### Concerns

- Unlike Zoekt code search, this feature will not be available in Omnibus and
  will require advanced components or use of CNG. Dependence on these components
  may delay the dedicated release as we work on tooling as part of
  https://handbook.gitlab.com/handbook/engineering/architecture/design-documents/selfmanaged_segmentation/
- Usage of a separate GKG service is a blocker for including Knowledge Graph in
  Omnibus in future if needed
- More complex deployment - instead of deploying single Zoekt service, two
  separate services will be deployed
- Additional complexity in inter-service communication
- Need to build and maintain separate container images
- Different programming language - GKG is written in Rust

## Implementation

This decision was made based on discussion in [issue #168](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/168).

## References

- [Knowledge Graph Server Epic](https://gitlab.com/groups/gitlab-org/-/epics/17518)
- [FFI vs Dedicated Process Discussion](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/168)
- [Segmentation Strategy](../../selfmanaged_segmentation/_index.md)
- [Knowledge Graph First iteration](https://gitlab.com/groups/gitlab-org/-/epics/17514) - a top-level epic for all Knowledge Graph work
