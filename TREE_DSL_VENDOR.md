# Tree-DSL Canonical Rewrite Pipeline

This branch tracks the tree-dsl rewrite experiment from orbit-next.

## Source MR

https://gitlab.com/gitlab-org/orbit/experiments/orbit-next/-/merge_requests/219

## Summary

Rewrites the tree-dsl indexer pipeline around canonical synthetic nodes.
Every language produces the same output shapes through YAML-defined rewrite
rules. SSA and resolver read synthetics directly.

42/44 Python tests pass (parity with old pipeline). -3,387 lines net.
5,528 lines total. 24 synthetic node kinds. 8 languages verified.
