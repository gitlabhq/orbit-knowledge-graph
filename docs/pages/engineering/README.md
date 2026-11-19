---
description: How to contribute to GitLab Orbit, and how Orbit is built, tested, and operated. The entry point to the Orbit developer documentation.
---

# Developer docs

These pages are for everyone who builds GitLab Orbit: how to contribute, and how Orbit is built, tested, and operated.
To use Orbit, see the [GitLab Orbit documentation](https://docs.gitlab.com/orbit/).

## Contribute

GitLab team members contribute to this repository directly. Community contributors use the [community fork](https://gitlab.com/gitlab-community/gitlab-org/orbit/knowledge-graph).

Install [mise](https://mise.jdx.dev/), then build Orbit and run the unit tests:

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise build
mise test:fast
```

Then:

1. Pick an [open issue](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues). The `orbit::hackathon` label marks good first issues.
1. [Write your first Orbit query](development/contributor-guides/first-query.md) to see the graph from the inside.
1. Read [CONTRIBUTING.md](../../../CONTRIBUTING.md) for the conventions for code, docs, issues, and merge requests.
1. GitLab team members can run the full stack on their computer with [local development](development/local-development.md).
