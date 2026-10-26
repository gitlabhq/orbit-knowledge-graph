---
title: "Orbit as a platform (O11y, Artifact central, Security integrations)"
description: Connect observability, artifact, and security data to the code and changes in the graph.
theme: data
status: planned
start: FY27 Q4
end: FY28 Q1
rank: 2
---

Orbit will connect more of your software lifecycle to the graph.
An agent can then follow a problem from production back to the change that caused it.
Observability data comes first, in FY27 Q4.

## Planned changes

- Observability: connect metrics, logs, and errors to the code and changes in the graph. Orbit will query the observability service when an agent asks, and will not copy the data.
- Artifact central: connect packages, container images, and build artifacts to the commits that built them and the projects that use them.
- Security integrations: link each vulnerability to the code that uses the affected package, so an agent can find each project and service that it affects.
