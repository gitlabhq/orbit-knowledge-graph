---
name: mermaid-diagrams
description: Generate Mermaid diagrams to visualize architecture, data flow, pipelines, and system design. Use when asked for a diagram, visualization, chart, flowchart, sequence diagram, before/after comparison, or system overview. Covers flowcharts, sequence diagrams, and before/after pairs from code or diffs.
---

# Mermaid Diagrams

Generate Mermaid diagrams that are accurate to the code or diff being described. Default to `flowchart TD` for pipelines and architecture. Use `sequenceDiagram` for request/response flows between services.

Always pair a diagram with a short readout — one paragraph per major component.

## Choosing diagram type

| Situation | Type |
|---|---|
| Pipeline stages, data flow within a service | `flowchart TD` |
| Request/response between services (client → server → DB) | `sequenceDiagram` |
| Before/after a refactor or MR | Two `flowchart TD` blocks side by side |
| File/module structure | `flowchart TD` with `subgraph` per layer |

## Flowchart conventions

- Use `subgraph` to group related stages (e.g. by service, by crate, by layer).
- Show data types on edges when they add information: `-->|"ExtractionOutput\n{ QueryResult }"|`.
- Use `\n` inside node labels for multi-line content.
- Use `["..."]` for rectangular nodes (stages), `(["..."])` for actors/external systems, `[("...")]` for databases.
- Keep node labels to: name + separator line + key responsibilities only. Omit obvious implementation details.
- Direction: `TD` (top-down) for pipelines, `LR` (left-right) for before/after comparisons.

## Before/after pairs

When documenting a refactor or MR, always produce both diagrams in the same file with a `## Before` / `## After` heading. Add a table summarising what was deleted, collapsed, or kept.

## Examples

### Pipeline flowchart

```mermaid
flowchart TD
    subgraph compile ["compile()  —  query-engine"]
        Security["SecurityStage\nClaims → SecurityContext"]
        Compiler["compile()\nnormalize → lower → enforce\n\nResultContext carries:\n  · node layout\n  · entity_auth map"]
    end

    ClickHouse[("ClickHouse")]

    subgraph pipeline ["QueryPipelineService"]
        Extraction["ExtractionStage\n────────────────\nbatches → QueryResult\n(ctx embedded)"]
        Authorization["AuthorizationStage\n────────────────\nresource_checks()\n→ Vec<ResourceCheck>"]
        Rails(["Rails\nRedaction Service"])
        Redaction["RedactionStage\n────────────────\napply_authorizations()\nfail-closed per row"]
        Formatting["FormattingStage\n────────────────\nauthorized_rows() → JSON"]
    end

    Security --> Compiler
    Compiler --> ClickHouse
    ClickHouse -->|"Arrow batches"| Extraction
    Compiler -->|"ResultContext"| Extraction
    Extraction -->|"ExtractionOutput\n{ QueryResult }"| Authorization
    Authorization -->|"Vec<ResourceCheck>"| Rails
    Rails -->|"Vec<ResourceAuthorization>"| Authorization
    Authorization -->|"AuthorizationOutput"| Redaction
    Redaction -->|"RedactionOutput"| Formatting
```

### Sequence diagram (cross-service request flow)

```mermaid
sequenceDiagram
    actor Client
    participant Rails
    participant WebServer as GKG Web Server
    participant AuthEngine as Query Pipeline

    Client->>Rails: Send Request
    Rails->>WebServer: Query Knowledge Graph

    activate WebServer
    WebServer->>WebServer: Compile & Execute Graph Query
    WebServer->>AuthEngine: Pass result set
    activate AuthEngine

    AuthEngine->>AuthEngine: resource_checks() — group by (resource_type, ability)
    AuthEngine->>Rails: Vec<ResourceCheck>
    Rails-->>AuthEngine: Vec<ResourceAuthorization>

    AuthEngine->>AuthEngine: apply_authorizations() — filter rows
    AuthEngine->>WebServer: Redacted QueryResult
    deactivate AuthEngine
    deactivate WebServer

    WebServer-->>Rails: Response
    Rails-->>Client: Response
```

### Before/after refactor

```mermaid
flowchart LR
    subgraph before ["Before"]
        direction TB
        A["ExtractionStage\nheld Arc<Ontology>\nbuilt RedactionPlan"]
        B["AuthorizationOutput\n{ QR + authorizations\n  + result_context\n  + entity_map + sql }"]
        A --> B
    end

    subgraph after ["After"]
        direction TB
        C["ExtractionStage\nstateless\nbatches → QueryResult"]
        D["AuthorizationOutput\n{ QR + authorizations }"]
        C --> D
    end

    before -. "collapse" .-> after
```

## Readout format

After each diagram, write one paragraph per major component:

**ComponentName** — What it does, what it takes as input, what it produces, any notable constraints or design decisions.

Keep each paragraph to 2–4 sentences. Do not restate what is already visible in the diagram node label.

## Anti-patterns

- Don't use `graph` — use `flowchart` instead (stricter syntax, better rendering).
- Don't put full struct definitions in node labels — show only the fields relevant to the diagram's argument.
- Don't use `-->` for synchronous calls in sequence diagrams — use `->>`.
- Don't skip the readout — diagrams without prose leave readers inferring intent.
