---
name: seed-data-snippet
description: Generate a seed data comparison snippet from the data_correctness integration test seed function. Use when seed data changes and the snippet needs updating, or to audit test assertions against the current topology.
allowed-tools: Read, Glob, Grep, Bash(glab *)
---

# Generate seed data snippet

Reads the `seed()` function and `allow_all()` helper from the data correctness integration tests and produces a markdown snippet matching the canonical format. The output is used to audit test assertions against the actual seeded topology.

## Source files

The seed data lives in one of these locations (check which exists):

- `crates/integration-tests/tests/server/data_correctness.rs` (single file)
- `crates/integration-tests/tests/server/data_correctness/helpers.rs` (split modules)

Read the file and extract:

1. The `seed()` function — all INSERT statements
2. The `allow_all()` function — the redaction service setup
3. The topology comment above `seed()` (if present)

## Output format

The output must exactly follow this format. Each entity table is a separate fenced code block with monospace-aligned columns. Do NOT use markdown tables.

### Section 1: Seed data

One code block per entity type. Columns are space-aligned. Include all columns from the INSERT statement.

```
USERS  id  username          name              state    user_type
        1  alice             Alice Admin       active   human
        2  bob               Bob Builder       active   human
```

Notes about nullable/unset columns go at the bottom of the relevant block.

### Section 2: Edges

A single code block with edge types as column headers, edges listed underneath in `Source → Target` format.

```
MEMBER_OF          CONTAINS              AUTHORED            HAS_NOTE
User 1 → Grp 100  Grp 100 → Prj 1000   User 1 → MR 2000   MR 2000 → Note 3000
```

### Section 3: Traversal path hierarchy

A tree diagram using box-drawing characters showing the namespace nesting.

```
1/                          (org root)
├── 100/                    (Public Group)
│   ├── 200/                (Deep Group A)
```

Annotate leaf nodes with what entities live there (MRs, Notes).

### Section 4: allow_all() redaction service

A `rust` fenced code block with the exact `svc.allow()` calls.

### Section 5: Graph

A `mermaid` fenced code block with a `graph LR` diagram showing all nodes and edges. Use short labels:

- Users: `U1[User 1 alice]`
- Groups: `G100[Grp 100 Public]`
- Projects: `P1000[Prj 1000]`
- MergeRequests: `MR2000[MR 2000]`
- Notes: `N3000[Note 3000]`

## Reference output

The current canonical snippet is at:
https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5970145

## Publishing

After generating the snippet content, offer to publish it:

```bash
glab snippet create \
  --title "Data Correctness Tests - Seed Data - <DATE>" \
  --description "Seed data topology for data_correctness integration tests." \
  --filename "data.md" \
  --visibility public <<'EOF'
<generated content>
EOF
```

## When to use this skill

- After modifying the `seed()` function (new entities, changed columns, new edges)
- After modifying `allow_all()` (new entity IDs)
- When auditing test assertions against the topology
- When onboarding someone to the test suite
