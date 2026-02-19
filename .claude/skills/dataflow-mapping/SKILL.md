---
name: dataflow-mapping
description: Trace and document how data transforms through a multi-step pipeline or function chain, showing intermediate state at each step with concrete example values. Use when explaining a data pipeline or complicated codepaths, tracing how a value changes across function calls, answering questions like "how does X get to Y", or producing a step-by-step dataflow walkthrough for a code review or design doc.
---

# Dataflow Mapping

Produce annotated pseudocode traces that show exactly how data changes at each step. Prefer concrete example values over abstract descriptions. The reader should be able to follow one piece of data from input to output without reading the source.

## Format

```
-- <stage name> --
<data state with concrete values>
  step → <what happens>       → <resulting state>
  step → <what happens>       → <resulting state>

→ <output type> { field: value, ... }
```

Label each stage with the function or method responsible. Use `→` to show transformations inline. Use indented continuation lines for multi-step operations within a single stage. Show the output type at the end of each stage.

Use `✓` and `✗` for pass/fail outcomes. Use `←` for annotations explaining why a value is what it is.

## When to use which depth

- **Single function**: show input → output only
- **Multi-stage pipeline**: show intermediate state after each stage
- **Authorization/filtering flows**: show per-item decisions, then the final set

## Examples

### Example 1 — multi-stage pipeline with per-row filtering

```
-- HTTP response (JSON) --
row 0: { id: 1, owner_id: 42, name: "foo", visibility: "private" }
row 1: { id: 5, owner_id: 99, name: "bar", visibility: "public"  }

-- parse_rows() → Vec<Record> --
records = [
  Record { id: 1, owner_id: 42, name: "foo", visibility: Private,  authorized: true },
  Record { id: 5, owner_id: 99, name: "bar", visibility: Public,   authorized: true },
]

-- collect_auth_checks() --
ids = {}
  record 0 → owner_id = 42, policy = "read"
             ids[("projects", "read")].insert(42)   → { ("projects","read"): {42} }
  record 1 → owner_id = 99, policy = "read"
             ids[("projects", "read")].insert(99)   → { ("projects","read"): {42, 99} }

→ AuthCheck { resource: "projects", policy: "read", ids: [42, 99] }

-- auth service round-trip --
request:  { resource: "projects", policy: "read", ids: [42, 99] }
response: { 42: true, 99: false }

-- apply_decisions() --
  record 0 → owner_id = 42 → decisions.get(42) = true  ✓  record stays
  record 1 → owner_id = 99 → decisions.get(99) = false ✗  record.set_unauthorized()

→ records = [
    Record { id: 1, ..., authorized: true  },   ← returned to caller
    Record { id: 5, ..., authorized: false },   ← filtered out
  ]
```

### Example 2 — key transformation through a compile pipeline

```
-- Input --
{ "type": "query", "node": { "id": "n", "entity": "Invoice" } }

-- normalize() --
  node.entity = "Invoice"
  schema.get("Invoice").auth_column = "account_id"   ← not the PK
  node.auth_id_column = "account_id"                 ← set from schema
  entity_auth["Invoice"] = AuthConfig {
      resource:   "accounts",
      policy:     "read_billing",
      id_column:  "account_id",
      owner_type: Some("Account"),
  }

-- codegen() --
  SELECT n.account_id AS _auth_n_id    ← auth_id_column used here, not "id"
         n."type"     AS _auth_n_type
  ctx.entity_auth = { "Invoice" → AuthConfig { ... } }

→ CompiledQuery { sql: "SELECT n.account_id AS _auth_n_id ...", ctx }
```

### Example 3 — single function, input/output only

```
-- build_auth_map(schema) --
input:  schema entities with auth config
  "Account" → { resource: "accounts",  policy: "read",         id_column: "id"         }
  "Invoice" → { resource: "accounts",  policy: "read_billing", id_column: "account_id" }
  "User"    → { resource: "users",     policy: "read_user",    id_column: "id"         }

output: HashMap<String, AuthConfig> {
  "Account" → AuthConfig { resource: "accounts", policy: "read",         auth_col: "id",         owner: None             }
  "Invoice" → AuthConfig { resource: "accounts", policy: "read_billing", auth_col: "account_id", owner: Some("Account")  }
  "User"    → AuthConfig { resource: "users",    policy: "read_user",    auth_col: "id",         owner: None             }
}
```

## Tips

- Pick example values that reveal edge cases (e.g. one authorized ID and one denied).
- When two stages share the same data, call it out explicitly: `← same value used in apply_decisions()`.
- For `HashMap`/`HashSet` collections, show the accumulation across iterations, not just the final state.
- Omit fields that don't change or aren't relevant to the question being answered.
- Adapt type names and field names to the actual codebase — the format is generic, the values should be concrete.
