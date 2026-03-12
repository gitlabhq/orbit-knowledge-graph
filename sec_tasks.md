# TQ1: SQL Injection — Security Task Results

## Task 1: Fuzz all JSON input fields with SQL injection payloads

**Status:** PASS — no injection vectors found

All user-supplied values flow through `emit_param()` which produces ClickHouse `{pN:Type}` placeholders. No value ever reaches the SQL string directly. The full input→SQL trace:

```
JSON → check_json() (schema validation) → check_ontology() → serde deserialize → normalize() → lower() → codegen()
```

Every field in the `Input` struct was traced to its SQL output:

| Field | Reaches SQL as | Parameterized? |
|-------|---------------|----------------|
| `nodes[].filters` values | `Expr::Param` → `{pN:Type}` | YES |
| `nodes[].node_ids` | `Expr::col_in()` → `{pN:Int64}` | YES |
| `nodes[].id_range.start/end` | `Expr::int()` → `{pN:Int64}` | YES |
| `relationships[].types` | `Expr::col_in()` → `{pN:String}` | YES |
| `path.rel_types` | `Expr::col_in()` → `{pN:String}` | YES |
| `limit` / `offset` | `u32` — numeric type, no injection possible | N/A |

Identifiers (table names, column names, aliases) are interpolated directly into SQL but defended by:
- JSON Schema regex: `^[a-zA-Z_][a-zA-Z0-9_]{0,63}$` (`config/schemas/graph_query.schema.json:175-181`)
- Ontology allowlist validation (`validate.rs` check_ontology)

### References

- Compilation pipeline (validation → lowering → codegen): `crates/query-engine/src/lib.rs:91-113`
- JSON Schema validation entry: `crates/query-engine/src/validate.rs:112-115`
- Ontology-derived schema validation: `crates/query-engine/src/validate.rs:119-132`
- Cross-reference validation: `crates/query-engine/src/validate.rs:135-148`
- `emit_param()` — all values become `{pN:Type}` placeholders: `crates/query-engine/src/codegen.rs:255-303`
- `emit_literal()` — delegates to `emit_param()`: `crates/query-engine/src/codegen.rs:305-317`
- `filter_expr()` — filter values → `Expr::param()`: `crates/query-engine/src/lower.rs:965-984`
- `id_filter()` — node_ids → `Expr::col_in(ChType::Int64)`: `crates/query-engine/src/lower.rs:956-962`
- `type_filter()` — relationship types: `crates/query-engine/src/lower.rs:657`
- Identifier regex definition: `config/schemas/graph_query.schema.json:175-181`
- Identifier refs across schema (node.id, filters, rel from/to, etc.): `config/schemas/graph_query.schema.json:229,244,266,283,392,396,422,480,484,488,492,520,524,553,577,581`

---

## Task 2: String concatenation audit — codegen.rs

**Status:** PASS with one dormant finding

Audited every `format!`/`write!` in `crates/query-engine/src/codegen.rs`. 30+ interpolation sites reviewed.

**All safe except one dormant path:**

| Location | Code | Risk |
|----------|------|------|
| `codegen.rs:124` | `format!("SET {key} = {value};")` | **DORMANT** — `set_statements` is always `vec![]` (ast.rs:209). Never populated by any lowerer code path. Dead code. |

All other interpolations are either:
- Compile-time literals (function names, operators, keywords)
- Ontology-derived values (table names via `normalize.rs:71-80`)
- Schema-validated identifiers (regex `^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)
- Parameterized values via `emit_param()`

**Additional finding — LIKE metacharacter escape gap:**
- `lower.rs:987-989`: user filter values in `contains`/`starts_with`/`ends_with` are parameterized (no SQL injection) but `%` and `_` chars are not escaped, allowing semantic broadening of LIKE matches. Not an injection bug, but a correctness issue.

### References

- All `format!` interpolation sites in codegen: `crates/query-engine/src/codegen.rs:124,150,154,169,174,178,182,188,193,198,208,211,216,219,227,231-232,238,241,247-249,260-261,277-278,288,291-292,313,321,331-333,340,344`
- Dormant SET statement injection: `crates/query-engine/src/codegen.rs:123-124`
- `set_statements` always empty: `crates/query-engine/src/ast.rs:190,209`
- LIKE pattern construction (no metachar escape): `crates/query-engine/src/lower.rs:987-989`
- LIKE callers (contains/startsWith/endsWith): `crates/query-engine/src/lower.rs:979-981`

---

## Task 3: Integer parsing rejects non-numeric strings

**Status:** PASS

`node_ids` field (`input.rs:167`):
```rust
pub node_ids: Vec<i64>,
```

**Defense layers:**
1. **JSON Schema** (`graph_query.schema.json:252-259`): type `integer`, minimum 1, maxItems 500
2. **Serde deserialization**: `Vec<i64>` — rejects any non-integer JSON value at parse time
3. **SQL binding**: flows through `id_filter()` → `Expr::col_in(ChType::Int64)` → `{pN:Int64}` placeholder

A non-numeric string in `node_ids` fails at JSON Schema validation before it ever reaches Rust deserialization.

### References

- `node_ids` field definition (`Vec<i64>`): `crates/query-engine/src/input.rs:167`
- Default empty vec: `crates/query-engine/src/input.rs:190`
- JSON Schema constraint (integer, min 1, maxItems 500): `config/schemas/graph_query.schema.json:251-259`
- `id_filter()` converts to `Value::from(id)` + `col_in(ChType::Int64)`: `crates/query-engine/src/lower.rs:956-962`
- `col_in()` wraps in `Expr::Param`: `crates/query-engine/src/ast.rs:305`
- Cardinality cap validation: `crates/query-engine/src/validate.rs:191-194`

---

## Task 4: Verify emit_literal() produces {pN:Type} placeholders for all Value variants

**Status:** PASS

`emit_literal()` (`codegen.rs:305-317`) and `emit_param()` (`codegen.rs:255-303`):

| Value Variant | Output | Parameterized? |
|---------------|--------|----------------|
| `Value::Null` | literal `"NULL"` | N/A (no user data) |
| `Value::Array` + `ChType::Array(T)` | `{pN:Array(T)}` | YES |
| `Value::Array` + scalar ChType | `({p0:T}, {p1:T}, ...)` per element | YES |
| `Value::String` | `{pN:Type}` | YES |
| `Value::Number` | `{pN:Type}` | YES |
| `Value::Bool` | `{pN:Type}` | YES |

No Value variant falls through to raw string interpolation.

**Defense-in-depth:** `check_ast()` (`check.rs:19-28`) runs post-compilation and verifies that every `gl_*` table alias has a `startsWith(traversal_path, path)` security predicate. This catches security filter injection bugs even if codegen is correct.

### References

- `emit_param()` — all Value arms produce `{pN:Type}`: `crates/query-engine/src/codegen.rs:255-303`
- `emit_literal()` — Null → `"NULL"`, Array → per-element `emit_param`, else → `emit_param`: `crates/query-engine/src/codegen.rs:305-317`
- `ChType::from_value()` type inference for literals: `crates/utils/src/ch_type.rs` (used at codegen.rs:311,315)
- `check_ast()` post-compilation security verification: `crates/query-engine/src/check.rs:19`
- `apply_security_context()` traversal_path injection: `crates/query-engine/src/security.rs` (called at lib.rs:100)
- `codegen()` entry point: `crates/query-engine/src/codegen.rs` (called at lib.rs:102)

---

---

# TQ2: Auth Filter Bypass — Security Task Results

Architecture: two-phase defense-in-depth. Phase 1 (`apply_security_context`) mutates the AST to inject `startsWith(alias.traversal_path, path)` predicates on every `gl_*` table scan. Phase 2 (`check_ast`) is a read-only post-compilation verifier that rejects any query where a `gl_*` alias lacks a valid predicate.

## Task 1: Every generated SQL includes traversal_path predicate

**Status:** PASS

`check_ast()` recursively walks the entire AST — main query, CTEs, UNION ALL arms, subqueries, joins — and rejects any `gl_*` alias missing a valid `startsWith(traversal_path, path)` predicate. The path literal must be a prefix of at least one `SecurityContext` traversal path.

All query types pass through `compile()` which calls both `apply_security_context` and `check_ast` in sequence (`lib.rs:100-101`). Compile-through tests exist for every query type (traversal, aggregation, path finding, neighbors, search).

**Gap:** No property-based tests (proptest/quickcheck) generating arbitrary query shapes. Coverage relies on handwritten cases covering known AST shapes.

### References

- `compile()` pipeline — inject then verify: `crates/query-engine/src/lib.rs:100-101`
- `apply_security_context()` entry: `crates/query-engine/src/security.rs:73`
- `apply_to_query()` — collects aliases, builds filters, recurses: `crates/query-engine/src/security.rs:84-106`
- `build_path_filter()` — single/multi-path predicate construction: `crates/query-engine/src/security.rs:108-121`
- `should_apply_security_filter()` — `gl_*` prefix check, skips `gl_user`: `crates/query-engine/src/security.rs:192-203`
- `SKIP_SECURITY_FILTER_TABLES` constant: `crates/query-engine/src/constants.rs:30`
- `check_ast()` entry: `crates/query-engine/src/check.rs:19`
- `check_query()` — alias collection + `has_valid_path_filter`: `crates/query-engine/src/check.rs:30-47`
- `has_valid_path_filter()` — recursive expression tree search: `crates/query-engine/src/check.rs:68-101`
- Compile-through tests (implicit verify): `crates/query-engine/src/lib.rs:214` (traversal), `:262` (search), `:284` (aggregation), `:300` (path), `:375` (neighbors)

---

## Task 2: Recursive CTEs have auth filters in each CTE

**Status:** PASS

`apply_security_context()` iterates over `q.ctes` and calls `apply_to_query()` on each CTE body. `check_ast()` does the same for verification. For path queries, the base CTE query scanning a `gl_*` start table gets a traversal_path filter. Recursive branches scanning `gl_edge` also get filters. Non-`gl_*` aliases (e.g. the `paths` CTE itself) are correctly skipped.

### References

- `apply_security_context()` walks CTEs: `crates/query-engine/src/security.rs:76-78`
- `check_ast()` walks CTEs: `crates/query-engine/src/check.rs:22-24`
- Path query lowering (recursive CTE): `crates/query-engine/src/lower.rs:215-278`
- `path_base_query()` — base CTE scans `gl_*` start table: `crates/query-engine/src/lower.rs:300`
- `path_recursive_branch()` — joins `paths` CTE with `gl_edge`: `crates/query-engine/src/lower.rs:308-422`
- Test: `rejects_cte_with_sensitive_table_missing_filter`: `crates/query-engine/src/check.rs:365`
- Test: `accepts_cte_with_security_filter`: `crates/query-engine/src/check.rs:400`
- Compile-through: `path_finding_query()`: `crates/query-engine/src/lib.rs:300`
- E2E: `path_finding_redaction_blocks_path`: `crates/integration-tests/tests/server/graph_formatter.rs:706`

---

## Task 3: UNION queries have auth filters in all branches

**Status:** PASS

`apply_security_to_from()` handles `TableRef::Union` by iterating all arm queries and calling `apply_to_query()` on each. Separately, `apply_to_query()` recurses into the `Query.union_all` field (used by recursive CTEs). `check_ast()` mirrors both paths for verification.

### References

- `apply_security_to_from()` handles `TableRef::Union` arms: `crates/query-engine/src/security.rs:174-177`
- `apply_security_to_from()` handles `TableRef::Subquery`: `crates/query-engine/src/security.rs:179-181`
- `apply_security_to_from()` handles `TableRef::Join`: `crates/query-engine/src/security.rs:182-185`
- `apply_to_query()` recurses into `union_all` arms: `crates/query-engine/src/security.rs:101-103`
- `check_query()` recurses into `union_all` arms: `crates/query-engine/src/check.rs:42-44`
- `check_derived_tables_in_from()` handles Union/Subquery/Join: `crates/query-engine/src/check.rs:51-66`
- Test: `rejects_union_all_arm_without_security_filter`: `crates/query-engine/src/check.rs:306`
- Test: `accepts_union_all_arms_with_security_filters`: `crates/query-engine/src/check.rs:338`
- Test: `rejects_union_arm_missing_security_filter`: `crates/query-engine/src/check.rs:438`
- Test: `accepts_union_arm_with_security_filter`: `crates/query-engine/src/check.rs:480`
- Test: `inject_recurses_into_union_from_arms`: `crates/query-engine/src/security.rs:363`
- Test: `inject_recurses_into_union_all_arms`: `crates/query-engine/src/security.rs:415`

---

## Task 4: Automated tests for predicate stripping attempts

**Status:** PASS

7 explicit rejection tests in `check.rs` simulate missing, wrong, or stripped predicates across different AST shapes. 11 `SecurityContext` input validation tests reject malformed traversal paths. 5 SQL injection tests in `lib.rs` verify schema rejects malicious identifiers. Integration tests verify end-to-end auth enforcement. A `qe.threat.auth_filter_missing` metric fires on any security rejection.

### References

**Rejection tests (check.rs):**

- `fails_without_any_filter` (trivial `WHERE true`): `crates/query-engine/src/check.rs:130`
- `fails_with_wrong_path_literal` (wrong org path): `crates/query-engine/src/check.rs:141`
- `rejects_subquery_without_inner_security_filter`: `crates/query-engine/src/check.rs:204`
- `rejects_aggregate_subquery_without_inner_security_filter`: `crates/query-engine/src/check.rs:234`
- `rejects_union_all_arm_without_security_filter`: `crates/query-engine/src/check.rs:306`
- `rejects_cte_with_sensitive_table_missing_filter`: `crates/query-engine/src/check.rs:365`
- `rejects_union_arm_missing_security_filter`: `crates/query-engine/src/check.rs:438`

**SecurityContext validation (security.rs):**

- `traversal_path_validation()` — 11 cases including org_id mismatch, missing slash, empty, non-numeric, overflow, negative: `crates/query-engine/src/security.rs:225-242`

**SQL injection tests (lib.rs):**

- `sql_injection_in_node_id`: `crates/query-engine/src/lib.rs:429`
- `sql_injection_in_relationship`: `crates/query-engine/src/lib.rs:436`
- `empty_node_id_rejected`: `crates/query-engine/src/lib.rs:447`
- `id_starting_with_number_rejected`: `crates/query-engine/src/lib.rs:452`
- `sql_injection_in_filter_property`: `crates/query-engine/src/lib.rs:460`

**Integration tests:**

- `search_no_authorization_returns_empty`: `crates/integration-tests/tests/server/graph_formatter.rs:308`
- `traversal_redaction_removes_unauthorized_paths`: `crates/integration-tests/tests/server/graph_formatter.rs:412`
- `path_finding_redaction_blocks_path`: `crates/integration-tests/tests/server/graph_formatter.rs:706`
- `traversal_redaction_removes_unauthorized_data`: `crates/integration-tests/tests/server/data_correctness.rs:499`
- `column_selection_fail_closed_on_any_unauthorized_node`: `crates/integration-tests/tests/server/redaction.rs:2320`

**Threat metric:**

- `auth_filter_missing` counter on `QueryError::Security`: `crates/query-engine/src/metrics.rs:122`

---

## Task 5: Verify SKIP_SECURITY_FILTER_TABLES entries are correctly excluded and cannot grow without review

**Status:** PASS with caveat

`SKIP_SECURITY_FILTER_TABLES` is a compile-time `&[&str]` constant containing only `gl_user`. The exclusion is justified: users don't belong to a namespace hierarchy, so they have no `traversal_path` column. Authorization for users is handled via the `redaction` block in the ontology (`user.yaml:15-18`) which delegates to Rails' `read_user` ability check.

The skip list is consumed in exactly one place: `should_apply_security_filter()` at `security.rs:199`. Both `apply_security_context()` (injection) and `collect_node_aliases()` (used by `check_ast()`) go through this function, so the skip applies symmetrically — no filter is injected AND no filter is expected.

**Caveat — no CODEOWNERS gate.** There is no `CODEOWNERS` file in the repo. Any contributor can add entries to `SKIP_SECURITY_FILTER_TABLES` without a mandatory security review. This is a process gap, not a code gap.

**Existing test coverage:**

- `should_apply_security_filter_skips_user` — asserts `gl_user` returns false
- `should_apply_security_filter_skips_ctes` — asserts non-`gl_` tables return false
- `inject_skips_user_table` — asserts `gl_user` alias is not collected in a join with `gl_merge_request`

### References

- `SKIP_SECURITY_FILTER_TABLES` definition (single entry `gl_user`): `crates/query-engine/src/constants.rs:27-30`
- `should_apply_security_filter()` — sole consumer: `crates/query-engine/src/security.rs:192-203`
- `collect_node_aliases()` — uses `should_apply_security_filter` for scan tables: `crates/query-engine/src/security.rs:153-167`
- User ontology — no `traversal_path`, auth via `redaction.ability: read_user`: `config/ontology/nodes/core/user.yaml:7,15-18`
- Test: `inject_skips_user_table`: `crates/query-engine/src/security.rs:314`
- Test: `should_apply_security_filter_skips_user`: `crates/query-engine/src/security.rs:329`
- Test: `should_apply_security_filter_skips_ctes`: `crates/query-engine/src/security.rs:337`

---

## Task 6: Test that subqueries within TableRef::Subquery carry auth filters

**Status:** PASS

`apply_security_to_from()` handles `TableRef::Subquery` by calling `apply_to_query()` on the inner query (`security.rs:179-181`). `check_derived_tables_in_from()` handles `TableRef::Subquery` by calling `check_query()` on the inner query (`check.rs:53`). Both recurse fully — nested subqueries are handled transitively.

**Existing test coverage:**

- `rejects_subquery_without_inner_security_filter` — subquery wrapping `gl_project` without filter is rejected
- `accepts_subquery_with_inner_security_filter` — same subquery with valid `startsWith` filter passes
- `rejects_aggregate_subquery_without_inner_security_filter` — aggregate (COUNT/GROUP BY/HAVING) subquery without filter rejected
- `accepts_aggregate_subquery_with_inner_security_filter` — aggregate subquery with filter passes
- `accepts_subquery_wrapping_non_sensitive_table` — subquery wrapping `dedup_cte` (non-`gl_`) correctly passes without filter

### References

- `apply_security_to_from()` handles `TableRef::Subquery`: `crates/query-engine/src/security.rs:179-181`
- `check_derived_tables_in_from()` handles `TableRef::Subquery`: `crates/query-engine/src/check.rs:53`
- Test: `rejects_subquery_without_inner_security_filter`: `crates/query-engine/src/check.rs:204`
- Test: `accepts_subquery_with_inner_security_filter`: `crates/query-engine/src/check.rs:215`
- Test: `rejects_aggregate_subquery_without_inner_security_filter`: `crates/query-engine/src/check.rs:234`
- Test: `accepts_aggregate_subquery_with_inner_security_filter`: `crates/query-engine/src/check.rs:259`
- Test: `accepts_subquery_wrapping_non_sensitive_table`: `crates/query-engine/src/check.rs:288`

---

---

# TQ3: Allow-List Bypass — Security Task Results

## Task 1: Case variations are rejected or normalized (GL_ISSUE vs gl_issue)

**Status:** PASS

Entity names are matched **case-sensitively** at two independent layers. "user", "USER", "GL_ISSUE" are all rejected.

1. **JSON Schema enum constraint** — `derive_json_schema()` populates `$defs.EntityType.enum` with exact PascalCase names from the ontology (`User`, `Project`, `MergeRequest`, etc.). The `check_ontology()` step rejects any value not in the enum as `AllowlistRejected`.
2. **BTreeMap lookup in normalization** — `ontology.table_name(entity)` and `ontology.get_node(entity)` use `BTreeMap::get()`, which is case-sensitive. Even if schema validation were somehow bypassed, normalization would reject the entity.

No case normalization (`to_lowercase`, `eq_ignore_ascii_case`) exists in the entity resolution path.

### References

- `derive_json_schema()` populates EntityType enum from `node_names()`: `crates/ontology/src/json_schema.rs:32-38`
- `node_names()` returns BTreeMap keys (case-sensitive): `crates/ontology/src/lib.rs:374-376`
- `check_ontology()` applies derived schema: `crates/query-engine/src/validate.rs:119-132`
- `validated_input()` calls `check_ontology` before normalization: `crates/query-engine/src/lib.rs:77-84`
- `table_name()` case-sensitive BTreeMap lookup: `crates/ontology/src/lib.rs:562-567`
- `get_node()` case-sensitive lookup: `crates/ontology/src/lib.rs:273-275`
- Defense-in-depth rejection in normalization: `crates/query-engine/src/normalize.rs:82-86`
- Entity names in ontology schema (PascalCase): `config/ontology/schema.yaml:26-64`
- Test: `invalid_entity_type_rejected`: `crates/query-engine/src/lib.rs:573-587`
- Test: `edge_cases` with UnknownEntity: `crates/query-engine/src/normalize.rs:327-335`

---

## Task 2: Unicode homoglyphs are rejected in entity names

**Status:** PASS

Two independent defenses block homoglyphs:

1. **Identifier regex is ASCII-only** — The `jsonschema` crate (0.44.1) uses `fancy-regex` (0.17.0) which delegates to `regex-syntax`. In the Rust regex family, `[a-zA-Z]` is a byte-range class matching only ASCII letters (`U+0041`–`U+005A`, `U+0061`–`U+007A`). Unicode letter classes require explicit `\p{...}` syntax. A Cyrillic `а` (`U+0430`) would fail the Identifier pattern.
2. **Enum constraints on all security-sensitive fields** — Entity names, column names, relationship types, and filter property names are all validated against strict enums derived from the ontology. Even if a homoglyph passed the regex, it would fail the enum match.

The only field where the Identifier regex is the sole defense is `node.id` (SQL aliases like `"u"`, `"mr"`). Even there, `fancy-regex` treats `[a-zA-Z]` as ASCII-only.

No explicit ASCII checks or Unicode normalization exist in the codebase — the defense relies on the regex engine's semantics.

### References

- Identifier regex (`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`): `config/schemas/graph_query.schema.json:175-181`
- `jsonschema` 0.44.1 depends on `fancy-regex` 0.17.0: `Cargo.lock`
- EntityType enum derived from ontology: `crates/ontology/src/json_schema.rs:32-38`
- Per-entity column enum in derived schema: `crates/ontology/src/json_schema.rs:93-122`
- Per-entity filter propertyNames enum: `crates/ontology/src/json_schema.rs:93-122`
- Relationship type enum: `crates/ontology/src/json_schema.rs:40-50`
- Base EntityType definition (overwritten at runtime): `config/schemas/graph_query.schema.json:187-191`

---

## Task 3: Allowlist completeness — ontology JSON schema vs ClickHouse schema

**Status:** PASS

Every `gl_*` table in `graph.sql` has a corresponding ontology YAML entry. Non-`gl_*` infrastructure tables (`checkpoint`, `namespace_deletion_schedule`, `code_indexing_checkpoint`) have no ontology entry and are completely inaccessible through queries.

| ClickHouse Table | Ontology Node | Accessible? |
|---|---|---|
| `checkpoint` | None | No (no `gl_` prefix) |
| `namespace_deletion_schedule` | None | No |
| `code_indexing_checkpoint` | None | No |
| `gl_user` | User | Yes |
| `gl_group` | Group | Yes |
| `gl_project` | Project | Yes |
| `gl_note` | Note | Yes |
| `gl_merge_request` | MergeRequest | Yes |
| `gl_merge_request_diff` | MergeRequestDiff | Yes |
| `gl_merge_request_diff_file` | MergeRequestDiffFile | Yes |
| `gl_milestone` | Milestone | Yes |
| `gl_label` | Label | Yes |
| `gl_work_item` | WorkItem | Yes |
| `gl_edge` | Built-in edge table | Special |
| `gl_pipeline` | Pipeline | Yes |
| `gl_stage` | Stage | Yes |
| `gl_job` | Job | Yes |
| `gl_vulnerability` | Vulnerability | Yes |
| `gl_vulnerability_scanner` | VulnerabilityScanner | Yes |
| `gl_vulnerability_identifier` | VulnerabilityIdentifier | Yes |
| `gl_finding` | Finding | Yes |
| `gl_security_scan` | SecurityScan | Yes |
| `gl_vulnerability_occurrence` | VulnerabilityOccurrence | Yes |
| `gl_branch` | Branch | Yes |
| `gl_directory` | Directory | Yes |
| `gl_file` | File | Yes |
| `gl_definition` | Definition | Yes |
| `gl_imported_symbol` | ImportedSymbol | Yes |

Column mismatch fails closed: if an ontology YAML declares a field that doesn't exist in ClickHouse, the query fails at execution time. No automated cross-validation between ontology YAML and `graph.sql` DDL exists in CI.

Ontology loading enforces `table_prefix` on all `destination_table` values.

### References

- `derive_json_schema()` generates entity enums from `node_names()`: `crates/ontology/src/json_schema.rs:23-61`
- ClickHouse DDL: `config/graph.sql`
- `table_prefix` validation at load time: `crates/ontology/src/loading/mod.rs:125-131`
- Test: `destination_table_must_match_table_prefix`: `crates/ontology/src/lib.rs:1203-1275`
- Ontology node YAMLs: `config/ontology/nodes/`

---

## Task 4: Verify validate_field() rejects columns not defined for a given entity type

**Status:** PASS

Column validation enforced at two layers:

1. **Ontology-derived JSON Schema** — `build_node_selector_validation()` creates per-entity `if/then` blocks. When `entity == "User"`, only User's declared columns are valid in `columns` and `filters.propertyNames`. Unknown columns fail as `AllowlistRejected` during `check_ontology()`.
2. **Programmatic `validate_field()`** — Called for order_by and aggregation properties during `check_references()`. Does a case-sensitive linear scan of the entity's `fields` list plus `NODE_RESERVED_COLUMNS` (`["id"]`).

### References

- `validate_field()` implementation: `crates/ontology/src/lib.rs:500-522`
- `NODE_RESERVED_COLUMNS`: `crates/ontology/src/constants.rs:13`
- `validate_field()` called for aggregation property: `crates/query-engine/src/validate.rs:365-370`
- `validate_field()` called for order_by property: `crates/query-engine/src/validate.rs:416-420`
- `build_node_selector_validation()` — per-entity column/filter enums: `crates/ontology/src/json_schema.rs:93-122`
- `check_ontology()` applies derived schema: `crates/query-engine/src/validate.rs:119-132`
- Defense-in-depth in normalization: `crates/query-engine/src/normalize.rs:82-86`
- Test: `invalid_column_in_filter`: `crates/query-engine/src/lib.rs:536-547`
- Test: `invalid_column_in_order_by`: `crates/query-engine/src/lib.rs:514-523`
- Test: `invalid_column_in_aggregation`: `crates/query-engine/src/lib.rs:561-569`
- Test: `validate_field` unit tests: `crates/ontology/src/lib.rs:752-771`
- Test: aggregation property rejection: `crates/query-engine/src/validate.rs:698-710`

---

---

# TQ5: Resource Exhaustion — Security Task Results

## Task 1: Identify worst-case query patterns within limits

**Status:** PASS (caps are well-defined) with analysis of worst-case cost

### Hard caps (two-layer enforcement)

All limits enforced first by JSON Schema, then by Rust defense-in-depth in `check_depth()`:

| Limit | Value | JSON Schema | Rust Cap |
|---|---|---|---|
| Max nodes per query | 5 | `graph_query.schema.json:20` | `validate.rs:157` |
| Max relationships | 5 | `graph_query.schema.json:29` | `validate.rs:158` |
| Max hops per relationship | 3 | `graph_query.schema.json:408-411` | `validate.rs:155` |
| Max path depth | 3 | `graph_query.schema.json:528-531` | `validate.rs:156` |
| Max node_ids per node | 500 | `graph_query.schema.json:258-259` | `validate.rs:159` |
| Max IN filter values | 100 | `graph_query.schema.json:371` | `validate.rs:160` |
| Max limit | 1000 | `graph_query.schema.json:54` | — |
| Max pagination window | 1000 | — | `validate.rs:295-298` |
| Path CTE internal LIMIT | 1000 | — | `lower.rs:244` |

### UNION ALL unrolling analysis

`build_hop_union_all()` (`lower.rs:548-554`) produces one UNION ALL arm per hop depth. Each arm at depth `d` chains `d` edge table scans via INNER JOINs.

For one relationship with `max_hops=3`:
- Arm 1: 1 edge scan
- Arm 2: 2 edge scans joined
- Arm 3: 3 edge scans joined
- Total: **6 edge table scans, 3 UNION ALL arms**

### Worst-case traversal (5 nodes, 5 rels at max_hops=3, 500 IDs each)

- 5 node table scans (each with `IN (500 ids)` + `startsWith(traversal_path)`)
- 5 UNION ALL subqueries x 3 arms = **15 UNION ALL arms**
- 6 + 6 + 6 + 6 + 6 = **30 edge table scans** total
- ~15 JOINs at the outer level connecting nodes to edge unions
- Final LIMIT 1000

### Worst-case path finding (max_depth=3)

- Recursive CTE with base query + 2 recursive branches (forward + reverse)
- Each branch joins `paths` CTE to `gl_edge`
- Cycle detection: `NOT has(path_ids, next_node)`
- Hard LIMIT 1000 on CTE rows (`lower.rs:244`)
- Final outer JOIN to end node table

### References

- `check_depth()` with all caps: `crates/query-engine/src/validate.rs:151-216`
- `build_hop_union_all()`: `crates/query-engine/src/lower.rs:548-554`
- `build_hop_arm()` — chains d edge JOINs: `crates/query-engine/src/lower.rs:558`
- Test: `max_hops=3 should produce three union arms`: `crates/query-engine/src/lower.rs:1329-1336`
- Path CTE lowering with LIMIT 1000: `crates/query-engine/src/lower.rs:231-244`
- Path recursive branches: `crates/query-engine/src/lower.rs:308-422`
- JSON Schema limits: `config/schemas/graph_query.schema.json:20,29,54,258-259,371,408-411,528-531`
- Default limit (30): `crates/query-engine/src/input.rs:124-126`

---

## Task 2: Load test with maximum complexity queries

**Status:** PARTIAL — infrastructure exists but no max-complexity test suite

### What exists

The `xtask` crate has a query evaluation framework with concurrent execution:

- `execute_all_concurrent()` uses `buffer_unordered(concurrency)` for bounded-concurrency load testing
- Concurrency is configurable (1 = serial, >1 = concurrent)
- `SAFE_QUERY_SETTINGS` applies ClickHouse safety limits during evaluation: `max_memory_usage = 1GB`, `max_execution_time = 30s`, `max_bytes_before_external_group_by = 100MB`
- `datalake-generator` crate generates synthetic GitLab data for testing

### What is missing

- No dedicated max-complexity query fixtures (5 nodes x 5 rels x 3 hops x 500 IDs)
- No path-finding stress test with dense graphs at max_depth=3
- No external load testing tools (k6, locust, gatling)
- Criterion declared as workspace dependency but no benchmark files exist under `benches/`
- `SAFE_QUERY_SETTINGS` only used in xtask, **not in production server**

### K8s resource limits (blunt backstop)

Webserver pod: 500m CPU / 512Mi memory. This is the only production-level resource cap.

### References

- `execute_all_concurrent()`: `crates/xtask/src/synth/evaluation/executor.rs:691-703`
- `SAFE_QUERY_SETTINGS`: `crates/xtask/src/synth/evaluation/executor.rs:21-26`
- Concurrency config: `crates/xtask/src/synth/config.rs:607-608`
- Serial back-off on memory errors: `crates/xtask/src/synth/evaluation/executor.rs:672-678`
- Datalake generator: `crates/datalake-generator/src/lib.rs:28-92`
- Query fixtures: `fixtures/queries/sdlc_queries.yaml`
- Criterion dependency (unused): `Cargo.toml:71`
- Helm webserver limits: `helm-dev/gkg/values.yaml:169-175`

---

## Task 3: Verify rate limiting under sustained load

**Status:** FAIL — rate limiting is not implemented

No application-level rate limiting exists. The metric `qe.threat.rate_limited` is defined (`metrics.rs:34`) but **no code path increments it**. The description says "exported for the server layer to increment" but the server layer does not.

Specific gaps:

1. **No HTTP rate limiting middleware** — Axum router (`webserver/router.rs`) has metrics and tracing layers only, no rate limiter.
2. **No gRPC rate limiting interceptor** — Tonic server (`grpc/server.rs`) has no interceptors.
3. **No query concurrency gate** — `run_query()` (`service.rs:35-79`) has no semaphore. Every request goes straight through.
4. **No ClickHouse-side limits** — No `max_concurrent_queries` or `max_memory_usage` sent to ClickHouse from the production query path.
5. **Indexer has concurrency controls** (`worker_pool.rs:56-87`) via semaphores, but these are for NATS message processing, not query serving.

### References

- `rate_limited` metric defined but never incremented: `crates/query-engine/src/metrics.rs:16,34,68-70`
- HTTP router (no rate limiter): `crates/gkg-server/src/webserver/router.rs:103-117`
- gRPC server (no interceptors): `crates/gkg-server/src/grpc/server.rs:44-51`
- `run_query()` no concurrency gate: `crates/gkg-server/src/query_pipeline/service.rs:35-79`
- Indexer semaphores (not query-relevant): `crates/indexer/src/worker_pool.rs:56-87`

---

## Task 4: Test timeout enforcement doesn't leave orphan queries in ClickHouse

**Status:** FAIL — no timeout enforcement exists, orphan queries are possible

### No query timeout at any layer

1. **ClickHouse client has no timeout config** — `ArrowClickHouseClient::new()` sets URL, database, username, password. No `.with_option("max_execution_time", ...)`. No connect or request timeout.
2. **No `tokio::time::timeout` wrapper** — `ExecutionStage::execute()` calls `query.fetch_arrow().await` with no timeout. Can block indefinitely.
3. **No overall pipeline timeout** — `run_query()` chains 8 stages sequentially with no deadline.
4. **`timeout` metric defined but never fired** — `qe.threat.timeout` exists (`metrics.rs:33`) but nothing increments it.

### Orphan query risk

The gRPC handler at `grpc/service.rs:119` spawns a **detached `tokio::spawn`** for the query pipeline. When a client disconnects:

1. The spawned task continues running independently
2. `tx.send()` will fail (receiver dropped), but the in-flight ClickHouse HTTP request is not cancelled
3. `fetch_arrow()` buffers the entire response in a loop — it will read until ClickHouse finishes sending all data
4. No `CancellationToken` or `select!` on client disconnect exists in the spawned task

The graceful shutdown handler (`shutdown.rs:1-22`) only applies to the indexer's `CancellationToken`, not the webserver query pipeline.

### References

- ClickHouse client — no timeout fields: `crates/clickhouse-client/src/configuration.rs:6-13`
- `ArrowClickHouseClient::new()` — no timeout options: `crates/clickhouse-client/src/arrow_client.rs:28-41`
- `ExecutionStage::execute()` — no timeout wrapping: `crates/gkg-server/src/query_pipeline/stages/execution.rs:19-47`
- `run_query()` — no pipeline deadline: `crates/gkg-server/src/query_pipeline/service.rs:35-79`
- Detached `tokio::spawn` in gRPC handler: `crates/gkg-server/src/grpc/service.rs:119`
- `fetch_arrow()` buffering loop: `crates/clickhouse-client/src/arrow_client.rs:206-232`
- `timeout` metric defined, never incremented: `crates/query-engine/src/metrics.rs:15,33`
- Shutdown handler (indexer only): `crates/gkg-server/src/shutdown.rs:1-22`
- `SAFE_QUERY_SETTINGS` exists but only in xtask: `crates/xtask/src/synth/evaluation/executor.rs:21-26`

---

## Task 5: Hydration fan-out bounded by MAX_DYNAMIC_HYDRATION_RESULTS

**Status:** PASS with caveat

`HydrationPlan::Dynamic` is used for PathFinding and Neighbors queries. The hydration stage extracts unique `(entity_type, id)` pairs from the base query's result rows, groups them by entity type, then fires one `compile() + fetch_arrow()` per entity type in parallel via `try_join_all`.

### Fan-out bounds

1. **Number of parallel queries** — One per distinct entity type in the result set. Bounded by the ontology node count (~20 types). In practice, path finding typically involves 2-3 entity types.

2. **`limit` field** — `build_dynamic_search_query` sets `"limit": ids.len().min(MAX_DYNAMIC_HYDRATION_RESULTS)` where `MAX_DYNAMIC_HYDRATION_RESULTS = 1000`. This caps the ClickHouse result set.

3. **`node_ids` field** — The full deduped ID list is passed as `node_ids` in the JSON. This JSON then goes through `compile()`, which calls `check_depth()` enforcing `MAX_NODE_IDS = 500`. If >500 unique IDs exist for one entity type, the hydration query **fails** with `LimitExceeded`.

4. **Static hydration** (`HydrationTemplate::with_ids`) — Same path: `compile_and_fetch()` calls `compile()`, so `MAX_NODE_IDS` applies equally.

### Potential issue: >500 deduped IDs per entity type

For path finding with base LIMIT 1000 and max_depth=3, each row can contain up to 4 nodes (depth+1). That's up to 4000 node references across all rows. After dedup per entity type, a dense graph could yield >500 unique IDs for a single entity type. This would cause the hydration `compile()` call to return `LimitExceeded`, failing the entire query rather than degrading gracefully (e.g., by truncating to 500).

### References

- `HydrationPlan::Dynamic` assigned to PathFinding/Neighbors: `crates/query-engine/src/lib.rs:125`
- `hydrate_dynamic()` — `try_join_all` over entity types: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:60-79`
- `extract_dynamic_refs()` — dedup IDs per entity type: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:135-152`
- `build_dynamic_search_query()` — limit capped, node_ids passed raw: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:169-206`
- `MAX_DYNAMIC_HYDRATION_RESULTS = 1000`: `crates/query-engine/src/constants.rs:52`
- `compile_and_fetch()` — passes through `compile()`: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:82-100`
- `check_depth()` enforces `MAX_NODE_IDS = 500` on hydration queries: `crates/query-engine/src/validate.rs:159,191-196`
- `hydrate_static()` — same `compile_and_fetch` path: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:30-57`
- `HydrationTemplate::with_ids()` — injects IDs into template JSON: `crates/query-engine/src/codegen.rs:59-64`
- Dynamic node extraction for path finding: `crates/gkg-server/src/redaction/query_result.rs:190-196`
- Dynamic node extraction for neighbors: `crates/gkg-server/src/redaction/query_result.rs:197-204`

---

# TQ6: Information Leakage — Security Task Results

## Task 1: Audit all error paths for schema/data/auth state leakage

**Status:** FAIL — two high-severity leakage paths

### Error flow architecture

All query errors flow through `send_query_error()` which sends `PipelineError.to_string()` **verbatim** as the `ExecuteQueryError.message` field in the gRPC response protobuf. There is no sanitization layer.

| PipelineError Variant | gRPC Code | Client Sees Verbatim? | Risk |
|---|---|---|---|
| `Security(SecurityError)` | `security_error` | YES | **LOW** — server-authored messages |
| `Compile(String)` | `compilation_error` | YES | **MEDIUM** — JSON Schema errors enumerate valid values |
| `Execution(String)` | `execution_error` | YES | **HIGH** — raw ClickHouse errors |
| `Authorization(RedactionExchangeError)` | varies | Mostly sanitized | **LOW** |
| `Streaming(String)` | `streaming_error` | YES | **LOW** — server-authored |

### Additional leakage: `raw_query_strings`

The compiled ClickHouse SQL (with parameterized placeholders) is sent to the client in every successful response via `QueryMetadata.raw_query_strings`. This exposes table names, column names, join structure, and security filter patterns (`startsWith(traversal_path, ...)`). The proto field is documented as "compiled ClickHouse SQL(s) for debugging".

### Positive findings

- JWT auth errors sanitized: `auth.rs:15` discards inner error with `|_|`
- `RedactionExchangeError` messages are static except `ClientError` (which passes client-provided values back)

### References

- `PipelineError` definition: `crates/gkg-server/src/query_pipeline/error.rs:8-24`
- `PipelineError::into_status()` — verbatim propagation: `crates/gkg-server/src/query_pipeline/error.rs:37-45`
- `send_query_error()` — sends `error.to_string()` to client: `crates/gkg-server/src/query_pipeline/helpers.rs:52-65`
- Called from gRPC handler: `crates/gkg-server/src/grpc/service.rs:166`
- `QueryError` → `PipelineError::Compile` conversion: `crates/gkg-server/src/query_pipeline/stages/compilation.rs:33`
- `QueryError` definition (10 variants): `crates/query-engine/src/error.rs:10-44`
- `raw_query_strings` set from compiled SQL: `crates/gkg-server/src/query_pipeline/stages/formatting.rs:38`
- `raw_query_strings` sent in protobuf: `crates/gkg-server/src/grpc/service.rs:153`
- Proto definition: `crates/gkg-server/proto/gkg.proto:80`
- JWT error sanitization: `crates/gkg-server/src/grpc/auth.rs:15`
- `RedactionExchangeError::into_status()`: `crates/gkg-server/src/redaction/stream.rs:24-41`

---

## Task 2: Verify auth failures don't reveal data existence

**Status:** PASS

Unauthorized rows are silently dropped. The client receives empty or reduced result sets, never an "access denied" error for specific rows.

1. **Redaction stage** — `apply_authorizations()` marks unauthorized rows with `set_unauthorized()`. Only `authorized_rows()` are included in formatting output.
2. **Fail-closed** — Missing authorizations result in all rows being redacted, not all rows being returned. NULL auth IDs → row denied.
3. **Zero traversal paths** — When `SecurityContext.traversal_paths` is empty, `build_path_filter` injects `WHERE false`, returning zero rows from ClickHouse. Client sees empty results.
4. **`redacted_count` stays server-side** — Tracked in `PipelineOutput` and emitted as a metric (`qp.redacted_count`) but NOT included in the protobuf `QueryMetadata` sent to the client. Only post-redaction `row_count` is sent.

### References

- `apply_authorizations()` marks rows: `crates/gkg-server/src/redaction/query_result.rs:318-359`
- `authorized_rows()` filter: `crates/gkg-server/src/redaction/query_result.rs:361-362`
- Fail-closed on NULL IDs: `crates/gkg-server/src/redaction/query_result.rs:331`
- Zero paths inject `WHERE false`: `crates/query-engine/src/security.rs:110`
- `redacted_count` not in protobuf: `crates/gkg-server/src/query_pipeline/types.rs:84` (server-side only)
- Test: `search_no_authorization_returns_empty`: `crates/integration-tests/tests/server/graph_formatter.rs:308`
- Test: `no_authorizations_redacts_all`: `crates/gkg-server/src/query_pipeline/stages/redaction.rs:97-103`
- Test: `traversal_redaction_removes_unauthorized_paths`: `crates/integration-tests/tests/server/graph_formatter.rs:412`

---

## Task 3: Verify ClickHouse SQL errors not propagated verbatim to clients

**Status:** FAIL — raw ClickHouse errors reach the client

`PipelineError::Execution` is produced by calling `.to_string()` on ClickHouse client errors. This string is sent to the client without sanitization via two paths:

1. **Primary path** — `send_query_error()` at `helpers.rs:61` sends `error.to_string()` as `ExecuteQueryError.message`
2. **Fallback path** — `into_status()` at `error.rs:41` sends `Status::internal(msg)`

A typical ClickHouse error could contain: table names (`gkg_graph.gl_project`), column names, SQL fragments from the generated query, ClickHouse server version, database topology info.

The error is already logged server-side at `helpers.rs:56` (`error!(error = %error, "Pipeline error")`), so sanitizing the client message would lose no observability.

Same issue exists for hydration execution errors (`hydration.rs:97`).

### References

- Execution error from base query: `crates/gkg-server/src/query_pipeline/stages/execution.rs:39`
- Execution error from hydration: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:97`
- `PipelineError::Execution` Display: `crates/gkg-server/src/query_pipeline/error.rs:17`
- `send_query_error()` sends verbatim: `crates/gkg-server/src/query_pipeline/helpers.rs:52-65`
- `into_status()` sends verbatim: `crates/gkg-server/src/query_pipeline/error.rs:41`
- Error already logged server-side: `crates/gkg-server/src/query_pipeline/helpers.rs:56`

---

## Task 4: Audit AllowlistRejected messages for valid value leakage

**Status:** MEDIUM — jsonschema errors enumerate valid values, but schema is already public

The `jsonschema` crate's `ValidationError` Display format lists all valid enum values when an enum constraint fails. For example, sending `"entity": "Foobar"` produces a message like:

> `"Foobar" is not one of ["User","Project","MergeRequest","Group",...]`

This enumerates the complete set of valid entity types, relationship types, and per-entity column names.

The error flows: `jsonschema` error → `collect_schema_errors()` formats with `format!("{} at {}", e, e.instance_path())` → `QueryError::Validation` → promoted to `QueryError::AllowlistRejected` → `PipelineError::Compile` → client verbatim.

**Mitigating factor:** The graph schema is already public via the `GetGraphSchema` gRPC endpoint (`service.rs:181-217`), which returns all entity types, edge types, and properties to any authenticated user. So this is not leaking information that is otherwise secret.

**Structural risk:** If "hidden" ontology entities were ever added (internal-only types), this error path would leak their existence.

Other `AllowlistRejected` sources do not enumerate valid values — they only confirm which entity type was queried:
- `validate_field()`: `"field \"foo\" does not exist on node type \"User\""`
- `normalize.rs`: `"entity 'Foo' passed schema validation but has no table mapping"`

### References

- `collect_schema_errors()` formats jsonschema errors verbatim: `crates/query-engine/src/validate.rs:28-42`
- `check_ontology()` promotes to `AllowlistRejected`: `crates/query-engine/src/validate.rs:128-129`
- `derive_json_schema()` inserts entity enum: `crates/ontology/src/json_schema.rs:32-38`
- Relationship type enum: `crates/ontology/src/json_schema.rs:40-50`
- Per-entity column enum: `crates/ontology/src/json_schema.rs:93-122`
- `validate_field()` error (no enumeration): `crates/ontology/src/lib.rs:519-521`
- Aggregation property rejection: `crates/query-engine/src/validate.rs:365-378`
- Order-by property rejection: `crates/query-engine/src/validate.rs:416-420`
- Normalization rejection: `crates/query-engine/src/normalize.rs:74-78,82-86`
- `GetGraphSchema` already exposes schema publicly: `crates/gkg-server/src/grpc/service.rs:181-217`

---

---

# TQ7: Aggregation Authorization — Security Task Results

Aggregation auth uses a reduced security model. Layer 1 (org_id) and Layer 2 (traversal_path) apply fully. Layer 3 (per-row redaction) applies only to GROUP BY nodes, not to aggregated target nodes. This is a documented, intentional trade-off.

| Layer | Applied? | Scope |
|---|---|---|
| Layer 1 (organization_id) | YES | All queries |
| Layer 2 (traversal_path) | YES | All `gl_*` tables in query |
| Layer 3 (per-row redaction) | PARTIAL | Only GROUP BY nodes; aggregated target values computed pre-redaction |
| Hydration | NO | `HydrationPlan::None` for aggregation |

## Task 1: Document aggregation authorization limitations

**Status:** PASS — documented, with one doc inconsistency

The security design document (`security.md:356-363`) explicitly acknowledges aggregation limitations:

> Aggregation queries rely on Layers 1 and 2... to ensure users can only aggregate over data they have group-level access to. We do not perform post-aggregation filtering, as this would be ineffective.

`enforce_return()` only adds `_gkg_{alias}_id`/`_gkg_{alias}_type` columns for GROUP BY nodes. The aggregated target node gets no redaction columns, so Layer 3 cannot check it.

**Doc inconsistency:** `security.md:247` says "GKG redaction module must be called for all non-aggregation queries" — but the code DOES run redaction for aggregation queries (it just only checks GROUP BY nodes). The doc should say "Layer 3 redaction has limited scope for aggregation queries."

### References

- `HydrationPlan::None` for aggregation: `crates/query-engine/src/lib.rs:124`
- Aggregation lowering: `crates/query-engine/src/lower.rs:138-200`
- `agg_expr()` — function mapping: `crates/query-engine/src/lower.rs:202-209`
- `enforce_return()` — only group_by nodes get redaction columns: `crates/query-engine/src/enforce.rs:133-138`
- `enforce_return()` adds redaction ID to GROUP BY clause: `crates/query-engine/src/enforce.rs:228-233`
- Test: `aggregation_only_adds_columns_for_group_by_nodes`: `crates/query-engine/src/enforce.rs:467-543`
- Test: `aggregation_adds_redaction_id_to_group_by`: `crates/query-engine/src/enforce.rs:546-599`
- Security doc acknowledging limitation: `docs/design-documents/security.md:356-363`
- Doc inconsistency: `docs/design-documents/security.md:247`
- Pipeline runs all stages including redaction: `crates/gkg-server/src/query_pipeline/service.rs:58-75`

---

## Task 2: Test aggregations respect traversal_path filtering

**Status:** PASS

`apply_security_context()` has NO query-type branching — it applies `startsWith(traversal_path, ...)` predicates identically for aggregation and all other query types. `check_ast()` then verifies every `gl_*` alias has a valid predicate.

### References

- `apply_security_context()` — no query-type check: `crates/query-engine/src/security.rs:73-106`
- `check_ast()` verification: `crates/query-engine/src/check.rs:19-66`
- Compile-through test: `aggregation_query()`: `crates/query-engine/src/lib.rs:275-287`
- Aggregate subquery rejection test: `rejects_aggregate_subquery_without_inner_security_filter`: `crates/query-engine/src/check.rs:234`
- Aggregate subquery acceptance test: `accepts_aggregate_subquery_with_inner_security_filter`: `crates/query-engine/src/check.rs:259`
- Integration test: `aggregation_redaction` (full pipeline with MockRedactionService): `crates/integration-tests/tests/server/graph_formatter.rs:590-626`
- Integration test: `aggregation_count_returns_correct_values`: `crates/integration-tests/tests/server/data_correctness.rs:555-611`

---

## Task 3: Assess whether aggregate values reveal sensitive patterns

**Status:** KNOWN RISK — documented as acceptable trade-off

Aggregate values (COUNT, SUM, AVG, MIN, MAX) are computed at the DB level over all rows matching the traversal_path filter. This means:

- **COUNT** may include items that would be individually redacted (e.g., confidential issues)
- **MIN/MAX on timestamps** could reveal timing of the earliest/latest entity creation
- **GROUP BY with COUNT=1** effectively identifies individual entities

**Mitigating controls:**

1. **HAVING is NOT user-accessible** — no `having` field in `Input` struct or JSON schema. Only exists in the internal AST.
2. **Collect (groupArray) is BLOCKED** — `validate.rs:334-338` explicitly rejects it. This prevents collecting per-item values into arrays.
3. GROUP BY must reference a declared node alias with ontology-validated columns — no arbitrary expressions.
4. GROUP BY nodes themselves ARE subject to Layer 3 redaction.

### References

- `AggFunction` enum (Count, Sum, Avg, Min, Max, Collect): `crates/query-engine/src/input.rs:378-398`
- `Collect` blocked at validation: `crates/query-engine/src/validate.rs:334-338`
- HAVING not in Input struct: `crates/query-engine/src/input.rs` (no `having` field)
- HAVING not in JSON schema: `config/schemas/graph_query.schema.json` (no `having` property)
- GROUP BY column validation: `crates/query-engine/src/lower.rs:154-165`
- Security doc acknowledging trade-off: `docs/design-documents/security.md:356-363`

---

## Task 4: Test groupArray (Collect) does not leak per-item data

**Status:** PASS — Collect is blocked at validation

The `Collect` variant maps to ClickHouse `groupArray()`, which would collect individual values into an array — leaking per-item data that should be redacted by Layer 3. However, `check_references()` explicitly rejects it:

```rust
if agg.function == AggFunction::Collect {
    return Err(QueryError::Validation("...\"collect\" is not supported"));
}
```

The enum variant exists (possibly for future use) but is a dead code path for the query API. No query using `"function": "collect"` can reach lowering, security injection, or execution.

### References

- `Collect` → `groupArray` mapping: `crates/query-engine/src/input.rs:395`
- Validation rejection: `crates/query-engine/src/validate.rs:334-338`
- `agg_expr()` would generate `groupArray(target.property)`: `crates/query-engine/src/lower.rs:202-209`

---

## Task 5: Test aggregation with partial namespace access

**Status:** PASS — hierarchical model means no exclusions by design

The access model is strictly hierarchical: `startsWith(traversal_path, '42/43/')` matches the group AND all subgroups/projects. There is no exclusion mechanism. This is an explicit design decision documented in `security.md:18-20`:

> The first iteration does not support individual project-level access or item-level permissions.

For multi-path contexts (e.g., `["1/2/4/", "1/2/5/"]`), `build_path_filter` generates an optimized LCP + OR filter. This correctly handles disjoint subtree access without over-including.

No explicit tests combine multi-path security contexts with aggregation queries, but since `apply_security_context` makes no query-type distinction, the generic multi-path tests (`multiple_paths_uses_prefix_and_or_starts_with`) cover the aggregation case transitively.

### References

- `build_path_filter()` — single and multi-path handling: `crates/query-engine/src/security.rs:108-121`
- Zero paths → `WHERE false`: `crates/query-engine/src/security.rs:110`
- Multi-path test: `multiple_paths_uses_prefix_and_or_starts_with`: `crates/query-engine/src/security.rs:255-259`
- LCP test: `accepts_lowest_common_prefix` (check.rs): `crates/query-engine/src/check.rs:157`
- Hierarchical model documented: `docs/design-documents/security.md:18-20`

---

---

# TQ8: Security Filter Exclusion — Security Task Results

## Task 1: Verify SKIP list contains only genuinely cross-tenant entities

**Status:** PASS

`SKIP_SECURITY_FILTER_TABLES` contains exactly one entry: `gl_user`. Exhaustive review confirms:

1. **`gl_user` lacks `traversal_path`** — the DDL has no such column. Applying `startsWith(traversal_path, ...)` would generate invalid SQL. The skip is functionally necessary.
2. **`gl_user` is the only `gl_*` table without `traversal_path`** — all 24 other `gl_*` tables (including `gl_edge`) have the column.
3. **User is the only `scope: global` entity** — all other entities have `scope: namespaced` in their ETL config.
4. **User auth via redaction service** — `redaction.ability: read_user` delegates to Rails. Users are post-filtered via Layer 3, not Layer 2.

### References

- Skip list definition: `crates/query-engine/src/constants.rs:27-30`
- `gl_user` DDL (no traversal_path): `config/graph.sql:27-50`
- User ontology `scope: global`: `config/ontology/nodes/core/user.yaml:153-156`
- User redaction config: `config/ontology/nodes/core/user.yaml:15-18`
- `should_apply_security_filter()`: `crates/query-engine/src/security.rs:192-203`
- Test: `should_apply_security_filter_skips_user`: `crates/query-engine/src/security.rs:329-333`

---

## Task 2: Derive skip list from ontology has_traversal_path metadata

**Status:** IMPROVEMENT OPPORTUNITY — ontology already has the metadata

The ontology crate already tracks this via `NodeEntity.has_traversal_path: bool`, auto-derived from YAML properties during loading. The indexer already uses it for namespace deletion. The query engine does not.

**Current state:**
- `has_traversal_path` field: `crates/ontology/src/entities.rs:97-99`
- Set during loading from YAML fields: `crates/ontology/src/loading/node.rs:157-159`
- Already consumed by indexer: `crates/indexer/src/modules/namespace_deletion/lower.rs:26-27`
- NOT consumed by `should_apply_security_filter()` — uses hardcoded constant instead

**Refactor path:** `should_apply_security_filter()` could take `&Ontology`, look up the entity by `destination_table`, and check `node.has_traversal_path`. This would eliminate the hardcoded skip list entirely. Minor plumbing needed: `collect_node_aliases` and `apply_security_to_from` would need `&Ontology` threaded through. The ontology is already available in `compile()` (`lib.rs:91`).

**Benefit:** Self-maintaining. If a new global entity is added (e.g., hypothetical `gl_organization`), it would automatically be skipped if it lacks `traversal_path` in its YAML — no manual constant update required.

### References

- `has_traversal_path` field on `NodeEntity`: `crates/ontology/src/entities.rs:97-99`
- Auto-derived during loading: `crates/ontology/src/loading/node.rs:157-159`
- Set in test builder: `crates/ontology/src/lib.rs:183-184`
- Consumed by indexer namespace deletion: `crates/indexer/src/modules/namespace_deletion/lower.rs:26-27`
- `compile()` already receives `&Ontology`: `crates/query-engine/src/lib.rs:91-93`
- `should_apply_security_filter()` hardcoded, no ontology ref: `crates/query-engine/src/security.rs:192-203`

---

## Task 3: Test joins between skipped and non-skipped tables don't leak

**Status:** PASS — defense in depth prevents cross-tenant leakage

For a User→MergeRequest traversal (`gl_user AS u JOIN gl_edge AS e0 JOIN gl_merge_request AS mr`):

- `gl_user` (`u`) — **NO filter** (skipped). Users are unfiltered at the SQL level.
- `gl_edge` (`e0`) — **HAS filter** (`startsWith(e0.traversal_path, ...)`). Cross-tenant edges are excluded.
- `gl_merge_request` (`mr`) — **HAS filter** (`startsWith(mr.traversal_path, ...)`). Cross-tenant MRs are excluded.

Even though User is unfiltered, both the edge and the target table are independently scoped to the caller's namespace. A cross-tenant edge or target row cannot survive both filters. Additionally, Layer 3 (redaction service) post-filters all returned entities.

`check_ast()` verifies every non-skipped `gl_*` alias has a valid predicate, providing defense-in-depth.

**Gap:** No dedicated end-to-end test proves the specific User→MR cross-tenant join scenario. Unit tests cover the pieces individually.

### References

- `collect_node_aliases()` skips `gl_user`, collects `gl_edge` and `gl_merge_request`: `crates/query-engine/src/security.rs:153-168`
- Test: `inject_skips_user_table` — User skipped, MR gets filter: `crates/query-engine/src/security.rs:314-326`
- Test: `inject_includes_edge_table` — both project and edge get filters: `crates/query-engine/src/security.rs:300-311`
- Test: `inject_filters_edge_table` — edge table alone gets filter: `crates/query-engine/src/security.rs:285-298`
- `check_ast()` post-compilation verification: `crates/query-engine/src/check.rs:19-28`
- Join building in `build_joins()`: `crates/query-engine/src/lower.rs:653-775`

---

## Task 4: CI check for SKIP_SECURITY_FILTER_TABLES changes

**Status:** FAIL — no automated gate exists

No CI job, CODEOWNERS rule, or automated check flags changes to `SKIP_SECURITY_FILTER_TABLES`. Reviewed all CI jobs in `.gitlab-ci.yml`:

- `agent-file-sync-check` — only checks AGENTS.md/CLAUDE.md
- `lint-check` — Clippy won't flag const array changes
- `unit-test` — runs `should_apply_security_filter_skips_user` but it only asserts specific table results, NOT the exhaustive skip list contents
- `ai:security` — manual trigger, not a gate
- No CODEOWNERS file exists

Adding a new table to the skip list would pass all existing CI checks. The only defense is human MR review.

**Recommended mitigations (any combination):**

1. **Exhaustive assertion test** — `assert_eq!(SKIP_SECURITY_FILTER_TABLES, &["gl_user"])` that fails if the list changes
2. **CODEOWNERS rule** — require security team approval for `crates/query-engine/src/constants.rs` and `crates/query-engine/src/security.rs`
3. **Ontology-derived skip** — replace the hardcoded list with `has_traversal_path` check (Task 2), making manual additions impossible

### References

- CI config: `.gitlab-ci.yml:1-565`
- No CODEOWNERS file in repo
- Existing unit test (non-exhaustive): `crates/query-engine/src/security.rs:329-333`
- `ai:security` manual job: `.gitlab-ci.yml:532-548`

---

## Summary

### TQ1: SQL Injection

| Task | Verdict | Open Items |
|------|---------|------------|
| Fuzz JSON inputs | **PASS** | None |
| String concat audit | **PASS** | Dormant SET code at `codegen.rs:124` — recommend removing or gating. LIKE escape gap at `lower.rs:987-989`. |
| Integer parsing | **PASS** | None |
| emit_literal() placeholders | **PASS** | None |

### TQ2: Auth Filter Bypass

| Task | Verdict | Open Items |
|------|---------|------------|
| Every SQL has traversal_path | **PASS** | No property-based tests for arbitrary query shapes |
| Recursive CTEs filtered | **PASS** | None |
| UNION branches filtered | **PASS** | None |
| Predicate stripping tests | **PASS** | None |
| SKIP_SECURITY_FILTER_TABLES review | **PASS** | No CODEOWNERS gate — adding entries requires no mandatory security review |
| Subquery auth filters | **PASS** | None |

### TQ3: Allow-List Bypass

| Task | Verdict | Open Items |
|------|---------|------------|
| Case variation rejection | **PASS** | None |
| Unicode homoglyph rejection | **PASS** | Defense relies on regex engine semantics, no explicit ASCII assertion |
| Allowlist completeness | **PASS** | No automated ontology-vs-DDL cross-validation in CI |
| Undefined column rejection | **PASS** | None |

### TQ5: Resource Exhaustion

| Task | Verdict | Open Items |
|------|---------|------------|
| Worst-case query patterns | **PASS** | Caps well-defined. Max traversal: ~30 edge scans, 15 JOINs, LIMIT 1000. |
| Load testing | **PARTIAL** | xtask eval framework exists; no max-complexity fixtures; no external tooling |
| Rate limiting | **FAIL** | Not implemented. Metric defined but unincremented. No concurrency gate. |
| Timeout / orphan queries | **FAIL** | No timeout at any layer. Detached `tokio::spawn` + unbounded `fetch_arrow` = orphan queries on disconnect. |
| Hydration fan-out | **PASS** with caveat | `limit` capped to `min(ids.len(), 1000)`. Queries go through `compile()` which enforces `MAX_NODE_IDS=500`. Fan-out bounded by ontology entity count (~20). But >500 deduped IDs per entity type will cause hydration to **fail**, not degrade gracefully. |

### TQ6: Information Leakage

| Task | Verdict | Open Items |
|------|---------|------------|
| All error paths audited | **FAIL** | Raw ClickHouse errors propagated verbatim. Compiled SQL sent in `raw_query_strings`. |
| Auth failures don't reveal existence | **PASS** | Unauthorized rows silently dropped. Fails closed. `redacted_count` stays server-side. |
| ClickHouse errors not verbatim | **FAIL** | `PipelineError::Execution` sends raw ClickHouse error to client (table names, SQL fragments, DB info). |
| AllowlistRejected value leakage | **MEDIUM** | `jsonschema` enum errors enumerate all valid entity types, relationship types, and column names. Low severity today (schema already public via `GetGraphSchema`), but structural vulnerability. |

### TQ7: Aggregation Authorization

| Task | Verdict | Open Items |
|------|---------|------------|
| Limitations documented | **PASS** | Minor doc inconsistency at `security.md:247` |
| Traversal_path filtering | **PASS** | `apply_security_context` is query-type agnostic |
| Aggregate value patterns | **KNOWN RISK** | COUNT/MIN/MAX computed pre-redaction. Documented as acceptable for Reporter+ access. |
| Collect (groupArray) leakage | **PASS** | Blocked at validation (`validate.rs:334-338`). Dead code path. |
| Partial namespace access | **PASS** | Hierarchical model — no exclusions by design. Multi-path OR filter is correct. |

### TQ8: Security Filter Exclusion

| Task | Verdict | Open Items |
|------|---------|------------|
| Skip list correctness | **PASS** | Only `gl_user`, the sole table without `traversal_path`. Verified exhaustively. |
| Ontology-derived skip list | **IMPROVEMENT** | `has_traversal_path` field already exists on `NodeEntity`. Refactor would eliminate hardcoded constant. |
| Cross-tenant join safety | **PASS** | Edge + target tables independently filtered. No end-to-end test for User→MR specifically. |
| CI gate for skip list changes | **FAIL** | No CODEOWNERS, no exhaustive assertion, no file-change trigger. Human review only. |

---

# TQ9: LIKE Enumeration — Security Task Results

## Task 1: LIKE + COUNT enumeration risk assessment

**Status:** FAIL — all string columns exposed to character-by-character probing via LIKE + COUNT

Three LIKE operators are available: `Contains` (`LIKE '%value%'`), `StartsWith` (`LIKE 'value%'`), `EndsWith` (`LIKE '%value'`). They are defined in `FilterOp` (`input.rs:245-257`) and lowered in `filter_expr()` (`lower.rs:923-925`) via the `like_pattern()` helper (`lower.rs:931-934`):

```rust
fn like_pattern(col: Expr, filter: &InputFilter, prefix: &str, suffix: &str) -> Expr {
    let s = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
    Expr::binary(Op::Like, col, Expr::string(format!("{prefix}{s}{suffix}")))
}
```

LIKE values are parameterized via `Expr::string()` → `{pN:String}` in codegen — no SQL injection risk. The enumeration risk is the threat.

**Which columns accept LIKE:** All 112 `type: string` columns across 24 entity YAML files. The JSON Schema (`graph_query.schema.json:343-380`) defines filters as `propertyNames: { $ref: Identifier }` with no column-level operator restriction. Validation in `check_filter_types()` (`validate.rs:226-284`) only checks value type compatibility — the `_ =>` catch-all at line 275 handles Contains/StartsWith/EndsWith identically to scalar ops.

**LIKE + COUNT composability:** Fully supported. `lower_aggregation()` (`lower.rs:123-185`) calls `build_full_where()` which processes node filters. A query like:

```json
{
  "query_type": "aggregation",
  "nodes": [{"id": "wi", "entity": "WorkItem", "columns": ["title"],
             "filters": {"title": {"op": "contains", "value": "secret"}}}],
  "aggregations": [{"function": "count", "target": "wi", "alias": "c"}],
  "limit": 1
}
```

Generates: `SELECT COUNT(wi.id) AS c FROM gl_work_item AS wi WHERE (wi.title LIKE {p0:String}) AND startsWith(wi.traversal_path, ...) LIMIT 1` — returning a count that reveals whether any matching rows exist without exposing actual values.

**Sensitive columns exposed (15+ HIGH/CRITICAL):**

| Entity | Column | Sensitivity |
|--------|--------|-------------|
| Vulnerability | title, description | CRITICAL |
| Finding | name, description, solution | CRITICAL |
| VulnerabilityOccurrence | name, description, solution, cve, location | CRITICAL/HIGH |
| VulnerabilityIdentifier | name, external_id | HIGH |
| User | email, name, first_name, last_name | HIGH (PII) |
| MergeRequest | title, description | HIGH |
| WorkItem | title, description | HIGH |
| Note | note | HIGH |

**Attack scenario:** An authenticated user can enumerate the contents of sensitive string columns within their authorized namespace by sending rapid `COUNT + LIKE '%probe%'` queries and observing zero vs non-zero counts. Character-by-character probing: `LIKE '%s%'` → `LIKE '%se%'` → `LIKE '%sec%'` → `LIKE '%secret%'`.

### References

- `FilterOp` enum (Contains, StartsWith, EndsWith): `crates/query-engine/src/input.rs:245-257`
- `filter_expr()` LIKE lowering: `crates/query-engine/src/lower.rs:923-925`
- `like_pattern()` — no length check, no escape: `crates/query-engine/src/lower.rs:931-934`
- `check_filter_types()` — no operator restriction: `crates/query-engine/src/validate.rs:226-284`
- `check_one_filter()` catch-all for LIKE ops: `crates/query-engine/src/validate.rs:275-279`
- JSON Schema filter definition (no column restriction): `config/schemas/graph_query.schema.json:343-380`
- `lower_aggregation()` — LIKE filters in aggregation WHERE: `crates/query-engine/src/lower.rs:123-185`
- String column count (112 across 24 entities): `config/ontology/nodes/`

---

## Task 2: Restriction analysis — no guards at any layer

**Status:** FAIL — no minimum pattern length, no per-column operator restriction, no sensitivity metadata

**No minimum pattern length:** `like_pattern()` (`lower.rs:931-934`) accepts any string including empty (`""`), producing `LIKE '%%'` which matches everything. Single-character patterns like `LIKE '%a%'` are fully accepted. The JSON Schema at `graph_query.schema.json:378` specifies `"value": { "type": "string" }` with no `minLength` constraint. No length validation exists anywhere in the pipeline.

**No per-column operator restrictions:** The ontology property definition (`ontology.schema.json:51-80`) supports only: `type`, `source`, `nullable`, `description`, `values`, `enum_type`. There is no `filterable`, `searchable`, `like_allowed`, `sensitive`, or `pii` field. `additionalProperties: false` at line 79 means extending requires a schema change.

**No operator-to-type restriction in validation:** `check_one_filter()` (`validate.rs:241-284`) only validates value type compatibility. The `_ =>` arm at line 275 treats Contains/StartsWith/EndsWith identically to Eq — it calls `check_value_type(value, data_type)` which accepts strings for String/Date/DateTime/Uuid DataTypes. LIKE can technically be applied to Date and UUID columns too.

### References

- `like_pattern()` — no length enforcement: `crates/query-engine/src/lower.rs:931-934`
- JSON Schema filter value — no minLength: `config/schemas/graph_query.schema.json:378`
- Ontology property schema — no sensitivity field: `config/schemas/ontology.schema.json:51-80`
- `additionalProperties: false` blocks extension: `config/schemas/ontology.schema.json:79`
- `check_one_filter()` — no operator restriction: `crates/query-engine/src/validate.rs:241-284`
- `check_value_type()` — string accepted for String/Date/DateTime/Uuid: `crates/query-engine/src/validate.rs:47-53`

---

## Task 3: Rate limiting against LIKE probing

**Status:** FAIL — no rate limiting exists at any layer

Rate limiting is not implemented. The metric `rate_limited: Counter<u64>` is defined (`metrics.rs:34`, `metrics.rs:68-71`) but never incremented by any server code. It is exported for the server layer (`metrics.rs:16`), but the server has no rate limiting middleware.

The router (`router.rs:103-117`) has four layers: `HttpMetricsLayer`, `CorrelationIdLayer`, `TraceLayer`, `PropagateCorrelationIdLayer`. No `RateLimitLayer`, `GovernorLayer`, or throttling middleware exists. No rate limiting dependencies appear in any `Cargo.toml`.

Without rate limiting, an attacker can send LIKE probing queries at HTTP throughput (~50-200 queries/second per connection). `LIKE '%x%'` on ClickHouse requires full column scans (cannot use primary key indexes for substring patterns), creating both an enumeration vector and a DoS amplification vector.

### References

- `rate_limited` counter defined but unused: `crates/query-engine/src/metrics.rs:34,68-71`
- Counter exported (never called): `crates/query-engine/src/metrics.rs:16`
- Router layers — no rate limiting: `crates/gkg-server/src/webserver/router.rs:103-117`
- No rate limit dependencies in Cargo.toml files

---

## Task 4: Ontology metadata for LIKE restriction — extension path

**Status:** IMPROVEMENT OPPORTUNITY — ontology schema needs extension

The ontology JSON schema (`ontology.schema.json:51-80`) has no field-level sensitivity or operator restriction metadata. `additionalProperties: false` means a schema change is required.

**Recommended extension:**

1. Add `like_allowed: { type: boolean, default: true }` to `propertyDefinition` in `ontology.schema.json`
2. Load into `FieldDefinition` in the ontology crate
3. Check in `validate.rs:check_one_filter()`:

```rust
if matches!(op, FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith) {
    if !self.ontology.is_like_allowed(entity, prop) {
        return Err(QueryError::Validation(...));
    }
}
```

4. Default sensitive columns to `like_allowed: false`: vulnerability title/description, finding name/description/solution, user email/name/first_name/last_name, note text, occurrence cve/location

**Alternative mitigations (combinable):**

- Minimum pattern length (e.g., 3 chars) in `like_pattern()` or `check_one_filter()`
- Rate limiting per caller per time window
- Escape `%` and `_` metacharacters in user-supplied LIKE values (`lower.rs:932`)

### References

- Ontology property schema: `config/schemas/ontology.schema.json:51-80`
- `additionalProperties: false`: `config/schemas/ontology.schema.json:79`
- `check_one_filter()` insertion point: `crates/query-engine/src/validate.rs:241-284`
- `like_pattern()` insertion point: `crates/query-engine/src/lower.rs:931-934`
- `compile()` has `&Ontology` available: `crates/query-engine/src/lib.rs:91-93`

---

## Summary

### TQ9: LIKE Enumeration

| Task | Verdict | Open Items |
|------|---------|------------|
| LIKE + COUNT enumeration risk | **FAIL** | All 112 string columns across 24 entities exposed. 15+ at HIGH/CRITICAL sensitivity. Character-by-character probing via COUNT + LIKE. |
| Restriction analysis | **FAIL** | No minimum pattern length. No per-column operator restriction. No sensitivity metadata in ontology. |
| Rate limiting against probing | **FAIL** | Not implemented. Metric defined but unincremented. No concurrency gate. |
| Ontology metadata for LIKE restriction | **IMPROVEMENT** | Schema extension path identified. `like_allowed: bool` on `propertyDefinition` + validation check. |

---

# TQ10: Hydration AuthZ Boundary — Security Task Results

## Task 1: Verify hydration queries include traversal_path filters

**Status:** PASS — hydration queries go through full `compile()` including `apply_security_context`

Both dynamic and static hydration queries are compiled via `compile_and_fetch()` (`hydration.rs:82-100`), which calls the full `compile()` pipeline (`lib.rs:91-113`):

```
validated_input() → lower() → enforce_return() → apply_security_context() → check_ast() → codegen()
```

At `hydration.rs:87-88`:

```rust
let compiled = compile(&query_json, &ctx.ontology, ctx.security_context()?)
    .map_err(|e| PipelineError::Compile(e.to_string()))?;
```

The `SecurityContext` is the same one established for the original request, passed through `ctx.security_context()`. This means:

1. `apply_security_context()` (`security.rs:73-82`) injects `startsWith(traversal_path, ...)` filters into the hydration query AST
2. `check_ast()` (`check.rs:19-28`) verifies post-compilation that every non-skipped `gl_*` alias has a valid security predicate
3. The hydration query is a `search` type (`hydration.rs:194`) targeting a single `gl_*` table, so exactly one alias gets the traversal_path filter

**Exception:** `gl_user` is in `SKIP_SECURITY_FILTER_TABLES` (`constants.rs:30`), so User hydration queries skip traversal_path filters. This is by design — User visibility is determined via Layer 3 redaction (Rails `read_user` ability), not path hierarchy.

### References

- `compile_and_fetch()` calls full `compile()`: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:87-88`
- `compile()` pipeline with `apply_security_context`: `crates/query-engine/src/lib.rs:91-113`
- `apply_security_context()`: `crates/query-engine/src/security.rs:73-82`
- `check_ast()` defense-in-depth: `crates/query-engine/src/check.rs:19-28`
- Same security context used: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:87`
- User skip list: `crates/query-engine/src/constants.rs:27-30`

---

## Task 2: Unknown entity types in _gkg_neighbor_type — graceful failure

**Status:** PASS — fail-closed at two layers

When `build_dynamic_search_query()` encounters an entity type string from the `_gkg_neighbor_type` column, it performs an ontology lookup at `hydration.rs:175-179`:

```rust
let node = ctx.ontology.get_node(entity_type).ok_or_else(|| {
    PipelineError::Execution(format!(
        "entity type not found in ontology during dynamic hydration: {entity_type}"
    ))
})?;
```

**Layer 1 — ontology lookup failure:** If the entity type is not in the ontology, `build_dynamic_search_query` returns `PipelineError::Execution`. This propagates via `?` through `hydrate_dynamic()` (`hydration.rs:71`) via the `collect::<Result<Vec<_>, PipelineError>>()?`, failing the entire hydration stage.

**Layer 2 — compile() validation:** Even if the ontology lookup somehow passed, the generated JSON goes through `compile()` which runs `validated_input()` (`lib.rs:96`). The ontology-derived JSON schema validates entity names against an enum of allowed values. An unknown entity would be rejected by `check_json()` or `check_ontology()`.

**Layer 3 — redaction fail-closed:** At the redaction level (`query_result.rs:374-388`), if `get_entity_auth()` returns `None` for an unknown entity type, `is_authorized()` returns `false` — the row is denied. This is correct fail-closed behavior.

**Finding:** The error at Layer 1 exposes the unknown entity type string in the error message sent to the client (`PipelineError::Execution` propagates verbatim — same issue as TQ6). This is a minor information leakage concern but not a security bypass.

### References

- Ontology lookup with fail-closed: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:175-179`
- Error propagation through `hydrate_dynamic()`: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:68-71`
- `compile()` validation layer: `crates/query-engine/src/lib.rs:96`
- `is_authorized()` returns false for unknown entity: `crates/gkg-server/src/redaction/query_result.rs:374-381`

---

## Task 3: MAX_DYNAMIC_HYDRATION_RESULTS enforcement

**Status:** PASS with caveat — enforced server-side, but tension with MAX_NODE_IDS

`MAX_DYNAMIC_HYDRATION_RESULTS` is defined as `1000` at `constants.rs:52` and applied at `hydration.rs:201`:

```rust
"limit": ids.len().min(MAX_DYNAMIC_HYDRATION_RESULTS)
```

This is baked into the hydration query JSON **before** compilation and ClickHouse execution. The user has no control over this value. The limit is per entity type — if dynamic hydration discovers N entity types, up to N × 1000 rows total could be fetched. N is bounded by the ontology (~25 entity types).

**Caveat — MAX_NODE_IDS tension:** The hydration query sets `"node_ids": ids` where `ids` are deduped IDs from base query results. The `node_ids` array goes through `compile()` → `validated_input()` → `check_depth()` which enforces `MAX_NODE_IDS = 500` (`validate.rs:159,191-197`). This means:

- If deduped IDs for one entity type exceed 500, the hydration `compile()` call **fails** with `LimitExceeded`
- `MAX_DYNAMIC_HYDRATION_RESULTS = 1000` can never actually be reached because `MAX_NODE_IDS = 500` is hit first
- This is a fail-hard, not fail-graceful scenario — the entire pipeline errors instead of returning partial results

The effective cap is min(500, 1000) = 500 per entity type. The 1000 cap is dead in practice.

### References

- `MAX_DYNAMIC_HYDRATION_RESULTS = 1000`: `crates/query-engine/src/constants.rs:52`
- Limit applied in query JSON: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:201`
- `MAX_NODE_IDS = 500`: `crates/query-engine/src/validate.rs:159`
- `node_ids` length check: `crates/query-engine/src/validate.rs:191-197`
- IDs deduped before query build: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:146-149`

---

## Task 4: HydrationPlan::Static templates cannot be manipulated by query input

**Status:** PASS — static hydration is currently dead code; if enabled, templates go through full compile()

`build_hydration_plan()` (`lib.rs:122-132`) returns `HydrationPlan::None` for Traversal and Search (the only query types that would use static hydration). The TODO at lines 126-129 explains that static hydration requires a "slim SELECT" refactor in `lower.rs` that hasn't landed yet.

**If static hydration were enabled**, the security boundary is:

1. `HydrationTemplate` (`codegen.rs:46-55`) contains `entity_type`, `node_alias`, and `query_json` — all derived from validated `InputNode` definitions at compile time
2. `template.with_ids(&ids)` (`codegen.rs:59-64`) injects `node_ids` into the pre-built JSON template
3. The IDs come from `collect_static_ids()` (`hydration.rs:103-112`) which reads `_gkg_{alias}_id` columns from **`result.authorized_rows()` only** (line 106) — redacted row IDs are never included
4. The final JSON goes through full `compile()` (`hydration.rs:87`) including schema validation, ontology validation, and security context injection
5. Entity types are validated against the ontology; column names are validated against ontology fields; node_ids are `Vec<i64>` extracted from Arrow columns (cannot contain arbitrary strings)

No user input can influence the static template SQL beyond the entity types and columns already validated in the original query compilation.

### References

- `build_hydration_plan()` returns None for Traversal/Search: `crates/query-engine/src/lib.rs:126-130`
- `HydrationTemplate` struct: `crates/query-engine/src/codegen.rs:46-55`
- `with_ids()` injects node_ids: `crates/query-engine/src/codegen.rs:59-64`
- `collect_static_ids()` uses `authorized_rows()`: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:103-112`
- `hydrate_static()` calls `compile_and_fetch()`: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:30-57`
- `compile_and_fetch()` calls full `compile()`: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:87-88`

---

## Task 5: Hydration of redacted rows is impossible

**Status:** PASS — redaction occurs before hydration; all hydration paths filter to authorized_rows() only

The pipeline execution order is defined at `service.rs:58-75`:

```
SecurityStage → CompilationStage → ExecutionStage → ExtractionStage →
AuthorizationStage → RedactionStage → HydrationStage → FormattingStage
```

**RedactionStage** (line 69) runs **before** HydrationStage (line 71). `apply_authorizations()` (`query_result.rs:318-359`) sets `row.authorized = false` on denied rows.

**Every hydration code path filters on `authorized_rows()`:**

| Code path | Filter method | Location |
|-----------|---------------|----------|
| Static: collect IDs | `result.authorized_rows()` | `hydration.rs:106` |
| Static: merge properties | `result.authorized_rows_mut()` | `hydration.rs:120` |
| Dynamic: extract refs | `result.authorized_rows()` | `hydration.rs:138` |
| Dynamic: merge properties | `result.authorized_rows_mut()` | `hydration.rs:156` |

`authorized_rows()` is defined at `query_result.rs:361-362` as `self.rows.iter().filter(|r| r.authorized)`. `authorized_rows_mut()` at `query_result.rs:365-366` is the mutable equivalent.

**Result:** Redacted row IDs are never sent for hydration fetch. Redacted rows never have properties merged. The formatter (`formatting.rs`) also iterates only `authorized_rows()`. There is no code path where hydrated data for a redacted row can reach the client.

**Defense-in-depth:** Even if a redacted row's ID somehow entered a hydration query, the hydration query goes through `compile()` with the same `SecurityContext`, so traversal_path filters would scope results to the caller's namespace. And the hydration results are merged by ID lookup into `authorized_rows_mut()` — a redacted row wouldn't be in that iterator to receive the merge.

### References

- Pipeline order (redaction before hydration): `crates/gkg-server/src/query_pipeline/service.rs:58-75`
- `apply_authorizations()` sets `row.authorized = false`: `crates/gkg-server/src/redaction/query_result.rs:318-359`
- `authorized_rows()` filter: `crates/gkg-server/src/redaction/query_result.rs:361-362`
- `authorized_rows_mut()` filter: `crates/gkg-server/src/redaction/query_result.rs:365-366`
- Static ID collection from authorized only: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:106`
- Static merge into authorized only: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:120`
- Dynamic ref extraction from authorized only: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:138`
- Dynamic merge into authorized only: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:156`

---

## Summary

### TQ10: Hydration AuthZ Boundary

| Task | Verdict | Open Items |
|------|---------|------------|
| Hydration queries include traversal_path filters | **PASS** | Full `compile()` path including `apply_security_context` and `check_ast`. `gl_user` excluded by design. |
| Unknown entity types in _gkg_neighbor_type | **PASS** | Fail-closed at ontology lookup + compile validation + redaction. Error message leaks entity string (minor, same class as TQ6). |
| MAX_DYNAMIC_HYDRATION_RESULTS enforcement | **PASS** | Enforced server-side before execution. Effective cap is 500 (not 1000) due to `MAX_NODE_IDS` validation in `compile()`. >500 IDs fails hard, not graceful. |
| Static templates cannot be manipulated | **PASS** | Dead code currently. If enabled: templates from validated input, IDs from authorized rows only, full `compile()` on hydration queries. |
| Hydration of redacted rows impossible | **PASS** | Redaction before hydration. All 4 hydration code paths (static collect/merge, dynamic extract/merge) filter on `authorized_rows()`. |

---

# TQ11: Redaction Stream Integrity — Security Task Results

## Task 1: Verify empty authorizations redact all rows

**Status:** PASS — fail-closed, all rows denied

When `apply_authorizations()` (`query_result.rs:318-359`) receives an empty `authorizations` slice (`&[]`), every call to `is_authorized()` fails at two points:

1. **Line 382-387:** `authorizations.iter().find(|a| a.resource_type == auth_config.resource_type)` returns `None` because the slice is empty — returns `false`
2. Even if somehow reached, **line 388:** `auth.authorized.get(&node_ref.id).copied().unwrap_or(false)` — `unwrap_or(false)` ensures missing keys default to deny

Every row's `is_authorized()` returns `false` → `set_unauthorized()` is called → all rows redacted.

Additionally, a `ResourceAuthorization` with an **empty** `authorized` map (`{}`) also denies all rows: the `get(&node_ref.id)` returns `None`, `unwrap_or(false)` yields `false`.

**Test coverage:** `fail_closed_no_authorization_returns_nothing` (`redaction.rs:91`) — empty mock denies all 5 users. `no_authorizations_redacts_all` (`redaction.rs:98`).

### References

- `apply_authorizations()`: `crates/gkg-server/src/redaction/query_result.rs:318-359`
- `is_authorized()` — three return paths, all default false: `crates/gkg-server/src/redaction/query_result.rs:374-389`
- Empty auth → no match at line 382-387: `crates/gkg-server/src/redaction/query_result.rs:382-387`
- Missing key → `unwrap_or(false)`: `crates/gkg-server/src/redaction/query_result.rs:388`
- Integration test: `crates/integration-tests/tests/server/redaction.rs:91`

---

## Task 2: Verify missing resource types default to DENY

**Status:** PASS — fail-closed

`is_authorized()` at `query_result.rs:382-387`:

```rust
let Some(auth) = authorizations
    .iter()
    .find(|a| a.resource_type == auth_config.resource_type)
else {
    return false;
};
```

If the row's entity maps to `resource_type: "project"` but no `ResourceAuthorization` with `resource_type == "project"` exists in the authorizations list, `find()` returns `None` and the function returns `false`. Row is denied.

Additionally, if the entity type has no `EntityAuthConfig` at all (unknown entity type), the check at **line 379-381** returns `false` immediately:

```rust
let Some(auth_config) = ctx.get_entity_auth(&node_ref.entity_type) else {
    return false;
};
```

**Test coverage:** `fail_closed_missing_resource_authorization` (`query_result.rs:883`).

### References

- Missing resource type → false: `crates/gkg-server/src/redaction/query_result.rs:382-387`
- Unknown entity type → false: `crates/gkg-server/src/redaction/query_result.rs:379-381`
- Test: `crates/gkg-server/src/redaction/query_result.rs:883`

---

## Task 3: Verify missing IDs default to DENY

**Status:** PASS — fail-closed

`is_authorized()` at `query_result.rs:388`:

```rust
auth.authorized.get(&node_ref.id).copied().unwrap_or(false)
```

If the `ResourceAuthorization` for the correct `resource_type` exists but the specific `id` is not a key in the `authorized` map, `get()` returns `None`, `unwrap_or(false)` yields `false`. Row is denied.

NULL IDs are also handled fail-closed at `apply_authorizations()` lines 329-334:

```rust
let Some(node_ref) = row.node_ref(redaction_node) else {
    // Fail closed: NULL IDs cannot be verified, so deny the row
    row.set_unauthorized();
    redacted_count += 1;
    break;
};
```

**Test coverage:** `fail_closed_partial_authorization_denies_unknown_ids` (`redaction.rs:122`) — 5 users, only 2 authorized, 3 denied. `fail_closed_null_id_denies_row` (`redaction.rs:863`). `fail_closed_null_type_denies_row` (`redaction.rs:1901`).

### References

- Missing ID → `unwrap_or(false)`: `crates/gkg-server/src/redaction/query_result.rs:388`
- NULL id/type → fail-closed: `crates/gkg-server/src/redaction/query_result.rs:329-334`
- Partial auth test: `crates/integration-tests/tests/server/redaction.rs:122`
- NULL ID test: `crates/integration-tests/tests/server/redaction.rs:863`
- NULL type test: `crates/integration-tests/tests/server/redaction.rs:1901`

---

## Task 4: Stream interruption mid-authorization (Rails goes down)

**Status:** PASS — fail-closed on all stream errors, but no timeout

The authorization exchange uses **bidirectional gRPC streaming** within the `ExecuteQuery` RPC (`gkg.proto:30`). The server sends a `RedactionRequired` message and waits for a `RedactionResponse` on the same stream.

At `stream.rs:112-122`, `stream.next().await` handles three outcomes:

```rust
let redaction_msg = match stream.next().await {
    Some(Ok(msg)) => msg,                    // success
    Some(Err(e)) => {                        // gRPC transport error
        return Err(RedactionExchangeError::ReceiveFailed(e));
    }
    None => {                                 // stream closed
        return Err(RedactionExchangeError::StreamClosed);
    }
};
```

**Stream closed (Rails disconnects):** Returns `StreamClosed` → `Status::cancelled` (`stream.rs:26-28`). Query fails, no results returned.

**gRPC transport error:** Returns `ReceiveFailed(status)` → original gRPC `Status` propagated (`stream.rs:29`). Query fails.

**Client sends Error message instead of RedactionResponse:** `unwrap_redaction()` at `stream.rs:62-66` catches `Content::Error(e)` and returns `ClientError { code, message }` → `Status::aborted` (`stream.rs:35-38`). Query fails.

**Client sends wrong message type:** `unwrap_redaction()` at `stream.rs:68-73` returns `InvalidMessage` → `Status::invalid_argument` (`stream.rs:30`). Query fails.

**All paths are fail-closed.** No partial results are ever returned.

**Gap — no timeout on `stream.next().await`:** If Rails hangs indefinitely without sending a response and without closing the stream, the `stream.next().await` will block forever. There is no `tokio::time::timeout` wrapping the await. The query will hang until the client (Rails) disconnects or the TCP connection times out at the OS level. This is the same class of issue as TQ5 (no query timeout).

**Minor — `let _ =` on send:** At `stream.rs:110`, `let _ = tx.send(...)` silently discards the send result. If the channel is closed (client already disconnected), the send fails silently. However, the subsequent `stream.next().await` will return `None` → `StreamClosed` → fail-closed. So this is safe but a code smell — an explicit error log would aid debugging.

### References

- Stream read with three outcomes: `crates/gkg-server/src/redaction/stream.rs:112-122`
- `StreamClosed` → `Status::cancelled`: `crates/gkg-server/src/redaction/stream.rs:26-28`
- `ReceiveFailed` → original Status: `crates/gkg-server/src/redaction/stream.rs:29`
- `ClientError` handling: `crates/gkg-server/src/redaction/stream.rs:62-66`
- `InvalidMessage` handling: `crates/gkg-server/src/redaction/stream.rs:68-73`
- No timeout on stream.next().await: `crates/gkg-server/src/redaction/stream.rs:112`
- Silent send discard: `crates/gkg-server/src/redaction/stream.rs:110`
- Error type → Status mapping: `crates/gkg-server/src/redaction/stream.rs:23-41`
- Proto bidirectional streaming: `crates/gkg-server/proto/gkg.proto:30`

---

## Task 5: result_id mismatch handling — stale/confused responses

**Status:** PASS — pipeline aborts on mismatch

Each authorization request generates a fresh UUID v4 at `stream.rs:86`:

```rust
let result_id = Uuid::new_v4().to_string();
```

This `result_id` is embedded in the `RedactionRequired` message sent to Rails (`stream.rs:105`). Upon receiving the response, the server validates the `result_id` match at `stream.rs:136-141`:

```rust
if redaction_response.result_id != result_id {
    return Err(RedactionExchangeError::ResultIdMismatch {
        expected: result_id,
        received: redaction_response.result_id,
    });
}
```

**On mismatch:** `ResultIdMismatch` is logged at warn level (`stream.rs:32`) and converted to `Status::invalid_argument("result_id mismatch in redaction response")` (`stream.rs:33`). The query fails entirely. No results are returned.

**Proto fields:** `RedactionRequired.result_id` (`gkg.proto:179`) and `RedactionResponse.result_id` (`gkg.proto:192`) are both `string` fields.

**Test coverage:** Unit test at `stream.rs:170-175` validates the error conversion. No integration test exercises an actual mismatch over a live stream.

### References

- UUID generation: `crates/gkg-server/src/redaction/stream.rs:86`
- result_id in request: `crates/gkg-server/src/redaction/stream.rs:105`
- result_id validation: `crates/gkg-server/src/redaction/stream.rs:136-141`
- Mismatch → Status::invalid_argument: `crates/gkg-server/src/redaction/stream.rs:31-34`
- Unit test for error conversion: `crates/gkg-server/src/redaction/stream.rs:170-175`
- Proto RedactionRequired.result_id: `crates/gkg-server/proto/gkg.proto:179`
- Proto RedactionResponse.result_id: `crates/gkg-server/proto/gkg.proto:192`

---

## Task 6: Verify mTLS is enforced on the authorization gRPC channel

**Status:** FAIL — no TLS at the application layer; relies entirely on infrastructure

The gRPC server is configured at `grpc/server.rs:47-49`:

```rust
TonicServer::builder()
    .add_service(self.service)
    .serve(self.addr)
```

No `ServerTlsConfig` is provided to the builder. No `ClientTlsConfig` exists anywhere in the gRPC server code. The connection is **plaintext** at the application level.

**TLS mentions in the codebase:**
- `rustls` is a dependency but used only for the `reqwest`-based `GitlabClient` (REST API for indexer, not authorization): `main.rs:25-27`
- `health_client.rs:59` installs `rustls` provider for HTTPS health checks
- Helm values show `ssl: false` for ClickHouse connections (`values.yaml:28,34`)
- No Istio, Envoy, or service mesh configuration found in `helm-dev/`

**The authorization exchange (RedactionRequired → RedactionResponse) travels over the same bidirectional gRPC stream as the client's ExecuteQuery request.** This means the transport security of the authorization exchange is determined by how the external client (Rails) connects to GKG — which is outside GKG's application code.

**In production (GitLab.com):** Transport encryption is likely handled at the infrastructure level (K8s ingress, service mesh, or load balancer TLS termination), but this is not enforced or verified by the GKG application itself. GKG cannot distinguish a plaintext connection from an encrypted one.

### References

- gRPC server builder — no TLS: `crates/gkg-server/src/grpc/server.rs:47-49`
- rustls used only for reqwest/health: `crates/gkg-server/src/main.rs:25-27`
- No TLS grep hits in gRPC code: `crates/gkg-server/src/grpc/` (zero matches)
- Helm ClickHouse ssl: false: `helm-dev/gkg/values.yaml:28,34`

---

## Task 7: Concurrent authorization requests do not share stream state

**Status:** PASS — fully isolated per request

Each `execute_query` call (`grpc/service.rs:105-137`) creates:

1. **Per-request stream:** `let mut stream = request.into_inner()` (`service.rs:113`) — each gRPC call has its own `Streaming<ExecuteQueryMessage>`
2. **Per-request channel:** `let (tx, rx) = mpsc::channel(4)` (`service.rs:114`) — fresh sender/receiver pair
3. **Per-request context:** `QueryPipelineContext` created fresh in `run_query()` (`service.rs:44-49`) with `compiled: None`, `security_context: None`
4. **Per-request pipeline request:** `PipelineRequest` at `service.rs:51-56` binds the per-request `tx` and `stream`

`RedactionService` is a **stateless unit struct** (`stream.rs:78`) with no fields. `request_authorization()` is a static method that operates entirely on the per-request `tx`/`stream` arguments. There is no shared state between concurrent authorization exchanges.

The only shared resources are `Arc<Ontology>` (immutable) and `Arc<ArrowClickHouseClient>` (connection-pooled HTTP client). Neither carries per-request state.

Each request is spawned via `tokio::spawn` (`grpc/service.rs:119`), providing task-level isolation.

### References

- Per-request stream: `crates/gkg-server/src/grpc/service.rs:113`
- Per-request channel: `crates/gkg-server/src/grpc/service.rs:114`
- Per-request context: `crates/gkg-server/src/query_pipeline/service.rs:44-49`
- Per-request pipeline request: `crates/gkg-server/src/query_pipeline/service.rs:51-56`
- `RedactionService` stateless struct: `crates/gkg-server/src/redaction/stream.rs:78`
- `request_authorization()` takes per-request args: `crates/gkg-server/src/redaction/stream.rs:81-84`
- `tokio::spawn` per request: `crates/gkg-server/src/grpc/service.rs:119`
- Shared immutable ontology: `crates/gkg-server/src/query_pipeline/service.rs:21`
- Shared pooled ClickHouse client: `crates/gkg-server/src/query_pipeline/service.rs:22`

---

## Summary

### TQ11: Redaction Stream Integrity

| Task | Verdict | Open Items |
|------|---------|------------|
| Empty authorizations redact all rows | **PASS** | `unwrap_or(false)` at every level. Integration test coverage. |
| Missing resource types default to DENY | **PASS** | `find()` returns None → false. Unknown entity type → false. |
| Missing IDs default to DENY | **PASS** | `get().unwrap_or(false)`. NULL id/type → fail-closed. |
| Stream interruption mid-authorization | **PASS** | All 4 error paths fail-closed (StreamClosed, ReceiveFailed, ClientError, InvalidMessage). **No timeout on `stream.next().await`** — hangs if Rails stalls without disconnecting (same class as TQ5). |
| result_id mismatch handling | **PASS** | UUID v4 per request. Strict equality check. Mismatch → query abort. No integration test for live mismatch. |
| mTLS on authorization gRPC channel | **FAIL** | No TLS at application layer. `TonicServer::builder()` with no `ServerTlsConfig`. Relies on infrastructure-level encryption. |
| Concurrent requests don't share state | **PASS** | Per-request stream, channel, context, pipeline. Stateless `RedactionService`. `tokio::spawn` isolation. |

---

# TQ12: Pipeline Stage Bypass — Security Task Results

## Task 1: ctx.security_context() returns Err if called before SecurityStage

**Status:** PASS — runtime guard via Option::None

`QueryPipelineContext` is initialized with `security_context: None` at `service.rs:48`. The accessor at `types.rs:39-43`:

```rust
pub fn security_context(&self) -> Result<&SecurityContext, PipelineError> {
    self.security_context.as_ref().ok_or_else(|| {
        PipelineError::Security(SecurityError("security context not yet available".into()))
    })
}
```

If `SecurityStage` has not run, `security_context` remains `None` and any caller gets `Err(PipelineError::Security(...))`. `CompilationStage` calls `ctx.security_context()?` at `compilation.rs:29` — it will fail if SecurityStage was skipped.

`SecurityStage` sets the field at `security.rs:43`: `ctx.security_context = Some(security_context)`.

**Note:** The `security_context` field is `pub` on the struct (`types.rs:29`), so any code with `&mut QueryPipelineContext` could set it directly, bypassing `SecurityStage`. However, grep confirms only `SecurityStage` writes to this field — no other code sets `ctx.security_context =`.

### References

- Context initialized with None: `crates/gkg-server/src/query_pipeline/service.rs:44-49`
- `security_context()` accessor returns Err on None: `crates/gkg-server/src/query_pipeline/types.rs:39-43`
- SecurityStage sets the field: `crates/gkg-server/src/query_pipeline/stages/security.rs:43`
- CompilationStage calls `security_context()?`: `crates/gkg-server/src/query_pipeline/stages/compilation.rs:29`
- Field is `pub`: `crates/gkg-server/src/query_pipeline/types.rs:29`

---

## Task 2: ctx.compiled() returns Err if called before CompilationStage

**Status:** PASS — runtime guard via Option::None

`QueryPipelineContext` is initialized with `compiled: None` at `service.rs:45`. The accessor at `types.rs:33-37`:

```rust
pub fn compiled(&self) -> Result<&Arc<CompiledQueryContext>, PipelineError> {
    self.compiled.as_ref().ok_or_else(|| {
        PipelineError::Compile("compiled query context not yet available".into())
    })
}
```

Consumers that call `ctx.compiled()?`:
- `ExecutionStage` at `execution.rs:27` — reads `compiled.base.sql` and `compiled.base.params`
- `HydrationStage` at `hydration.rs:174,257` — reads `compiled.input` and `compiled.hydration`
- `FormattingStage` at `formatting.rs:38` — reads `compiled.base.sql` for `raw_query_strings`

All will fail with `PipelineError::Compile` if CompilationStage has not run.

`CompilationStage` sets the field at `compilation.rs:40`: `ctx.compiled = Some(Arc::new(compiled))`.

**Same `pub` note as Task 1:** The `compiled` field is `pub` (`types.rs:26`), but only `CompilationStage` writes to it.

### References

- Context initialized with None: `crates/gkg-server/src/query_pipeline/service.rs:45`
- `compiled()` accessor returns Err on None: `crates/gkg-server/src/query_pipeline/types.rs:33-37`
- CompilationStage sets the field: `crates/gkg-server/src/query_pipeline/stages/compilation.rs:40`
- ExecutionStage calls `compiled()?`: `crates/gkg-server/src/query_pipeline/stages/execution.rs:27`
- HydrationStage calls `compiled()?`: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:174,257`
- FormattingStage calls `compiled()?`: `crates/gkg-server/src/query_pipeline/stages/formatting.rs:38`
- Fields are `pub`: `crates/gkg-server/src/query_pipeline/types.rs:26,29`

---

## Task 3: All pipeline invocations use the full 8-stage chain

**Status:** PASS — single invocation site, type-enforced from Extraction onward

There is exactly **one** pipeline invocation in the entire codebase: `run_query()` at `service.rs:58-75`:

```rust
PipelineRunner::start(&mut ctx, req, &mut obs)
    .then(&SecurityStage).await?
    .then(&CompilationStage).await?
    .then(&ExecutionStage).await?
    .then(&ExtractionStage).await?
    .then(&AuthorizationStage).await?
    .then(&RedactionStage).await?
    .then(&HydrationStage).await?
    .then(&self.formatter).await?
    .finish()
```

This is called from two places in `grpc/service.rs`: line 131 (`raw_pipeline.run_query`) and line 135 (`llm_pipeline.run_query`). Both use the same `run_query` method with the full chain.

**Type chain analysis:**

The `PipelineRunner::then()` method at `mod.rs:56-58` enforces `S: PipelineStage<M, Input = T>` — each stage's `Input` must match the previous stage's `Output`. The full chain:

| Stage | Input | Output |
|-------|-------|--------|
| SecurityStage | `()` | `()` |
| CompilationStage | `()` | `()` |
| ExecutionStage | `()` | `ExecutionOutput` |
| ExtractionStage | `ExecutionOutput` | `ExtractionOutput` |
| AuthorizationStage | `ExtractionOutput` | `AuthorizationOutput` |
| RedactionStage | `AuthorizationOutput` | `RedactionOutput` |
| HydrationStage | `RedactionOutput` | `HydrationOutput` |
| FormattingStage | `HydrationOutput` | `PipelineOutput` |

**From ExtractionStage onward, the type system enforces ordering at compile time.** You cannot call AuthorizationStage before ExtractionStage because the types don't match.

**Limitation:** The first three stages (Security, Compilation, Execution) all accept `()` as input, so Rust's type system alone doesn't prevent reordering them. The ordering of these three is enforced only by **runtime guards**: CompilationStage calls `ctx.security_context()?` (fails if Security hasn't run), ExecutionStage calls `ctx.compiled()?` (fails if Compilation hasn't run).

**No other pipeline invocations exist.** Grep for `PipelineRunner::start`, `run_query`, and `.then(&` confirms only `service.rs:58` constructs a pipeline. Hydration sub-queries call `compile()` directly (not via the pipeline) — this is correct since they only need compilation, not the full authorization/redaction flow.

### References

- Single pipeline invocation: `crates/gkg-server/src/query_pipeline/service.rs:58-75`
- Two callers of `run_query`: `crates/gkg-server/src/grpc/service.rs:131,135`
- `then()` type constraint `S::Input = T`: `crates/gkg-server/src/query_pipeline/stages/mod.rs:56-58`
- Security→Compilation runtime guard: `crates/gkg-server/src/query_pipeline/stages/compilation.rs:29`
- Compilation→Execution runtime guard: `crates/gkg-server/src/query_pipeline/stages/execution.rs:27`
- Hydration uses `compile()` directly (not pipeline): `crates/gkg-server/src/query_pipeline/stages/hydration.rs:87`

---

## Task 4: Concurrent queries do not share QueryPipelineContext

**Status:** PASS — fresh context per request

Each `execute_query` gRPC call at `grpc/service.rs:119` spawns a `tokio::spawn`. Inside, `run_query()` creates a fresh `QueryPipelineContext` at `service.rs:44-49`:

```rust
let mut ctx = QueryPipelineContext {
    compiled: None,
    ontology: Arc::clone(&self.ontology),
    client: Arc::clone(&self.client),
    security_context: None,
};
```

- `compiled: None` — fresh, per-request
- `security_context: None` — fresh, per-request
- `ontology: Arc::clone(...)` — shared reference to immutable `Ontology`
- `client: Arc::clone(...)` — shared reference to connection-pooled `ArrowClickHouseClient`

The context is passed as `&mut ctx` to the pipeline, so it is exclusively owned by the current task. No other task can access it. The `PipelineRunner` holds `&'a mut QueryPipelineContext` (`mod.rs:35`), which Rust's borrow checker ensures is exclusive.

Shared resources (`ontology`, `client`) are immutable or connection-pooled with no per-request state.

### References

- Per-request context creation: `crates/gkg-server/src/query_pipeline/service.rs:44-49`
- `tokio::spawn` per request: `crates/gkg-server/src/grpc/service.rs:119`
- `PipelineRunner` holds `&mut`: `crates/gkg-server/src/query_pipeline/stages/mod.rs:33-38`
- `QueryPipelineService` shared fields are `Arc`: `crates/gkg-server/src/query_pipeline/service.rs:20-24`

---

## Task 5: PipelineStage trait implementations — no shared mutable state

**Status:** PASS — all stages are stateless unit structs or contain only immutable config

Every stage struct:

| Stage | Fields | Mutability |
|-------|--------|------------|
| `SecurityStage` | none (unit struct) | stateless |
| `CompilationStage` | none (unit struct) | stateless |
| `ExecutionStage` | none (unit struct) | stateless |
| `ExtractionStage` | none (unit struct) | stateless |
| `AuthorizationStage` | none (unit struct) | stateless |
| `RedactionStage` | none (unit struct) | stateless |
| `HydrationStage` | none (unit struct) | stateless |
| `FormattingStage<F>` | `formatter: F` (impl `ResultFormatter`) | immutable config, cloned per service |

No stage contains `Mutex`, `RwLock`, `AtomicU*`, `Cell`, `RefCell`, `static`, `lazy_static`, `once_cell`, or `thread_local`. Grep across all stage files confirms zero hits for any shared mutable state primitives.

All state mutation happens through `&mut QueryPipelineContext` (per-request, exclusively owned) or through the stage's `Input` value (moved, not shared).

The `PipelineStage::execute` signature at `mod.rs:22-28` takes `&self` (shared reference to stage) and `&mut QueryPipelineContext` (exclusive reference to per-request context). This design means stages cannot mutate their own fields — they can only mutate the per-request context.

### References

- `PipelineStage::execute` signature (`&self`): `crates/gkg-server/src/query_pipeline/stages/mod.rs:22-28`
- SecurityStage unit struct: `crates/gkg-server/src/query_pipeline/stages/security.rs:13`
- CompilationStage unit struct: `crates/gkg-server/src/query_pipeline/stages/compilation.rs:14`
- ExecutionStage unit struct: `crates/gkg-server/src/query_pipeline/stages/execution.rs:13`
- ExtractionStage unit struct: `crates/gkg-server/src/query_pipeline/stages/extraction.rs:11`
- AuthorizationStage unit struct: `crates/gkg-server/src/query_pipeline/stages/authorization.rs:15`
- RedactionStage unit struct: `crates/gkg-server/src/query_pipeline/stages/redaction.rs:11`
- HydrationStage unit struct: `crates/gkg-server/src/query_pipeline/stages/hydration.rs:26`
- FormattingStage has only `formatter: F`: `crates/gkg-server/src/query_pipeline/stages/formatting.rs:10-12`
- No shared mutable state primitives in stages: `crates/gkg-server/src/query_pipeline/stages/` (zero grep hits)

---

## Summary

### TQ12: Pipeline Stage Bypass

| Task | Verdict | Open Items |
|------|---------|------------|
| `security_context()` Err before SecurityStage | **PASS** | Option::None → Err. Only SecurityStage writes the field. Field is `pub` — no compile-time write restriction, but no other code writes it. |
| `compiled()` Err before CompilationStage | **PASS** | Option::None → Err. Only CompilationStage writes the field. Same `pub` note. |
| All invocations use full 8-stage chain | **PASS** | Single `run_query()` site. Type system enforces order from Extraction onward. First 3 stages (`()→()`) order enforced by runtime guards only. |
| Concurrent queries don't share context | **PASS** | Fresh `QueryPipelineContext` per request. `&mut` exclusive borrow. Shared `Arc` resources are immutable/pooled. |
| No shared mutable state in stages | **PASS** | All stages are stateless unit structs or hold immutable config. `&self` on `execute()`. No Mutex/RwLock/Atomic/static. |
