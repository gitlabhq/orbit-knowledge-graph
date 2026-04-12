# AI Usefulness Report: Orbit Code Graph

## 20 Opus 4.6 agents evaluated the graph against real AI use cases on the GitLab monolith (415K defs, 334K CALLS, 160K IMPORTS)

## Ratings by Use Case

| Use Case | Rating | Precision | Recall | Key Finding |
|----------|--------|-----------|--------|-------------|
| Architecture visualization | **5/10** | High | Medium | Layer detection works; found real model->service violations. Needs module-level abstraction layer |
| Code review context | **7/10** | ~100% | 88% | Blast radius estimation is the killer feature (createAlert: 2,270 callers). Missing: function signatures, test coverage |
| Migration planning | **7/10** | High | High | Immediate scope sizing (643 Vuex vs 271 Pinia). Gap: inter-store dependency ordering |
| Code similarity | **7/10** | High | Medium | Import-path Jaccard correctly identifies near-clones (89.5% overlap) |
| Find all callers | **8/10** | ~100% | 88.3% | createAlert: 598/677 non-spec callers found. Zero false positives. 79 missed (arrow fn Variable defs) |
| Onboarding | **6/10** | High | Medium | gl_imported_symbol is the most useful table. Template composition invisible |
| Complexity hotspots | **6/10** | Medium | High | File-level great. Function-level needs COUNT(DISTINCT target_id) |
| Circular dependencies | **6/10** | High | Low | Found 3 real JS import cycles. Ruby: no import edges exist |
| Semantic search | **5/10** | 34% | High | Strong at dependency tracing from known entry points. 66% false positive on keyword search |
| Impact analysis | **3/10** | 0% | 0% | CALLS edges linked by name matching, not import resolution. 100% of sampled cross-file CALLS were spurious |
| Error handling audit | **4/10** | 50-70% | Medium | Useful triage signal (641 axios files -> 130 candidates). Too many false positives for automation |
| Dead code detection | **4/10** | 40% | Medium | 60% false positive rate. ee/ scope + default export gaps |
| Vuex store tracing | **3/10** | Medium | 60% | commit()/dispatch() are string-based. Mutations invisible. 3 critical links broken |
| Vue component API | **2/10** | High | 15-20% | Only methods visible. No props/emits/slots/inject. Template invisible |
| Data model tracing | **3/10** | Medium | 25% | 0% GraphQL resolution. Template bindings invisible |

## What Works Best for AI (the graph's strengths)

1. **Import inventory (100% accurate)** -- the single most reliable signal. Every import captured with path, name, type
2. **Blast radius estimation** -- counting callers/importers of a function gives immediate impact assessment
3. **File-level coupling** -- cross-directory CALLS/IMPORTS identify architectural hotspots
4. **Intra-component call chains** -- Vue method-to-method resolution at ~90% accuracy
5. **Migration scope sizing** -- import patterns quantify framework usage precisely

## What Doesn't Work for AI (the graph's gaps)

### Gap 1: Cross-file CALLS are unreliable (affects 8 use cases)
- CALLS edges are name-matched, not import-resolved
- 100% of sampled cross-file CALLS were spurious name collisions (impact analysis agent)
- The IMPORTS table is the reliable signal, but it only shows file-to-symbol, not call-site-to-definition

### Gap 2: Vue component interface invisible (affects 5 use cases)
- Props: 0% captured (15+ per component)
- Emits: 0% captured
- Slots: 0% captured
- Provide/Inject: 0% captured
- Template bindings: 0% captured
- Only methods/computed visible (15-20% of API surface)

### Gap 3: No semantic annotations (affects 4 use cases)
- No domain/module tags on definitions
- Keyword search has 66% false positive rate
- No computed vs method vs watcher distinction (all "Method")
- No function signatures, parameters, return types

### Gap 4: GraphQL not indexed (affects 3 use cases)
- 1,698 .graphql imports at 0% resolution
- Data layer architecture invisible
- Cannot trace fetch -> transform -> display

### Gap 5: Arrow function exports = no outgoing CALLS (affects 3 use cases)
- `export const fn = () => {}` classified as Variable
- Vuex store actions (41/45 files) produce zero CALLS edges
- 79 production files calling createAlert are invisible because they use arrow function patterns

## Top 5 Improvements Ranked by AI Impact

1. **Import-resolved CALLS edges** -- replace name-matched CALLS with import-aware resolution. Would fix impact analysis (3->8), error handling (4->7), dead code (4->7)
2. **Vue props/emits/slots extraction** -- extract component interface from Options API. Would fix component API (2->7), onboarding (6->8)
3. **Index .graphql files** -- parse GraphQL queries/mutations as definitions. Would fix data model tracing (3->6)
4. **Arrow function Variable -> Function promotion** -- emit CALLS from arrow function bodies. Would fix find-all-callers (88->95% recall), Vuex store tracing
5. **Domain/module annotations** -- add tags derived from directory structure. Would fix semantic search (5->8)

## Methodology

Each agent was given a specific AI task scenario, ran actual DuckDB queries against the indexed GitLab monolith, verified results against source code at /Users/angelo.rivera/gitlab/gdk/gitlab, and rated the graph's usefulness for that task on a 1-10 scale with precision/recall analysis.
