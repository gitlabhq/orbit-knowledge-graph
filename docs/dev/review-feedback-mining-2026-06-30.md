# Review feedback mining: last 60 merged MRs

Date: 2026-06-30
Repo: `gitlab-org/orbit/knowledge-graph`
Window: last 60 merged merge requests, ordered by recent merged MRs from the GitLab API.

## Collection and filtering

I used the `review-feedback-mining` helper script:

```shell
/home/dgruzd/.agents/skills/review-feedback-mining/scripts/mine-mr-feedback.sh \
  gitlab-org/orbit/knowledge-graph --last 60 --out ./tmp/review-feedback-mining/run
```

The script listed 60 merged MRs, filtered dependency bumps/release chores/trivial MRs, and retained 42 substantive MRs with at least four human notes. It extracted 94 reviewer-initiated threads.

The raw collection produced:

- `tmp/review-feedback-mining/run/merged_mrs.json`
- `tmp/review-feedback-mining/run/threads.json`
- `tmp/review-feedback-mining/run/reviewer_threads.txt`
- `tmp/review-feedback-mining/run/summary.txt`

Those files are local helper output; this report carries the committed, auditable summary so the MR stays reviewable.

## Method

I did not use the GitLab `resolved` flag as the action signal. For each clustered theme, I read representative discussion reply chains and checked whether the author replied with "Done" or changed direction in a follow-up commit. Unresolved approval, maintainer handoff, bot orchestration, benchmark output, release/dependency chores, and pure praise were excluded from the theme counts.

## Ranked feedback themes

| Theme | Reviewer threads | Distinct MRs | Actioned | Enforcement decision |
| --- | ---: | ---: | ---: | --- |
| Keep LLM-/user-facing ontology and prompt text short | 6 | 3 | 6 | JSON Schema `maxLength` for ontology descriptions plus root guidance |
| Reuse existing infrastructure / put logic at the shared layer | 9 | 6 | 7 | Root guidance already exists; no honest lint for semantic reuse decisions |
| Avoid narration / low-value comments | 6 | 4 | 6 | Existing comment-guard CI + root guidance already cover this |
| Move graph-shape facts to ontology or generic hooks | 4 | 3 | 4 | Root guidance already exists; no additional machine-checkable pattern found |
| Prove performance-sensitive query/indexer changes | 5 | 4 | 3 | Prose-only; requires production context and EXPLAIN review |
| Prefer typed/named structures over tuples/constants | 4 | 3 | 3 | Prose-only; semantic design judgment |
| Add code-graph edge-case fixtures with new language behavior | 4 | 1 | 4 | Preventive default; too concentrated in one MR for a new recurring gate |

## Landed theme

### Keep LLM-/user-facing ontology and prompt text short

Reviewers repeatedly asked authors to shorten text that is read by users or fed to agents:

- !1930: ontology property descriptions were too long for LLM context.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1930#note_3508178150>
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1930#note_3508181115>
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1930#note_3508191260>
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1930#note_3508193112>
- !1897: validator/tool text should be shorter and more direct.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1897#note_3497371824>
- !1898: new agent guidance should say the same thing with fewer words.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1898#note_3497340914>

Classification: all six representative threads were actioned in reply chains or follow-up commits. The !1930 feedback was especially concrete: the reviewer tied description length to LLM token cost and asked for multiple ontology descriptions to be shortened.

Enforcement: ontology `description` fields are already validated by `ontology-schema-validate`, so the strongest honest machine check is a JSON Schema `maxLength` on node, edge, domain, variant, derived, and property descriptions. This MR sets that limit to 200 characters and shortens the one current over-limit ontology description. Root `AGENTS.md` / `CLAUDE.md` guidance explains the judgment behind the limit.

Verification: I created a throwaway ontology copy with a 250-character property description and ran the same validation command as CI. `check-jsonschema` failed with `is too long`, confirming the proposed gate fires.

## Themes not changed in this MR

### Reuse existing infrastructure / put logic at the shared layer

Representative notes:

- !1910: traversal-path logic belonged in shared helpers.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1910#note_3499303884>
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1910#note_3499304765>
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1910#note_3499306745>
- !1890: Python-specific behavior should be expressed through a generic import hook.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1890#note_3495084990>
- !1725: Java-specific logic should not be added to the performance-sensitive shared linker.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1725#note_3447911344>
- !1879: streaming writer foundations should be reusable.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1879#note_3493349170>

Classification: most representative threads were actioned: !1910 moved code into `gkg_utils::traversal_path`, !1890 moved Python behavior behind `LanguageHooks`, and !1725 reduced scope to avoid shared linker changes. One or two were explicit future follow-ups rather than immediate changes.

Enforcement: no reliable lint can know whether a new helper duplicates existing infrastructure or whether a language-specific hook is the right abstraction. Root guidance already says to reuse existing infrastructure and has code-graph-specific guidance. I did not add more prose because the rule already exists; recurrence indicates a review/judgment gap, not a missing sentence.

### Avoid narration / low-value comments

Representative notes:

- !1929: clean up comments before merge.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1929#note_3505305762>
- !1897: collapse or remove code comments.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1897#note_3497485664>
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1897#note_3497488768>
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1897#note_3503860234>
- !1879: comment not necessary.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1879#note_3493723693>

Classification: these were actioned in replies such as `Done using the improved remove-llm-comments skill` and reviewer approvals after cleanup.

Enforcement: the repo already has root guidance and `.gitlab/ci/comment-guard.yml`; !1832 added the mechanical guard. I did not add another check because it would be redundant.

### Move graph-shape facts to ontology or generic hooks

Representative notes:

- !1912: logic related to `ReindexSource` looked like ontology behavior.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1912#note_3500343065>
- !1890: language-specific behavior should be a generic hook.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1890#note_3495084990>
- !1725: Java-specific edge modeling should not add shared linker and ontology surface area.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1725#note_3447911344>

Classification: actioned in !1890 and !1725; !1912 was a design prompt in a larger MR. The root `AGENTS.md` already includes the ontology single-source-of-truth and generic-hook guidance, added by !1898.

Enforcement: no additional machine check is honest here. The decision depends on whether a fact is graph shape, ingestion plumbing, language parsing, or runtime config.

### Prove performance-sensitive query/indexer changes

Representative notes:

- !1930: potential full table scan needed production ClickHouse verification.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1930#note_3508186406>
- !1912: attach query EXPLAIN plans.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1912#note_3504031550>
- !1910: confirm queries work.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1910#note_3499212992>
- !1879: concurrency/semaphore behavior needed explanation.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1879#note_3490904654>

Classification: some actioned with production timings or explanations; some were accepted as follow-up/clarification.

Enforcement: prose-only. Whether to run EXPLAIN, benchmark, or prod SQL depends on the changed query and environment access. A generic grep or lint would create noise.

### Prefer typed/named structures over tuples/constants

Representative notes:

- !1910: use a struct so types are understandable.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1910#note_3499349695>
- !1892: consider a typed struct.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1892#note_3495036481>
- !1912: centralize a constant and consider DTO/type-safe query framework.
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1912#note_3500336794>
  - <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1912#note_3500273559>

Classification: several were actioned, for example !1910 returned a named `TopLevelSplit`. Others were larger design suggestions.

Enforcement: no honest lint. Rust lints can catch some complexity or type issues, but not whether a tuple should become a domain struct.

### Add code-graph edge-case fixtures with new language behavior

Representative notes in !1903:

- <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1903#note_3498369812>
- <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1903#note_3498807283>
- <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1903#note_3498814991>
- <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1903#note_3498822808>

Classification: actioned; the author added Scala tests and edge-case support.

Enforcement: this was concentrated in one new-language MR. I did not inflate it into a recurring defect. Existing code-graph guidance already asks for benchmark entries and fixture repos when adding a language.

## Substantive MRs retained by the helper

- !1930: feat(ontology): source MERGED_AT_COMMIT from merged_commit_sha FK (author: `dgruzd`)
- !1943: docs: add click-through demo link to index (author: `iganbaruch`)
- !1923: docs(remote): add Orbit permissions section with role requirements (author: `michaelangeloio`)
- !1938: fix(config): name required by Helm chart configmap (author: `michaelangeloio`)
- !1934: feat(indexer): entity-level incremental SDLC dispatch (author: `jgdoyon1`)
- !1929: refactor(indexer): unify retry/backoff loops behind one engine harness (author: `michaelusa`)
- !1928: refactor(config): rename `default.yaml` to `example.yaml` (author: `michaelangeloio`)
- !1906: docs(i18n): [Translation] Update ja-jp for commit 600c7593 (author: `laurenbarker`)
- !1927: docs(local-dev): align fresh-droid GKG setup with GDK (author: `dgruzd`)
- !1919: feat(indexer): dead-letter timed-out jobs, retry transient write failures (author: `michaelusa`)
- !1918: feat(code-graph): record file size_bytes on gl_file (author: `michaelusa`)
- !1917: perf(indexer): bound per-repo disk to 2GB, fix partial-extraction leak, raise concurrency (author: `michaelusa`)
- !1912: feat(indexer): dispatch namespace changes incrementally (author: `jgdoyon1`)
- !1922: fix(indexer): reclaim inflight slot on dropped code-index commit (author: `dgruzd`)
- !1924: docs(skills): expand remove-llm-comments with tighten/de-dup/redundancy cases (author: `dgruzd`)
- !1897: feat(query-engine): surface valid candidates in validator errors (author: `dgruzd`)
- !1899: chore(code-graph): deny bare allow attributes without reason (author: `dgruzd`)
- !1911: feat(indexer): bin-pack small backfill code jobs into a shared writer (author: `michaelusa`)
- !1915: fix(code-graph): resolve Python re-export imports to definitions (author: `michaelangeloio`)
- !1903: feat(code-graph): add Scala language support (Phase 1 - definitions) (author: `vivekshukl007`)
- !1913: perf(query): scope a query's authorization filter to its resolved namespace (author: `michaelangeloio`)
- !1914: chore: prune low-value comments across the codebase (author: `michaelangeloio`)
- !1910: fix(indexer): scope coverage telemetry to top-level namespaces and log gate skips (author: `michaelangeloio`)
- !1908: fix(indexer): exclude non-top-level namespaces from migration completion gate (author: `michaelangeloio`)
- !1904: feat(ontology): add reindex source metadata (author: `jgdoyon1`)
- !1901: docs: add data retention and deletion section to how-it-works (author: `michaelangeloio`)
- !713: docs(adr): add orbit monetization engineering (ADR 007) (author: `michaelangeloio`)
- !1766: docs(cookbook): add fetch source code recipes for File.content and Definition.content (author: `koves`)
- !1898: docs(agents): add ontology single-source-of-truth and generic-hook rules (author: `dgruzd`)
- !1832: chore(ci): add warning-mode narration + MR-description lint gates (author: `dgruzd`)
- !1890: fix(code-graph): resolve Python source-root imports (author: `michaelangeloio`)
- !1892: feat(indexer): support targeted SDLC indexing requests (author: `jgdoyon1`)
- !1886: refactor(indexer): collapse Destination to ClickHouseWriter (author: `michaelusa`)
- !1885: perf(indexer): skip soft-deleted source rows on full SDLC re-index (author: `jgdoyon1`)
- !1879: perf(indexer): stream code-graph writes to bound indexer memory (author: `michaelusa`)
- !1875: fix(server-config): truncate sub-second precision in cron schedule to prevent drift (author: `dgruzd`)
- !333: fix(ci): add build-proto-gem dependency to semantic-release (author: `michaelangeloio`)
- !1725: feat(code-graph): extract Java sealed-type permits clauses into graph edges (author: `fongse`)
- !1871: test(docs): validate Orbit examples (author: `michaelangeloio`)
- !1868: feat(orbit-local): add --db flag to orbit index (author: `michaelusa`)
- !1817: feat(code-graph): record per-file skip/fault reason on gl_file (author: `michaelusa`)
- !1849: fix(ontology): emit REOPENED edges from resource_state_events (author: `dgruzd`)
