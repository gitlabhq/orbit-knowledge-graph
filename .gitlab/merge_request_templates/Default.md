<!--
TEMPLATE CONVENTION — read before filling this out

The top sections are for a REVIEWER skimming in 30 seconds: what changed, why,
how it was verified. They must read as plain prose.

ALL implementation mechanics — function names, type names, constants, encoder
details, wire-format traces, file-by-file walkthroughs, alternatives
considered, full benchmark tables, raw logs, agent reasoning — go in the Agent
context block at the bottom. Never above it.

Agents: if you feel the urge to write a wall of text, write it inside the
Agent context block. The top sections stay terse.
-->

### What does this MR do and why?

<!--
HARD LIMITS for this section:
  - At most 80 words.
  - At most 3 inline `code` spans.
  - NO bare function/type/constant names in prose (no foo_bar, Foo::bar,
    do_thing(), CONST_NAME). If you need them, you are writing for the wrong
    section — move it to Agent context.

Write 2-3 plain sentences: the operator/user-visible effect, and why. State the
symptom that motivated it, not the code path that implements it. Keep this
updated as discussion happens.

Example of the RIGHT level:
  "Indexing froze when a large batch produced an over-long request URL that the
   HTTP layer rejected. This guards the URL length at the single point every
   datalake lookup passes through, so an oversized batch is split instead of
   wedging the pipeline."
-->

### Related Issues

<!--
Does this MR close or contribute to any issues/epics? `Closes #N` or
`Relates to #N`. Omit if trivial.
-->

### Testing

<!--
How did you verify the change? One or two lines plus a CI job link is
usually enough. Full test transcripts and exploratory notes go in the Agent
context block.
-->

### Performance Analysis

<!--
The headline result and any regression risk. Full flamegraphs, profiler
output, and benchmark tables go in the Agent context block — link or
summarize them here.
-->

- [ ] This merge request does not introduce any performance regression. If a performance regression is expected, explain why.

<details>
<summary><b>Agent context</b> — long-form analysis, file-by-file walkthroughs, profiler output, alternatives considered</summary>

<!--
Agents: put extended reasoning here. File-by-file walkthroughs, full
benchmark tables, raw profiler output, dataflow narratives, alternatives
considered, and anything else that would bury the sections above belongs in
this block.
-->

%{all_commits}

</details>

/label ~"devops::analytics" ~"section::analytics" ~"knowledge graph"

/assign me
<!--
/request_review @jgdoyon1 @michaelangeloio @michaelusa @bohdanpk
-->
