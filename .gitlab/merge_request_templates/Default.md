<!--
TEMPLATE CONVENTION — read before filling this out

Each section below is for the *important* content a reviewer needs: the
headline, the diagram, the screenshot, the one number that proves the fix.
Keep it short.

Long-form output — file-by-file walkthroughs, profiler dumps, alternatives
considered, full benchmark tables, raw logs, agent reasoning — goes in the
<details> block at the bottom, not in the sections above.

Agents: if you feel the urge to write a wall of text, write it inside the
<details> block. The top sections stay terse.
-->

### What does this MR do and why?

<!--
Describe at a high level what your merge request does and why. Keep this
description updated as discussion happens.

Headline first, supporting detail below. Diagrams, screenshots, and the key
numbers belong here. Long narratives belong in the Agent context block.
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
