---
title: Page template
description: The structure and the rules for every Orbit architecture page.
order: 99
authors:
  - michaelangeloio
---

**TL;DR:** An architecture page states the contract of one part of Orbit in
about 500 words. It must stay true after any refactor that keeps the
contract.

## Rules

- Write the design contract: what the part owns, what it guarantees, and
  what it never does.
- Do not name code. No crate, module, file, function, table, metric, config
  key, or command.
- Do not use counts, sizes, or dated numbers. Point to the report or the
  dashboard that holds the current number.
- Keep the reasons for a decision in an ADR, and link to it.
- Keep procedures in a runbook, and keep known gaps in issues.
- Use one PlantUML diagram at most, at the level of components, not code.
- If a refactor could make a sentence false, delete the sentence.

## Structure

Copy this skeleton. Leave out a section that has nothing to say.

````markdown
---
title: <Name>
description: <One sentence for search results.>
order: <Position in the Architecture sidebar.>
authors:
  - <gitlab-username>
---

**TL;DR:** <Three sentences at most: what this part does, and the one
guarantee that matters most.>

## Terms

- <Only terms that a new engineer does not know.>

## Responsibilities

- <What this part owns. Five bullets at most.>
- <What this part does not own. Five bullets at most.>

## Contracts

- <Always and never statements that other parts depend on. Seven bullets
  at most.>

## Failure behavior

- <What happens when a dependency is slow, wrong, or gone. Five bullets at
  most.>

## Trade-offs

- <What the design gives up, and what it gets for it. Links to ADRs.>

## Decisions

- <Links to the ADRs for this part.>
````
