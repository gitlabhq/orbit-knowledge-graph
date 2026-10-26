---
name: orbit-architecture-docs
description: >
  Write and review the Orbit architecture pages in
  docs/pages/engineering/architecture. Covers the page layout, the voice, the
  sentence and diagram rules, fact checks, and the lint and review loop. Use it
  to write, rewrite, restructure, or review a page, or to apply review comments.
allowed-tools: Bash, Read, Edit, Write, Agent
---

# Orbit architecture pages

An architecture page explains one part of Orbit on one page, for engineers. A
reader who has never seen the code should understand how that part works end to
end. The SDLC indexing, code indexing, and schema evolution pages under
`architecture/indexing/` are the reference for everything below.

## Voice

- Write like a person who built the system and explains it to a colleague. Use
  "we" for decisions the team made.
- Keep each paragraph on one train of thought. Each sentence leads to the next
  one. Cut a fact that does not serve the paragraph, or give it its own
  paragraph in the right place.
- Keep the intro plain. "Here is a simplified view of the main areas and how
  they connect" is right. A caption that explains how to read a picture is not.
- Keep the density. A rewritten paragraph is about as long as the one it
  replaces.
- Say GitLab, not Rails or the monolith. Say Orbit, not Knowledge Graph or GKG.
- Name no code: no crate, file, function, table, config key, metric, or
  command. Products and systems are fine: Siphon, NATS, ClickHouse, Gitaly,
  Workhorse.
- Use no dated numbers. A rule like "a 30-day grace period" is fine.

## Page layout

Every process page uses the same sections, in this order:

1. Frontmatter with `title`, `description`, `order`, and `authors`. The
   sidebar sorts pages by `order`. A folder with a `README.md` becomes a
   section of the sidebar.
2. A header section, like `## What gets indexed`. One paragraph, then the
   header image from the `orbit-docs-svg` skill.
3. `## How it works`. One overview diagram, then one paragraph that follows the
   data along the path.
4. One `### N. Step` per step. Each step has one Mermaid diagram, then short
   paragraphs. Leave out the diagram when the step reads well without it.
5. `## When things go wrong`. For an indexing page, link to its section on the
   resiliency page. Elsewhere, write an explicit list: a bold failure, then how
   Orbit handles it.
6. `## Why we built it this way`. The argument for the design, in prose.
7. `## Trade-offs and what's next`, when the page has open problems. One bullet
   each, with a bold lead.
8. `## Decisions`. Links to the ADRs.

A reference page like the resiliency page has no header image.

Prose sections stay prose. Use a list only for a real list: failures,
trade-offs, decisions, or a short set of rules. Do not mix tables, numbered
lists, and bold-label bullets into the explanation.

## Mermaid diagrams

GitLab stretches every Mermaid diagram to the width of the page. A wide diagram
gets smaller text than a narrow one. So keep every diagram narrower than the
page, and they all render at the same scale.

- Use `flowchart LR` with at most five boxes in a row.
- Keep labels to one or two words. Use cylinders for stores and boxes for
  services.
- When a diagram gets too wide, remove a box that the text already covers. Do
  not shrink the font.

## Facts

Every claim must match the code that runs today. Before writing a page, have a
research agent build a fact sheet from the Orbit repository and, when GitLab is
involved, the GitLab repository. Ask it for file and line evidence and for a
list of things the page must not claim. Write only what the code does. Put
plans in "Trade-offs and what's next", and say they are plans.

## Lint and reflow

The prose lint covers these pages. A sentence has at most 25 words, a paragraph
averages at most 20, and dashes and filler words fail. Split a long sentence.
Never widen the gate.

After an edit, rewrap the paragraphs. The script keeps links and code spans on
one line:

```shell
uv run .claude/skills/orbit-architecture-docs/scripts/reflow.py docs/pages/engineering/architecture/indexing/*.md
```

Then run every check on the changed pages:

```shell
F="docs/pages/engineering/architecture/indexing/code-indexing.md"
uv run --quiet scripts/linting/prose_lint.py $F
mise x -- bun x markdownlint-cli2 $F
mise x -- vale --minAlertLevel error $F
mise x -- lychee --offline --include-fragments 'docs/pages/**/*.md'
ORBIT_MARKDOWN_TOKEN="$(glab config get token --host gitlab.com)" mise x -- bun run --filter @orbit/site build
```

The site build renders Markdown through the GitLab API, so it needs a token.
Pass it in the environment only, and never print it.

## Review loop

1. Open the page in gldraft and start a background watch, as the `gldraft`
   skill describes.
2. When the review ends, apply every comment. Remove the CriticMarkup and the
   endmatter.
3. Reflow, lint, and open the page again. Stop when the author approves.

Moving a page breaks links in other pages. Move it with `git mv`, then fix every
link to and from it, and run lychee on all of `docs/pages`.
