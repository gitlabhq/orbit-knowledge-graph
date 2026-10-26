---
name: orbit-docs-svg
description: >
  Draw, compare, and ship the SVG header images of the Orbit architecture
  pages. Use it for a header image, a graph illustration, or a banner, or to
  iterate on how one looks. Covers the visual language and the theme-neutral
  palette. Also covers the build scripts, the previews, and page placement.
allowed-tools: Bash, Read, Edit, Write
---

# Orbit docs SVG images

The architecture pages use one header image each. Every image follows the same
visual language, so a reader can tell at a glance what kind of picture it is.

## Visual language

- **A radial map shows what a graph holds.** The center node is the anchor,
  like Core for the SDLC graph. Domains sit on an ellipse around it.
- **A horizontal banner shows a process.** Steps go from left to right. A fork
  shows two paths that join again, like copy or re-index.
- **A time axis under a banner shows who serves queries.** Gray spans the old
  state, blue spans the new one, and a dot marks the switch.
- Nodes are circles with a `@gitlab/svgs` icon. Each node has a title and up to
  two short lines under it.
- SDLC and code entities use their color from the ontology (`style.color`).
  Process steps use the fixed palette in `scripts/svgkit.py`.
- Keep labels short. A header explains one idea, and the page explains the rest.

## Light and dark themes

GitLab shows a repository image in an `<img>` tag. An SVG in an `<img>` tag
sees only the OS theme, not the GitLab theme. So never use
`prefers-color-scheme` in these files.

- The background stays transparent.
- Text and lines use the mid-gray colors in `scripts/svgkit.py`. They read on
  white and on dark pages.
- Lines are masked behind every label, so a line never crosses text.

## Build the images

The scripts need the `@gitlab/svgs` icon sprite. Use the copy in a GDK checkout,
or download it once into a private folder:

```shell
mkdir -p /tmp/orbit-svg && curl -sfL https://unpkg.com/@gitlab/svgs/dist/icons.svg -o /tmp/orbit-svg/icons.svg
export GITLAB_SVGS_ICONS=/tmp/orbit-svg/icons.svg
```

Build every committed header from the repository root:

```shell
uv run .claude/skills/orbit-docs-svg/scripts/headers.py
```

The output goes to `docs/assets/architecture/`. Pass names to build only some,
for example `headers.py schema-evolution`. The SDLC map reads its colors and
edges from `config/ontology`, so a rebuild follows ontology changes.

## Iterate on a new image

1. Add a function to `scripts/headers.py` that returns `canvas.render(...)`.
   Reuse `labelled`, `arrow`, and `curved` from `scripts/svgkit.py`.
2. Draw two to five variants when the user is still choosing. Each variant
   tries one idea, like where the timeline goes or which layout to use.
3. Write the drafts to a private folder for this session. Other sessions share
   `/tmp`, so never use a generic path like `/tmp/out`.
4. Build a comparison page and the previews:

   ```shell
   uv run .claude/skills/orbit-docs-svg/scripts/preview.py /tmp/orbit-svg/<topic>/*.svg \
     --out /tmp/orbit-svg/<topic>/preview --png --title "Schema header ideas"
   open /tmp/orbit-svg/<topic>/preview/compare.html
   ```

5. Read the PNG previews yourself before you show them. Check for labels that
   touch, lines through text, and empty space.
6. Keep the display width at 720. A banner is about 720 by 300. A radial map
   is about 720 by 400.

## Put the image in a page

- Commit the SVG to `docs/assets/architecture/<page>.svg`.
- Link it with a relative path from the page. From
  `architecture/indexing/` that is `../../../../assets/architecture/<page>.svg`.
- Write alt text that says what the picture shows in plain words.
- gldraft shows local images only through its local asset route. If an image
  does not show in gldraft, open the comparison page instead.
