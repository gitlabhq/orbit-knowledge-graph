---
name: pajamas
description: >
  GitLab Pajamas Design System expert for building UIs with Pajamas components
  and patterns. Use when: (1) implementing UI that should follow GitLab's Pajamas
  design system, (2) selecting or configuring Pajamas/GlComponent components
  (GlButton, GlAlert, GlModal, etc.), (3) translating Figma designs into
  Pajamas-compliant code, (4) questions about Pajamas component usage, variants,
  categories, or accessibility, (5) building GitLab-style interfaces, or
  (6) the user mentions "Pajamas", "GitLab UI", "Gl components", or
  "design system" in a GitLab context. Works hand-in-hand with the
  implement-design skill and Figma MCP tools.
---

# Pajamas Design System

Provides expertise for implementing UIs using GitLab's Pajamas Design System, drawing from the single source of truth (SSoT) documentation and design token definitions bundled in this skill.

## INITIALIZATION — Run Before Anything Else

**Before answering any question or performing any task**, check whether the local reference files exist. If the `references/pajamas-docs/` directory is missing or empty, you **MUST** populate it by running the following commands from this skills directory:

```bash
bash scripts/update-refs.sh
python3 scripts/build-index.py
python3 scripts/build-token-map.py
```

These scripts clone the upstream Pajamas repository, extract docs and design tokens into `references/`, and build the component index and token map. The `references/` directory is gitignored — it is always populated locally via these scripts.

**Do not skip this step.** The reference files are required for all component lookups, token resolution, and design guidance below.

## Project Scaffold — Check Peer Dependencies First

Before creating or scaffolding a new project, **always check the `peerDependencies` of `@gitlab/ui`** to determine compatible framework versions. Then pick a project scaffold that satisfies those constraints.

1. **Read `peerDependencies`**: Run `npm info @gitlab/ui peerDependencies --json` (or inspect the `package.json` in the installed package) to discover required Vue version, etc.
2. **Pick a compatible scaffold**: Choose a project template / generator that matches the required Vue version and build toolchain. For example, if `@gitlab/ui` requires `vue@^2.7`, do **not** scaffold a Vue 3 / Vite project — use a Vue 2.7-compatible setup instead.
3. **Verify after install**: After scaffolding, confirm there are no `ERESOLVE` or peer-dependency warnings. Resolve any conflicts before writing component code.

## CRITICAL RULE: Always Use Design Tokens — NEVER Use Absolute Values

**NEVER write absolute/hardcoded values for colors, spacing, typography, borders, shadows, or any visual property.** Always use Pajamas design tokens instead.

This applies to ALL output — CSS, SCSS, HTML attributes, inline styles, utility classes. No exceptions.

- **NEVER** write `color: #1f75cb` — use `var(--gl-color-blue-500)` or a semantic token
- **NEVER** write `padding: 16px` — use a spacing token via utility class or CSS variable
- **NEVER** write `font-size: 14px` — use `var(--gl-font-size-200)` or equivalent
- **NEVER** write `border-radius: 4px` — use `var(--gl-border-radius-base)` or equivalent
- **NEVER** write `background: white` or `color: black` — use `var(--gl-background-color-default)` and `var(--gl-text-color-default)`

### When encountering absolute values

When you see an absolute value in a Figma design, existing code, or user request:

1. **Search the token map**: Read [references/design-tokens/token-map.md](references/design-tokens/token-map.md) and search for the exact value
2. **If exact match found**: Use that token
3. **If no exact match**: Find the closest value in the same category (e.g., closest color in the same hue, closest spacing value) and use that token
4. **Prefer semantic over constant tokens**: Use `--gl-text-color-subtle` (semantic) over `--gl-color-neutral-500` (constant) — semantic tokens adapt across color modes (dark mode, high contrast)

### Token usage priority (in order of preference)

1. **Pajamas components** — use `GlButton`, `GlAlert`, etc. which apply tokens automatically
2. **CSS utility classes** — `gl-bg-subtle`, `gl-text-default`, `gl-p-3`, `gl-rounded-base`
3. **CSS custom properties** — `var(--gl-background-color-default)` when utilities don't cover the case
4. **NEVER raw values** — no hex codes, no pixel values, no named colors

### Token reference files

| File | Use for |
|------|---------|
| [references/design-tokens/token-map.md](references/design-tokens/token-map.md) | **Primary lookup**: reverse-maps absolute values to tokens, forward-maps tokens to values |
| [references/design-tokens/tokens.css](references/design-tokens/tokens.css) | Full CSS custom properties (light mode resolved values) |
| [references/design-tokens/tokens.dark.css](references/design-tokens/tokens.dark.css) | Dark mode resolved values |
| `references/design-tokens/source/semantic/` | Semantic token definitions with `$description` and dark mode mappings |
| `references/design-tokens/source/contextual/` | Component-specific token definitions |
| `references/design-tokens/source/constant/` | Base constant values (color ramps, spacing scales) |
| [references/pajamas-docs/product-foundations/design-tokens.md](references/pajamas-docs/product-foundations/design-tokens.md) | Conceptual overview: categories (constant/semantic/contextual), concepts (action, control, feedback, status, highlight) |
| [references/pajamas-docs/product-foundations/design-tokens-using.md](references/pajamas-docs/product-foundations/design-tokens-using.md) | Usage guide: components vs. utilities vs. CSS custom properties, dark mode patterns |

## SSoT References

The authoritative Pajamas docs live at `references/pajamas-docs/` within this skill, mirroring the `contents/` directory of the upstream repo. Design token definitions live at `references/design-tokens/`. These files are **not committed to git** — they are populated locally by running the initialization scripts (see above).

- **Upstream**: `git@gitlab.com:gitlab-org/gitlab-services/design.gitlab.com.git` (branch: `main`)
  - Docs: `contents/`
  - Tokens: `packages/gitlab-ui/src/tokens/`
- **Populate / update**: Run `bash scripts/update-refs.sh` then `python3 scripts/build-index.py && python3 scripts/build-token-map.py`

## Component Lookup Workflow

1. **Consult the index**: Read [references/component-index.md](references/component-index.md) to find the relevant component, pattern, or foundation by name
2. **Read the SSoT doc**: Load the full doc from `references/pajamas-docs/{reference_file}` (e.g., `references/pajamas-docs/components/button.md`) for detailed usage, variants, props, examples, and accessibility guidance
3. **Check related components**: Each doc's YAML frontmatter includes a `related:` list — review related components when choosing between alternatives
4. **Resolve tokens**: For every visual property, look up the correct design token in [references/design-tokens/token-map.md](references/design-tokens/token-map.md) — never output an absolute value

When searching for a component by function rather than name, grep the component-index.md descriptions or search the `references/pajamas-docs/` directory.

## CRITICAL RULE: Use Real Components — NEVER Reimplement With Raw HTML/CSS

**If a Code Connect mapping exists for a Figma node, you MUST use that component. Do not reimplement it with raw HTML/CSS.**

When matching a Figma element to a Pajamas component, follow this lookup chain in order:

1. **Code Connect mapping** — If the Figma node has a Code Connect mapping to a `@gitlab/ui` component, use it. This is authoritative; no further lookup needed.
2. **Layer names** — Inspect the Figma layer names. If a layer is named after a known component (e.g., "GlButton", "Button", "Avatar"), use the corresponding `@gitlab/ui` component.
3. **Component source** — If layer names are generic, look at the source component in Figma (the main component the instance derives from) to identify the design-system component.
4. **Description match** — If the source is unclear, search [references/component-index.md](references/component-index.md) for the component whose description most closely matches the visual element and behavior.
5. **Ask the user** — If none of the above produce a confident match, ask the user to confirm which component they are using, or whether this is a non-component (custom UI that has no Pajamas equivalent).

Only after exhausting all five steps may you fall back to custom HTML/CSS — and even then, you **must** use Pajamas design tokens (never absolute values).

## Integration with Figma and implement-design

This skill complements the `implement-design` skill. When implementing a Figma design for a GitLab project:

1. **implement-design** fetches design context and screenshots from Figma
2. **This skill** maps the Figma output to the correct Pajamas components and design tokens
3. Together they produce pixel-perfect, design-system-compliant code

During **Step 5** (Translate to Project Conventions) of the implement-design workflow:

- Identify Pajamas components that match each Figma element
- Look up the SSoT doc for each component to get correct props, variants, and categories
- **Replace every absolute value from Figma** (hex colors, px sizes, font specs) with the corresponding Pajamas design token — consult token-map.md for reverse lookups
- Follow Pajamas patterns for form layouts, navigation, feedback, etc.

## Key Pajamas Conventions

### Component import reference

The GitLab Pajamas component library is published as **`@gitlab/ui`**. It requires **Vue 2.7**.

Key components (non-exhaustive):

| Import | Purpose |
|--------|---------|
| `GlButton` | Actions and navigation triggers |
| `GlCheckbox` | Boolean form input |
| `GlAvatar` | User / project identity |
| `GlSearchBoxByType` | Inline search-as-you-type |
| `GlBreadcrumb` | Hierarchical navigation |
| `GlAlert` | System feedback messages |
| `GlModal` | Confirmation / dialog overlays |
| `GlDrawer` | Contextual side panel |
| `GlTabs` | Tabbed content areas |
| `GlTable` | Data rows and columns |
| `GlToggle` | On/off switches |
| `GlBadge` | Status metadata |
| `GlLabel` | Categorization |
| `GlLink` | Navigation to URL |

All imports come from `@gitlab/ui`:

```js
import { GlButton, GlCheckbox, GlAvatar } from '@gitlab/ui';
```

For the full catalog see [references/component-index.md](references/component-index.md).

### Component naming

- **Vue components**: `Gl` prefix — `GlButton`, `GlAlert`, `GlModal`, `GlDrawer`
- **HTML tags** (in templates): `<gl-button>`, `<gl-alert>`, `<gl-modal>`
- **Ruby/HAML**: `Pajamas::ComponentName` — `Pajamas::ButtonComponent`

### Common component API patterns

- **category**: `primary`, `secondary`, `tertiary` (visual prominence)
- **variant**: `default`, `confirm`, `danger` (semantic intent), or `info`, `warning`, `success`, `danger` (feedback)
- **size**: `small`, `medium` (default)
- **block**: Boolean, makes component full-width
- **disabled**, **loading**, **selected**: State booleans

### Utility classes

Pajamas uses `gl-` prefixed utility classes (e.g., `gl-mt-3`, `gl-mb-5`, `gl-flex`). These map to Pajamas design tokens for spacing, color, and layout. Always use these instead of raw Tailwind or custom CSS.

Using color scale values directly like `.gl-text-green-900` or `.gl-bg-white` is **deprecated**. Use semantic utilities: `.gl-text-subtle`, `.gl-bg-default`.

## Component Selection Guide

When deciding which component to use:

| Need | Component | SSoT Reference |
|------|-----------|----------------|
| Trigger an action | `GlButton` | components/button.md |
| System feedback message | `GlAlert` | components/alert.md |
| Temporary notification | `GlToast` | components/toast.md |
| Confirm destructive action | `GlModal` | components/modal.md |
| Contextual side panel | `GlDrawer` | components/drawer.md |
| Boolean input | `GlCheckbox` | components/checkbox.md |
| Single choice from group | Radio button | components/radio-button.md |
| Text entry | `GlFormInput` / `GlTextarea` | components/text-input.md |
| Select from list | `GlCombobox` | components/dropdown-combobox.md |
| Menu of actions | Disclosure | components/dropdown-disclosure.md |
| Tabbed content | `GlTabs` | components/tabs.md |
| Data in rows/columns | `GlTable` | components/table.md |
| Show/hide content | `GlAccordion` / `GlCollapse` | components/accordion.md |
| Navigate hierarchy | `GlBreadcrumb` | components/breadcrumb.md |
| Page-level navigation | Pagination | components/pagination.md |
| User identity | `GlAvatar` | components/avatar.md |
| Status metadata | `GlBadge` | components/badge.md |
| Additional info on hover | `GlTooltip` / `GlPopover` | components/tooltip.md |
| Loading state | `GlSkeletonLoader` / `GlSpinner` | components/skeleton-loader.md |
| Date selection | Date picker | components/date-picker.md |
| Categorize objects | `GlLabel` | components/label.md |
| Navigate to URL | `GlLink` | components/link.md |
| Process steps | `GlPath` / Stepper | components/path.md |
| On/off toggle | `GlToggle` | components/toggle.md |
| Search | Search component | components/search.md |
| Filter content | Filter | components/filter.md |
| Segmented options | Segmented control | components/segmented-control.md |

SSoT references in the table above are relative to `references/pajamas-docs/`. For the full catalog, see [references/component-index.md](references/component-index.md).

## Patterns

Pajamas defines higher-level interaction patterns. Always check the relevant pattern doc when implementing:

| Pattern | When to reference | SSoT Reference |
|---------|-------------------|----------------|
| Forms | Any form layout | patterns/forms.md |
| Empty states | No-data views | patterns/empty-states.md |
| Loading | Async content | patterns/loading.md |
| Saving & feedback | After mutations | patterns/saving-and-feedback.md |
| Destructive actions | Delete/remove flows | patterns/destructive-actions.md |
| Navigation sidebar | App navigation | patterns/navigation-sidebar.md |
| Notifications | System alerts | patterns/notifications.md |
| Filtering | List/table filters | patterns/filtering.md |
| Search | Search UX | patterns/search.md |
| Feature discovery | New feature callouts | patterns/feature-discovery.md |
| AI interactions | Duo/AI features | patterns/duo-chat.md |

SSoT references above are relative to `references/pajamas-docs/`.

## Accessibility

Every Pajamas implementation must meet WCAG AA. Key rules:
- Always provide `aria-label` on icon-only buttons
- Ensure sufficient color contrast (check with foundation docs)
- Support keyboard navigation for all interactive elements
- Use semantic HTML elements
- Read `references/pajamas-docs/accessibility/a11y.md` for comprehensive guidelines

## Updating References

Pull the latest docs and token definitions from upstream, then regenerate indexes:

```bash
bash scripts/update-refs.sh
python3 scripts/build-index.py
python3 scripts/build-token-map.py
```
