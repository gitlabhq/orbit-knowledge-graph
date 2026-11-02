---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Give your agent a graph of your GitLab data and code, so the agent can answer questions about changes, pipelines, security, and code structure.
title: GitLab Orbit
layout: landing
---

Software lifecycle context graph for AI agents
{.product-hero-title}

<section class="product-landing product-hero">

GitLab Orbit indexes your GitLab data and code into a graph.
AI agents query the graph to answer questions about merge requests, pipelines,
vulnerabilities, and code structure.
Results include only the data you have permission to access.
{.product-hero-lead}

[Quickstart](quickstart.md) [{{< icon name="play" >}} Watch the overview video](https://www.youtube.com/watch?v=dKaPp-TWd_o)
{.product-hero-actions}

<!-- Video published on 2026-09-03 -->

</section>

## Set up GitLab Orbit

{{< cards >}}

- [Get started with GitLab Orbit](quickstart.md)
- [Start a free trial](trial.md)
- [Connect your agent](agents/_index.md)
- [Set up GitLab Orbit on GitLab Self-Managed](self-managed/orbit-setup.md)

{{< /cards >}}

## Use GitLab Orbit

{{< cards >}}

- [Ask your agent about your work](prompt-examples.md)
- [Query your GitLab data](queries/_index.md)
- [Use the GitLab Orbit CLI](cli.md)
- [Check indexing and manage groups](dashboard.md)

{{< /cards >}}

## Learn how GitLab Orbit works

{{< cards >}}

- [GitLab Orbit architecture](how-it-works.md)
- [Indexed data](schema.md)
- [Security and permissions](security.md)

{{< /cards >}}

## Reference

{{< cards >}}

- [Query language](queries/query-language.md)
- [Glossary](glossary.md)
- [Troubleshooting](troubleshooting.md)

{{< /cards >}}

## Related topics

- [Introducing GitLab Orbit](https://about.gitlab.com/blog/introducing-gitlab-orbit/) (launch blog post)
- [GitLab Orbit roadmap](https://gitlab-org.gitlab.io/orbit/knowledge-graph/)

<style>
/* Product landing page. Visual wrappers only, with GitLab UI design tokens; the content above is Markdown. */
/* Page title: the standard H1, with the GitLab Orbit logo after it. */
body:has(.product-landing) h1#skipTarget {
  display: flex;
  align-items: center;
  gap: var(--gl-spacing-scale-4);
}
body:has(.product-landing) h1#skipTarget::after {
  content: "";
  flex: none;
  width: 40px;
  height: 40px;
  background: URL("img/orbit-logo-v19_6.svg") center / contain no-repeat;
}
/* Hero: tagline, lead, and actions, with no panel. */
.docs-content .product-hero {
  margin: var(--gl-spacing-scale-5) 0 var(--gl-spacing-scale-6);
}
.docs-content .product-hero p { margin: 0; }
/* Tagline: directly under the page title, smaller than a heading. */
.docs-content .product-hero-title {
  margin: calc(-1 * var(--gl-spacing-scale-4)) 0 var(--gl-spacing-scale-5);
  font-size: 1.125rem;
  line-height: 1.4;
  font-weight: 400;
  color: var(--gl-text-color-subtle);
}
.docs-content .product-hero-lead {
  max-width: 68ch;
}
.docs-content .product-hero-actions {
  display: flex;
  flex-wrap: wrap;
  gap: var(--gl-spacing-scale-3);
  margin: var(--gl-spacing-scale-5) 0 var(--gl-spacing-scale-6) !important;
}
/* Same metrics and tokens as .btn.gl-button.btn-md: 2rem tall, 0.875rem text, 8px radius. */
.docs-content .product-hero-actions a {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: var(--gl-spacing-scale-2);
  min-height: 2rem;
  padding: 0 var(--gl-spacing-scale-4);
  border: 1px solid transparent;
  border-radius: 8px;
  font-size: var(--gl-font-size-md);
  line-height: 1rem;
  font-weight: 400;
  text-decoration: none !important;
  transition: color 0.15s ease-in-out, background-color 0.15s ease-in-out, border-color 0.15s ease-in-out;
}
.docs-content .product-hero-actions a:first-child {
  background-color: var(--gl-button-confirm-primary-background-color-default);
  border-color: var(--gl-button-confirm-primary-border-color-default);
  color: var(--gl-button-confirm-primary-foreground-color-default) !important;
}
.docs-content .product-hero-actions a:first-child:hover {
  background-color: var(--gl-button-confirm-primary-background-color-hover);
  border-color: var(--gl-button-confirm-primary-border-color-hover);
  color: var(--gl-button-confirm-primary-foreground-color-hover) !important;
}
.docs-content .product-hero-actions a:not(:first-child) {
  background-color: var(--gl-button-default-primary-background-color-default);
  border-color: var(--gl-button-default-primary-border-color-default);
  color: var(--gl-button-default-primary-foreground-color-default) !important;
}
.docs-content .product-hero-actions a:not(:first-child):hover {
  background-color: var(--gl-button-default-primary-background-color-hover);
  border-color: var(--gl-button-default-primary-border-color-hover);
  color: var(--gl-button-default-primary-foreground-color-hover) !important;
}
.docs-content .product-hero-actions a span { display: inline-flex; }
.docs-content .product-hero-actions a svg {
  width: 1rem;
  height: 1rem;
  fill: currentColor;
}

</style>
