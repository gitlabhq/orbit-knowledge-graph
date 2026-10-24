---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Give your agent a graph of your GitLab data and code, so the agent can answer questions about changes, pipelines, security, and code structure.
title: GitLab Orbit
---

Software lifecycle context graph for AI agents
{.product-hero-title}

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment) in GitLab 18.10.
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) from experiment to beta in GitLab 19.1.
- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) for GitLab Self-Managed in GitLab 19.2.2.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5.

{{< /history >}}

<section class="product-landing product-hero">

GitLab Orbit indexes your GitLab data and code into a graph your agent can query.
Ask what breaks if you change a service, who should review a file, or why pipelines fail.
Your agent answers with a few graph queries in place of many file reads and API calls.
{.product-hero-lead}

[Quickstart](quickstart.md) [{{< icon name="play" >}} Watch the deep dive video](https://www.youtube.com/watch?v=wddpf0rDGnA)
{.product-hero-actions}

<!-- Video published on 2026-06-26 -->

</section>

- [Ask across code and lifecycle](prompt-examples.md)<br>One graph holds your projects, merge requests, pipelines, vulnerabilities, files, and definitions, so one question can cross all of them.
- [Run one query in place of many API calls](how-it-works.md)<br>Your agent sends one graph query and gets back rows, not pages of JSON to filter.
- [Get only the data you can access](security.md#how-gitlab-orbit-enforces-access)<br>GitLab Orbit filters every answer by your GitLab permissions.
{.product-cards}

## Questions GitLab Orbit can answer

- [Review a merge request with proof](prompt-examples.md#review-a-merge-request-with-proof)<br>Which code and projects does this merge request affect? Each finding comes with the graph result that shows it.
- [Fix a vulnerability across every affected project](prompt-examples.md#fix-a-vulnerability-across-every-affected-project)<br>Which projects in my group does this CVE affect, and which fixes reached production?
- [Respond to a production incident](prompt-examples.md#respond-to-a-production-incident)<br>Which changes to the failing component, and to the code that it calls, reached production this week?
- [Change shared code safely](prompt-examples.md#change-shared-code-safely)<br>Which classes inherit from this base class, and what calls the method that I change?
{.product-cards .product-cards-2}

For more examples, see [prompt examples](prompt-examples.md).

## The query language

GitLab Orbit queries use [GQL](queries/query-language.md), a graph query language that large language models
already know from their training data.
A short GQL pattern describes connections, for example from a merge request to its pipelines and to the files
it changed.

Your agent reads the [schema](queries/query-language.md#discover-the-schema), then writes its own query for each question.
One query replaces a chain of API calls, so the agent puts less load on your GitLab instance and makes fewer errors.

## Connect your agent

To get GitLab Orbit, [contact sales](https://about.gitlab.com/sales/).
On GitLab.com, you can also [start a free trial](trial.md).
For information about GitLab Orbit pricing and usage, see [GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/).

Choose how your agent connects:

- For agents that run shell commands, such as Claude Code, Codex, and OpenCode, use the [GitLab Orbit CLI](agents/_index.md).
  For the steps, see the [quickstart](quickstart.md#use-gitlab-orbit-with-your-agent).
- In GitLab Duo Agent Platform, GitLab Orbit is built in.
  For more information, see [GitLab Orbit in GitLab Duo](agents/duo.md).
- For agents that support the Model Context Protocol (MCP) but cannot run shell commands, use the [GitLab Orbit MCP endpoint](agents/mcp.md#gitlab-orbit-mcp-endpoint).
  If your agent already connects to the GitLab MCP server, [add the GitLab Orbit tools](agents/mcp.md#gitlab-orbit-tools-in-the-gitlab-mcp-server) to that connection.
- For scripts, scheduled reports, and custom tools, use the [GitLab Orbit API](https://docs.gitlab.com/api/orbit/).

## Code on your machine

The GitLab Orbit CLI also indexes repositories on your machine.
Your agent can then search code you have not pushed, offline and with no GitLab account.
For more information, see [local graph](cli.md#local-graph).

## Learn more

- [Read the launch blog post](https://about.gitlab.com/blog/introducing-gitlab-orbit/)<br>Introducing GitLab Orbit, from the GitLab blog.
- [See the roadmap](https://gitlab-org.gitlab.io/orbit/roadmap/)<br>The work the GitLab Orbit team plans next, on the public GitLab Orbit roadmap.
{.product-cards .product-cards-2}

## Reference

{{< cards >}}

- [GitLab Orbit CLI](cli.md)
- [GitLab Orbit dashboard](dashboard.md)
- [Indexed data](schema.md)
- [Queries](queries/_index.md)
- [Security](security.md)
- [GitLab Self-Managed setup](https://docs.gitlab.com/orbit/self-managed/orbit-setup/ "Install GitLab Orbit on Kubernetes, connect GitLab to it, and index your first group.")
- [Glossary](glossary.md)
- [Troubleshooting](troubleshooting.md)

{{< /cards >}}

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
  width: 48px;
  height: 48px;
  background: URL("img/orbit-logo-v19_6.svg") center / contain no-repeat;
}
body:has(.product-landing) .docs-content .language-plaintext {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
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
  margin-top: var(--gl-spacing-scale-5) !important;
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
.docs-content .product-hero-actions a:nth-child(2) {
  background-color: var(--gl-button-default-primary-background-color-default);
  border-color: var(--gl-button-default-primary-border-color-default);
  color: var(--gl-button-default-primary-foreground-color-default) !important;
}
.docs-content .product-hero-actions a:nth-child(2):hover {
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

/* Cards: same metrics as the cards shortcode (.card-body.card.gl-rounded-lg): 1.25rem padding, 8px radius, neutral-100 border, subtle hover. */
.docs-content ul.product-cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(13rem, 1fr));
  gap: 1.25rem;
  margin: 0 0 var(--gl-spacing-scale-6);
  padding: 0;
  list-style: none;
}
.docs-content ul.product-cards-2 { grid-template-columns: repeat(auto-fit, minmax(16rem, 1fr)); }
.docs-content ul.product-cards > li {
  position: relative;
  margin: 0;
  padding: 1.25rem;
  border: 1px solid var(--gl-color-neutral-100);
  border-radius: var(--gl-border-radius-lg);
  background-color: var(--gl-background-color-default);
  color: var(--gl-text-color-default);
  line-height: 1.5rem;
  transition: all 0.2s cubic-bezier(0.22, 0.61, 0.36, 1);
}
.docs-content ul.product-cards > li:hover { background-color: var(--gl-background-color-subtle); }
.docs-content ul.product-cards > li > a {
  display: inline-block;
  padding-bottom: var(--gl-spacing-scale-3);
  font-weight: 600;
  color: var(--gl-text-color-default) !important;
  text-decoration: none !important;
}
.docs-content ul.product-cards > li > a::before {
  content: "";
  position: absolute;
  inset: 0;
  border-radius: inherit;
}
/* Capitalised CSS function on purpose: markdownlint MD044 reads the lowercase form as the proper name URL. */
.docs-content ul.product-cards > li > a::after {
  content: "";
  position: relative;
  top: 0.15rem;
  left: 0.25rem;
  display: inline-block;
  width: 1rem;
  height: 1rem;
  mask: URL("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Cpath d='M10.159%2010.72a.75.75%200%201%200%201.06%201.06l3.25-3.25L15%208l-.53-.53-3.25-3.25a.75.75%200%200%200-1.061%201.06l1.97%201.97H1.75a.75.75%200%201%200%200%201.5h10.379l-1.97%201.97z'/%3E%3C/svg%3E") center no-repeat;
  background-color: transparent;
  translate: -50% 0;
  transition: all 0.2s cubic-bezier(0.22, 0.61, 0.36, 1);
}
.docs-content ul.product-cards > li:hover > a::after {
  background-color: var(--gl-text-color-default);
  translate: 0 0;
}
</style>
