---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Give your agent a graph of your GitLab data and code, so the agent can answer questions about changes, pipelines, security, and code structure.
title: GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) for GitLab Self-Managed in GitLab 19.2.2.
- [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.

{{< /history >}}

<section class="product-landing product-hero">

<p class="product-hero-brand"><img src="img/orbit-logo-v19_6.svg" alt="GitLab Orbit logo" width="48" height="48"> GitLab Orbit</p>

Software lifecycle context graph for AI agents
{.product-hero-title}

GitLab Orbit indexes your GitLab data and code into a graph that your agent can query.
Ask what breaks if you change a service, who should review a file, or why pipelines fail.
Your agent answers with a few graph queries in place of many file reads and API calls.
{.product-hero-lead}

[Quickstart](quickstart.md) [{{< icon name="play" >}} Watch the deep dive video](https://www.youtube.com/watch?v=wddpf0rDGnA)
{.product-hero-actions}

<!-- Video published on 2026-06-26 -->

</section>

- [Ask across code and lifecycle](prompt-examples.md)<br>One graph holds your projects, merge requests, pipelines, vulnerabilities, files, and definitions, so one question can cross all of them.
- [Run one query in place of many API calls](how-it-works.md)<br>Your agent sends one graph query and gets back rows, not pages of JSON to filter.
- [Get only the data that you can access](security.md#how-gitlab-enforces-access)<br>GitLab Orbit filters every answer by your GitLab permissions, on GitLab.com, GitLab Self-Managed, and GitLab Dedicated.
{.product-cards}

## What you can ask

- [Find out how agent-written code performs](prompt-examples.md#find-out-how-agent-written-code-performs)<br>`Using GitLab Orbit, how many merge requests did agents create in <my-group>/<my-project> this month, how many merged, and how many reached production?`
- [See what shipped to production this week](prompt-examples.md#see-what-shipped-to-production-this-week)<br>`Using GitLab Orbit, which issues and bugs did the merge requests deployed to <production> this week close?`
- [Change a class hierarchy safely](prompt-examples.md#change-a-class-hierarchy-safely)<br>`Using GitLab Orbit, which classes inherit from <BasePolicy>, and which open merge requests touch them right now?`
- [Unblock a release](prompt-examples.md#unblock-a-release)<br>`Using GitLab Orbit, which open merge requests in milestone <19.6> have a failed pipeline, and who owns them?`
- [Find the right reviewer for a file](prompt-examples.md#find-the-right-reviewer-for-a-file)<br>`Using GitLab Orbit, who approved changes to <app/models/ability.rb> in the last six months, and what did reviewers warn about?`
- [Find a shared CI/CD job that fails across the group](prompt-examples.md#find-a-shared-cicd-job-that-fails-across-the-group)<br>`Using GitLab Orbit, which projects in <my-group> failed the <docs-lint> job this week?`

## See an answer

1. Ask your agent:

   ```plaintext
   Using GitLab Orbit, which projects in <my-group> have the most failed pipelines?
   ```

1. The agent runs one graph query:

   ```gql orbit-query
   MATCH (pl:Pipeline)-[:IN_PROJECT]->(p:Project)
   WHERE pl.status = 'failed' AND p.full_path STARTS WITH 'my-group/'
   RETURN p.full_path, count(pl) AS failed_pipelines
   ORDER BY failed_pipelines DESC
   LIMIT 3
   ```

1. The agent answers from the rows.
   The answer is similar to:

   ```plaintext
   1. my-group/payments-api: 47 failed pipelines
   2. my-group/web-frontend: 31 failed pipelines
   3. my-group/auth-service: 12 failed pipelines
   ```

## The query language

GitLab Orbit queries use [GQL](queries/query-language.md), a graph query language that large language models
already know from their training data.
A short GQL pattern describes connections, for example from a merge request to its pipelines and to the files
that it changed.

Your agent reads the [schema](queries/query-language.md#discover-the-schema), then writes its own query for each question.
A retrieval-augmented generation (RAG) system gives the agent only the text that a search selected.
With GitLab Orbit, the agent selects the exact data that it needs.
One query replaces a chain of API calls, so the agent puts less load on your GitLab instance and makes fewer errors.

## Connect your agent

To get GitLab Orbit, [contact sales](https://about.gitlab.com/sales/).
On GitLab.com, you can also [start a free trial](trial.md).
For information about GitLab Orbit pricing and usage, see [GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/).

For agents that run shell commands, the GitLab Orbit CLI is the recommended method.

{{< tabs >}}

{{< tab title="Claude Code, Codex, OpenCode" >}}

Sign in with `glab auth login`, then run one command in a Git repository:

```shell
glab orbit setup
```

`glab orbit setup` tells your agent to search code with the graph and installs the GitLab Orbit skill,
which teaches your agent to query your GitLab data.
For the full steps, see the [quickstart](quickstart.md#step-2-sign-in-and-set-up-your-agent).
For the setup options, see [connect your agent](agents/_index.md).

{{< /tab >}}

{{< tab title="GitLab Duo" >}}

GitLab Duo Agent Platform has GitLab Orbit built in.
You do not install anything.
Agents call the GitLab Orbit tools when the graph can answer a question.
For more information, see [GitLab Orbit in GitLab Duo](agents/duo.md).

{{< /tab >}}

{{< tab title="MCP" >}}

If your agent supports the Model Context Protocol (MCP) and cannot run shell commands, add the GitLab MCP endpoint.
For example, in Claude Code:

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

On GitLab Self-Managed and GitLab Dedicated, replace `gitlab.com` with the hostname of your instance.
For other clients, see [GitLab MCP endpoint](agents/mcp.md#gitlab-mcp-endpoint).

{{< /tab >}}

{{< tab title="REST API" >}}

To build a script, a scheduled report, or a custom tool on the graph, query it over HTTP.
For the endpoints, authentication, and examples, see the [REST API](queries/api.md).

{{< /tab >}}

{{< /tabs >}}

## Code on your machine

The GitLab Orbit CLI also indexes repositories on your machine.
Your agent can then search code that you have not pushed, offline and with no GitLab account.
For more information, see [local graph](cli.md#local-graph).

## Learn more

- [Watch the deep dive video](https://www.youtube.com/watch?v=wddpf0rDGnA)<br>GitLab Orbit Technical Deep Dive: The AI Context Graph Layer, on the GitLab YouTube channel.
- [Read the launch blog post](https://about.gitlab.com/blog/introducing-gitlab-orbit/)<br>Introducing GitLab Orbit, from the GitLab blog.
- [See the roadmap](https://gitlab-org.gitlab.io/orbit/roadmap/)<br>The work that the GitLab Orbit team plans next, on the public GitLab Orbit roadmap.
{.product-cards .product-cards-2}

<!-- Video published on 2026-06-26 -->

## Reference

{{< cards >}}

- [GitLab Orbit CLI](cli.md)
- [GitLab Orbit dashboard](dashboard.md)
- [Indexed data](schema.md)
- [Queries](queries/_index.md)
- [Security](security.md)
- [GitLab Self-Managed](self-managed/_index.md)
- [Glossary](glossary.md)
- [Troubleshooting](troubleshooting.md)

{{< /cards >}}

<style>
/* Product landing page. Visual wrappers only, with GitLab UI design tokens; the content above is Markdown. */
body:has(.product-landing) h1#skipTarget {
  position: absolute;
  width: 1px;
  height: 1px;
  margin: 0;
  overflow: hidden;
  clip-path: inset(50%);
  white-space: nowrap;
}
body:has(.product-landing) .docs-content .language-plaintext {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

/* Hero: a bordered panel on the subtle surface, like the availability box and code blocks. */
.docs-content .product-hero {
  margin: var(--gl-spacing-scale-5) 0 var(--gl-spacing-scale-6);
  padding: var(--gl-spacing-scale-6);
  border: 1px solid var(--gl-border-color-default);
  border-radius: var(--gl-border-radius-lg);
  background: var(--gl-background-color-subtle);
}
.docs-content .product-hero p { margin: 0; }
.docs-content .product-hero img {
  border: 0;
  padding: 0;
  border-radius: 0;
}
.docs-content .product-hero-brand {
  display: flex;
  align-items: center;
  gap: var(--gl-spacing-scale-4);
  font-size: var(--gl-text-size-h1);
  line-height: 1.15;
  font-weight: 600;
  color: var(--gl-text-color-heading);
}
.docs-content .product-hero-title {
  margin-top: var(--gl-spacing-scale-4) !important;
  font-size: var(--gl-text-size-h3);
  line-height: 1.25;
  font-weight: 500;
  color: var(--gl-text-color-default);
}
.docs-content .product-hero-lead {
  margin-top: var(--gl-spacing-scale-4) !important;
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
@media (max-width: 640px) {
  .docs-content .product-hero { padding: var(--gl-spacing-scale-5); }
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
