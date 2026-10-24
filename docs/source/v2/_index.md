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
Ask which of your projects a CVE affects, which changes reached production before an incident,
or what a merge request affects outside its diff.
Your agent answers with a few graph queries in place of many file reads and API calls.
{.product-hero-lead}

[Quickstart](quickstart.md) [{{< icon name="play" >}} Watch GitLab Orbit in two minutes](https://www.youtube.com/watch?v=dKaPp-TWd_o)
{.product-hero-actions}

<!-- Video published on 2026-09-03 -->

</section>

- {{< icon name="connected" >}}[Ask across code and lifecycle](prompt-examples.md)<br>One graph connects your GitLab data and code, so one question can cross both.
- {{< icon name="search" >}}[Run one query in place of many API calls](how-it-works.md)<br>Your agent sends one graph query and gets back rows, not pages of JSON to filter.
- {{< icon name="lock" >}}[Get only the data you can access](security.md#how-gitlab-orbit-enforces-access)<br>GitLab Orbit filters every answer by your GitLab permissions.
{.product-cards .product-cards-icons}

## About GitLab Orbit

- <img src="img/orbit-video-how-it-works-v19_6.png" alt="Thumbnail of the GitLab Orbit architecture video." width="640" height="360">[How GitLab Orbit works](https://www.youtube.com/watch?v=YtPc8kOmyi0)<br>The architecture of GitLab Orbit, from indexing to queries.
- <img src="img/orbit-video-deep-dive-v19_6.png" alt="Thumbnail of the GitLab Orbit technical deep dive video." width="640" height="360">[How we built GitLab Orbit](https://www.youtube.com/watch?v=wddpf0rDGnA)<br>A technical deep dive into the context graph.
{.product-cards .product-cards-2 .product-video-cards}

<!-- Videos published on 2026-07-06 and 2026-06-26 -->

- [Read the launch blog post](https://about.gitlab.com/blog/introducing-gitlab-orbit/)<br>Introducing GitLab Orbit, from the GitLab blog.
- [See the roadmap](https://gitlab-org.gitlab.io/orbit/roadmap/)<br>The work the GitLab Orbit team plans next, on the public GitLab Orbit roadmap.
{.product-cards .product-cards-2}

## Questions GitLab Orbit can answer

- {{< icon name="merge-request" >}}[Review a merge request with proof](prompt-examples.md#review-a-merge-request-with-proof)<br>Which code and projects does this merge request affect? Each finding comes with the graph result that shows it.
- {{< icon name="shield" >}}[Fix a vulnerability across every affected project](prompt-examples.md#fix-a-vulnerability-across-every-affected-project)<br>Which projects in my group does this CVE affect, and which fixes reached production?
- {{< icon name="issue-type-incident" >}}[Respond to a production incident](prompt-examples.md#respond-to-a-production-incident)<br>Which changes to the failing component, and to the code that it calls, reached production this week?
- {{< icon name="code" >}}[Change shared code safely](prompt-examples.md#change-shared-code-safely)<br>Which classes inherit from this base class, and what calls the method that I change?
{.product-cards .product-cards-2 .product-cards-icons}

For more examples, see [prompt examples](prompt-examples.md).

## Reference

{{< cards >}}

- [Connect your agent](agents/_index.md)
- [Indexed data](schema.md)
- [Queries](queries/_index.md)
- [Security](security.md)
- [GitLab Self-Managed setup](https://docs.gitlab.com/orbit/self-managed/orbit-setup/ "Install GitLab Orbit on Kubernetes and connect it to GitLab.")
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
  width: 40px;
  height: 40px;
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

/* Value cards: a GitLab icon in a tinted badge above the title. */
.docs-content ul.product-cards-icons > li > span:first-child {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 2.25rem;
  height: 2.25rem;
  margin: 0 auto var(--gl-spacing-scale-4);
  border-radius: var(--gl-border-radius-lg);
  background-color: color-mix(in srgb, currentColor 12%, transparent);
  color: var(--gl-color-purple-600);
}
.gl-dark .docs-content ul.product-cards-icons > li > span:first-child { color: var(--gl-color-purple-300); }
.docs-content ul.product-cards-icons > li > span:first-child svg {
  width: 1.25rem;
  height: 1.25rem;
  fill: currentColor;
}

/* Video cards: the thumbnail fills the top of the card, edge to edge, clipped to the card's rounded corners. */
.docs-content ul.product-video-cards > li { overflow: hidden; }
.docs-content ul.product-video-cards > li > img {
  display: block;
  width: calc(100% + 2.5rem);
  max-width: none;
  margin: -1.25rem -1.25rem var(--gl-spacing-scale-4);
  padding: 0;
  border: 0;
  border-radius: 0;
  aspect-ratio: 16 / 9;
  object-fit: cover;
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
/* Links outside GitLab.com get the site's external-link icon instead of the arrow. A larger offset keeps a gap while the icon sits half a width to the left; it still slides right on hover. */
.docs-content ul.product-cards > li > a[href^="http"]:not([href*="GitLab.com" i])::after {
  left: var(--gl-spacing-scale-3);
}
</style>
