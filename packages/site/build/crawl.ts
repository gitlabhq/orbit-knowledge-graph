import type { CrawlContent } from "@gitlab-org/orbit-site/vite";
import { questionsJsonLd, questionsMarkdown } from "../src/questions";
import { HOME, USER_DOCS } from "../src/sections";

const SITE = "https://gitlaborbit.dev/";
const DEMO = "https://about.gitlab.com/sales/?contact-topic=request-demo";

const HOME_MARKDOWN = `
## One context layer for all your agents

The page opens with an example chat with an agent that uses GitLab Orbit. Its examples are "What breaks if I merge \`web-app!412\`? Answer with proof.", "Which of our services have CVE-2025-29927, and is a fix open?", and "Who depends on auth-client before I change its API?". For each one, the agent lists the graph queries it ran and answers with proof.

## Higher agent accuracy, lower token cost

An agent that reads one file at a time guesses at what connects. GitLab Orbit gives it the connections.

- **Answers with proof.** Answers follow real edges, not text matches, and come with the query that found them.
- **Questions about your whole org.** Ask about every project in your group, not only the repository the agent has open.
- **Code, and what happened to it.** Merge requests, pipelines, deployments, and findings, linked to the code they touched.

## The context graph for your software lifecycle

Code, merge requests, pipelines, deployments, and vulnerabilities in one graph that stays up to date. A video shows the GitLab Orbit graph explorer with the projects and groups of a GitLab instance.

## Your permissions, on every query

Orbit stores no permission data, and the graph is read-only. A diagram shows the four steps of a query:

1. **Your agent** sends the query with the CLI, MCP, REST, or GitLab Duo.
1. **GitLab** applies your permissions.
1. **The query engine** compiles GQL to SQL.
1. **The graph** runs the query.

## What you can do with Orbit

Each query runs on public gitlab-org projects. Each recipe in the docs has the prompt to use.

- **Review a merge request.** The callers of the code a merge request changes, inside the project and in the tests.
- **Fix a vulnerability everywhere.** Every project with a finding, its severity, and where to fix it next.
- **Change shared code safely.** Every project that imports a package, before you change it.
- **Spot conflicting work.** Other open merge requests that change the same files as yours.

## From a zero-day to a fix in production

Your agent finds the vulnerable code, traces it to production, and ships the fix. An animated graph shows the path: your agent starts at a zero-day package, finds the three files that import it, follows them to the four projects that use them, and ships a fix to the three that run in production. A staging project is not fixed.

## What our users say

- "GitLab Orbit gave us an AI code reviewer that actually understands our codebase, not just the diff in front of it." Ryan Harvey, Head of AI Engineering, Compare the Market. Comments landed in the right place 70% of the time, against 58% with RAG, in [a study of 79 merge requests](https://comparethemarketcareers.com/blog/comparing-context-retrieval-approaches-for-ai-code-review/).
- "We were able to get responses back that used to take us a day or like a half a day. We got back in seven minutes." An engineering team member at a US insurer.
- A routine bug fix cost $70 without Orbit and $15 with it: "Same bug, same developer. Only the context changed." Gartner, "The Mythical Token Month," June 2026.
- "It was a lot more precise than the search bar of GitLab. It saved me, I think, about one hour of time." An application security engineer at a telecom company.
- "The thing that y'all have and nobody else has is the ability to auth into this context graph ecosystem." A site reliability engineer at a digital health company, which stopped its in-house graph project.
- "For the right person, it is really like a killer tool." A platform engineering lead at a healthcare diagnostics company, where 75 repository checks became one question.

## Fewer retries, fewer tokens

With GitLab Orbit, your agent makes fewer retries and fewer API calls. A calculator takes your agent runs per day and your model, and shows what those tokens cost each month, at 150,000 tokens per run, and what 20% to 35% fewer tokens saves.

## Works with your agents

Built into GitLab Duo. Connect your agent with the CLI or MCP:

\`\`\`shell
glab orbit setup
# Sets up Claude Code, Codex, GitLab Duo, OpenCode, or Pi
glab orbit status --full-path my-group/my-project
glab orbit query "CALL db.schema('MergeRequest')"
\`\`\`

## Common questions

${questionsMarkdown()}

## Your agent already has the code. Give it the rest.

Answers from your real GitLab data, not guesses or digging by hand. [Request a demo](${DEMO}), or start with the [quickstart](${USER_DOCS.href}quickstart/).
`;

const NEXT_MARKDOWN = `
## Search all of your code, and every tool around it

The page opens with a code search demo. Its code searches are "Find where an error comes from", "unwrap() in Rust, on all branches", and "A feature flag in Ruby and Vue". Its third-party searches follow an incident or a work item across other tools to the code.

## Code search: every branch, every language

The same search works on any repository you can see, in any file, on any branch.

- **Every branch and commit.** Search every branch and commit, including work that has not merged.
- **Every language.** Text and regex read any file. Structural search parses 20 languages.
- **With file context.** Each match shows the lines around it, its branch, and its project.

## Built for scale

In our testing, we indexed and searched 51,656 open source projects with their branches and history: 1.38 M branches, 44.2 M commits, 45.7 M files at HEAD, 22.6 M unique files, and a 767 GiB index. Warm queries took about 80 ms for text search on default branches, about 111 ms for structural search in an early test, and about 120 ms for text search on every branch and commit, in a benchmark of 3,351 projects.

## Third-party data: your other tools, next to your code

Orbit finds the path from tool to tool, checks your access in each one, and your agent ships the fix. The tools are GitLab, Linear, Slack, incident.io, and Grafana. An animated diagram shows an example: your agent starts at an incident in incident.io, follows the alert in Grafana to a deploy in GitLab, reads the Slack thread behind it, finds the line of code with code search, and ships the merge request that fixes it. Its log reads: the agent adds a timeout to cache.get, opens \`checkout-api!43\`, the pipeline passes, \`!43\` deploys to production, and p95 is back to 420 ms, which resolves INC-77.

This page contains information related to upcoming products, features, and functionality. It is important to note that the information presented is for informational purposes only. Please do not rely on this information for purchasing or planning purposes. The development, release, and timing of any products, features, or functionality may be subject to change or delay and remain at the sole discretion of GitLab Inc.
`;

const APPLICATION = {
  "@context": "https://schema.org",
  "@type": "SoftwareApplication",
  name: HOME.title,
  description: HOME.description,
  url: SITE,
  applicationCategory: "DeveloperApplication",
  publisher: { "@type": "Organization", name: "GitLab", url: "https://about.gitlab.com/" },
  softwareHelp: { "@type": "CreativeWork", url: USER_DOCS.href },
  sameAs: ["https://about.gitlab.com/gitlab-orbit/", USER_DOCS.href, "https://gitlab.com/gitlab-org/orbit/knowledge-graph", "https://www.npmjs.com/package/@gitlab/orbit"],
};

export const CRAWL: Record<string, CrawlContent> = {
  "": { markdown: HOME_MARKDOWN.trim(), jsonLd: [APPLICATION, questionsJsonLd()] },
  next: { markdown: NEXT_MARKDOWN.trim() },
};
