export const HOME = {
  title: "GitLab Orbit",
  description: "GitLab Orbit is one context layer for all your AI agents: your code, merge requests, pipelines, and vulnerabilities in one graph they can query.",
};

export type Tab = { route: string; label: string; menu?: { route: string; label: string; description: string; icon?: string }[] };

export const NEXT_PAGE = {
  title: "Orbit Next",
  description: "What GitLab Orbit is building next: code search across every repository and branch, and data from your other tools. Coming soon.",
};

export const TABS: Tab[] = [
  { route: "about", label: "About" },
  {
    route: "engineering",
    label: "Developers",
    menu: [
      { route: "engineering", label: "Contribute", description: "Set up Orbit and open your first merge request" },
      { route: "engineering/development", label: "Development", description: "Run Orbit locally and extend it", icon: "terminal" },
      { route: "engineering/architecture", label: "Architecture", description: "The design contracts of each component", icon: "diagram" },
      { route: "engineering/adr", label: "ADRs", description: "Architecture decision records", icon: "doc-text" },
      { route: "engineering/testing", label: "Testing", description: "How Orbit is tested", icon: "work-item-test-case" },
      { route: "team", label: "Team", description: "Who builds Orbit, and how we work", icon: "users" },
    ],
  },
  { route: "next", label: "Coming soon" },
];

export const GUIDES = TABS.flatMap((tab) => tab.menu ?? []).filter(({ route }) => route !== "engineering");

export const USER_DOCS = { label: "Docs", href: "https://docs.gitlab.com/orbit/" };

export const LLMS_NOTES = [
  "- GitLab Orbit connects your code, merge requests, pipelines, work items, and vulnerabilities in one graph. Ask what breaks if a service changes, or which CI/CD jobs fail most.",
  "- Each answer shows only the data that your GitLab role lets you see. Orbit stores no permission data, and the graph is read-only.",
  "- The graph of your group needs GitLab Premium or Ultimate with GitLab Orbit purchased: https://about.gitlab.com/gitlab-orbit/",
  "- `glab orbit setup` sets up Claude Code, Codex, GitLab Duo, OpenCode, or Pi.",
  `- This site is the product overview and the developer docs. To use Orbit, see ${USER_DOCS.href}`,
].join("\n");

export const PAGES_DIR = "docs/pages";

export const SECTIONS = [
  { route: "about", label: "About" },
  { route: "team", label: "Team" },
  { route: "engineering/development", label: "Development" },
  { route: "engineering/architecture", label: "Architecture" },
  { route: "engineering/adr", label: "ADRs" },
  { route: "engineering/testing", label: "Testing" },
];

export const OFF_SITE = [
  "engineering/adr/006_dap_integration.md",
  "engineering/adr/007_monetization_engineering.md",
  "engineering/adr/020_billing_sox_scope.md",
  "engineering/development/adding-data-to-the-knowledge-graph.md",
  "engineering/development/agents-crate-map.md",
  "engineering/development/agents-reference-index.md",
  "engineering/development/sox-billing-boundary.md",
];

export const ENGINEERING = SECTIONS.filter(({ route }) => route.startsWith("engineering/"));
