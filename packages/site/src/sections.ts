export const HOME = { title: "GitLab Orbit", description: "What GitLab Orbit is, its roadmap, and its developer docs." };

export type Tab = { route: string; label: string; menu?: { route: string; label: string; description: string }[] };

export const TABS: Tab[] = [
  { route: "about", label: "About" },
  {
    route: "engineering",
    label: "Developers",
    menu: [
      { route: "engineering", label: "Contribute", description: "Set up Orbit and open your first merge request" },
      { route: "engineering/development", label: "Development", description: "Run Orbit locally and extend it" },
      { route: "engineering/architecture", label: "Architecture", description: "The design contracts of each component" },
      { route: "engineering/adr", label: "ADRs", description: "Architecture decision records" },
      { route: "engineering/testing", label: "Testing", description: "How Orbit is tested" },
      { route: "team", label: "Team", description: "Who builds Orbit, and how we work" },
    ],
  },
];

export const USER_DOCS = { label: "Docs", href: "https://docs.gitlab.com/orbit/" };

export const PAGES_DIR = "docs/pages";

export const SECTIONS = [
  { route: "about", label: "About" },
  { route: "roadmap", label: "Roadmap" },
  { route: "team", label: "Team" },
  { route: "engineering/development", label: "Development" },
  { route: "engineering/architecture", label: "Architecture" },
  { route: "engineering/adr", label: "ADRs" },
  { route: "engineering/testing", label: "Testing" },
];

// Pages that stay in the repository but not on the public site: internal operations, billing controls, and
// guides written for coding agents.
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
