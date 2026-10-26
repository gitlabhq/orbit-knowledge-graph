export const HOME = { title: "GitLab Orbit", description: "The roadmap, content, team, and engineering docs of GitLab Orbit." };

export type Tab = { route: string; label: string; menu?: { route: string; label: string; description: string }[] };

export const TABS: Tab[] = [
  { route: "", label: "Home" },
  { route: "roadmap", label: "Roadmap" },
  { route: "content", label: "Content" },
  {
    route: "engineering",
    label: "Engineering",
    menu: [
      { route: "engineering", label: "Overview", description: "How the Orbit team builds and runs Orbit" },
      { route: "engineering/runbooks", label: "Runbooks", description: "Operate Orbit and recover from failures" },
      { route: "engineering/architecture", label: "Architecture", description: "The design contracts of each component" },
      { route: "engineering/adr", label: "ADRs", description: "Architecture decision records" },
      { route: "engineering/testing", label: "Testing", description: "How Orbit is tested" },
    ],
  },
  { route: "team", label: "Team" },
];

export const USER_DOCS = { label: "User docs", href: "https://docs.gitlab.com/orbit/" };

export const PAGES_DIR = "docs/pages";

export const SECTIONS = [
  { route: "roadmap", label: "Roadmap" },
  { route: "content", label: "Content" },
  { route: "team", label: "Team" },
  { route: "engineering/runbooks", label: "Runbooks", description: "How to operate Orbit, recover from failures, and restore it after a disaster." },
  { route: "engineering/architecture", label: "Architecture", description: "What Orbit is, why we built it, and the design contracts of each component." },
  { route: "engineering/adr", label: "ADRs", description: "Each design decision, the reasons for it, and its consequences." },
  { route: "engineering/testing", label: "Testing", description: "How Orbit proves that a change is correct before it ships." },
  { route: "engineering/development", label: "Development", description: "How to run Orbit locally, add a language or data source, and contribute." },
];

export const ENGINEERING = SECTIONS.filter(({ route }) => route.startsWith("engineering/"));
