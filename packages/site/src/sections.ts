export const HOME = { title: "GitLab Orbit", description: "The roadmap, content, team, and engineering docs of GitLab Orbit." };

export type Tab = { route: string; label: string; menu?: { route: string; label: string; description: string }[] };

export const TABS: Tab[] = [
  { route: "", label: "Home" },
  { route: "roadmap", label: "Roadmap" },
  {
    route: "content",
    label: "Content",
    menu: [
      { route: "content", label: "Overview", description: "Posts, demos, and talks about Orbit" },
      { route: "content/blog", label: "Blog", description: "Engineering notes and product updates" },
      { route: "content/demos", label: "Demos", description: "Videos and talks about Orbit" },
    ],
  },
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

export const SECTIONS = [
  { route: "roadmap", dir: "docs/roadmap", label: "Roadmap" },
  { route: "content", dir: "docs/content", label: "Content" },
  { route: "team", dir: "docs/team", label: "Team" },
  { route: "engineering/runbooks", dir: "docs/runbooks", label: "Runbooks" },
  { route: "engineering/architecture", dir: "docs/architecture", label: "Architecture" },
  { route: "engineering/adr", dir: "docs/adr", label: "ADRs" },
  { route: "engineering/testing", dir: "docs/testing", label: "Testing" },
];

export const ENGINEERING = SECTIONS.filter(({ route }) => route.startsWith("engineering/"));
