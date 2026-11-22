import { type ComponentProps, useEffect, useState } from "react";
import { AgentLogos, AgentPrompt, cn, Contrasts, CopyButton, CostCalculator, DotWave, Feature, FeatureGrid, FieldBand, Flow, GravityGlobe, Icon, OrbitHalo, OrbitHero, PageFrame, pageHead, type PromptExample, Reveal, RippleField, RippleLog, Section, segment, site, SiteLink, type Slide, Slideshow, Terminal, Testimonial, type UseCase, UseCases } from "@gitlab-org/orbit-site";
import { createFileRoute } from "@tanstack/react-router";
import { QUESTIONS } from "../questions";
import { HOME, USER_DOCS } from "../sections";

export const Route = createFileRoute("/")({
  head: () => pageHead(site.name, HOME.description),
  component: HomePage,
});

const DOCS = USER_DOCS.href.replace(/\/$/, "");
const asset = (path: string) => `${import.meta.env.BASE_URL}${path}`;

const SETUP = `
$ glab orbit setup
# Sets up Claude Code, Codex, GitLab Duo, OpenCode, or Pi
$ glab orbit status --full-path my-group/my-project
$ glab orbit query "CALL db.schema('MergeRequest')"`;

const ZERO_DAY = {
  source: { label: "react-server-dom-webpack", x: 0.12, y: 0.5 },
  nodes: [
    { label: "app/actions.ts", x: 0.38, y: 0.22, quiet: true },
    { label: "ui-kit/server.tsx", x: 0.46, y: 0.42, quiet: true },
    { label: "lib/rsc.ts", x: 0.35, y: 0.82, quiet: true },
    { label: "web-app", x: 0.78, y: 0.15 },
    { label: "admin-console", x: 0.83, y: 0.42, staging: true },
    { label: "checkout-api", x: 0.88, y: 0.66 },
    { label: "mobile-bff", x: 0.75, y: 0.9 },
  ],
  log: [
    { at: 0.05, tool: true, text: "Explored Orbit" },
    { at: 0.22, text: "3 files import it. ui-kit is shared by 3 projects." },
    { at: 0.3, tool: true, text: "Explored Orbit" },
    { at: 0.45, text: "3 of 4 projects run it in production." },
    { at: 0.5, text: "Bumping to 19.2.1, production first." },
    { at: 0.66, tone: "ok" as const, text: "ui-kit!57 merged, released 4.2.1" },
    { at: 0.74, tone: "ok" as const, text: "web-app!412 deployed to production" },
    { at: 0.8, tone: "ok" as const, text: "checkout-api!88 deployed to production" },
    { at: 0.86, tone: "ok" as const, text: "mobile-bff!31 deployed to production" },
  ],
};

// On a phone, the ripple runs top to bottom, so the labels fit: the package, then its files, then the projects.
const ZERO_DAY_NARROW = {
  source: { ...ZERO_DAY.source, x: 0.5, y: 0.06 },
  nodes: [
    { label: "app/actions.ts", x: 0.18, y: 0.44, quiet: true },
    { label: "ui-kit/server.tsx", x: 0.5, y: 0.36, quiet: true },
    { label: "lib/rsc.ts", x: 0.84, y: 0.44, quiet: true },
    { label: "web-app", x: 0.2, y: 0.72 },
    { label: "admin-console", x: 0.74, y: 0.72, staging: true },
    { label: "checkout-api", x: 0.26, y: 0.96 },
    { label: "mobile-bff", x: 0.78, y: 0.96 },
  ],
};

function useNarrow() {
  const [narrow, setNarrow] = useState(false);
  useEffect(() => {
    const query = window.matchMedia("(max-width: 47.99rem)");
    const update = () => setNarrow(query.matches);
    update();
    query.addEventListener("change", update);
    return () => query.removeEventListener("change", update);
  }, []);
  return narrow;
}

const CHAT: PromptExample[] = [
  {
    label: "Review a merge request with proof",
    icon: "merge-request",
    prompt: "What breaks if I merge web-app!412? Answer with proof.",
    calls: [
      { tool: "Read Orbit guide", detail: "orbit" },
      { tool: "Explored Orbit", detail: "MergeRequest → DiffFile → Definition" },
      { tool: "Explored Orbit", detail: "Definition ← CALLS ← Definition" },
      { tool: "Explored Orbit", detail: "ImportedSymbol ← File → Project" },
    ],
    answer: (
      <>
        <p>
          !412 changes the signature of <code>formatPrice</code> in <code>lib/money.ts</code>. It has 14 callers in 3 projects:
        </p>
        <ul>
          <li>
            <code>web-app</code>
            <span>6 callers</span>
          </li>
          <li>
            <code>checkout-api</code>
            <span>5 callers</span>
          </li>
          <li>
            <code>invoices</code>
            <span>3 callers</span>
          </li>
        </ul>
        <p>
          Two of them still pass a string: <code>cart.tsx:88</code> and <code>invoice.ts:41</code>. Fix those before you merge.
        </p>
      </>
    ),
    stats: { apiCallsReplaced: 120, nodes: 38, edges: 52 },
  },
  {
    label: "Trace a CVE",
    icon: "shield",
    prompt: "Which of our services have CVE-2025-29927, and is a fix open?",
    calls: [
      { tool: "Read Orbit guide", detail: "orbit" },
      { tool: "Explored Orbit", detail: "CVE ← Finding ← Vulnerability → Project" },
      { tool: "Explored Orbit", detail: "Vulnerability ← FIXES ← MergeRequest" },
    ],
    answer: (
      <>
        <p>3 services have it. One is fixed, one has a fix in review, and one needs a fix:</p>
        <ul>
          <li>
            <code>web-app</code>
            <span>high · fix in !418</span>
          </li>
          <li>
            <code>admin-console</code>
            <span>high · no fix yet</span>
          </li>
          <li>
            <code>docs-site</code>
            <span>medium · fixed</span>
          </li>
        </ul>
        <p>
          <code>admin-console</code> runs in production. Start there.
        </p>
      </>
    ),
    stats: { apiCallsReplaced: 260, nodes: 21, edges: 24 },
  },
  {
    label: "Find who uses a package",
    icon: "branch",
    prompt: "Who depends on auth-client before I change its API?",
    calls: [
      { tool: "Read Orbit guide", detail: "orbit" },
      { tool: "Explored Orbit", detail: "ImportedSymbol ← File → Project" },
    ],
    answer: (
      <>
        <p>12 projects import it, in 214 files. The most files:</p>
        <ul>
          <li>
            <code>checkout-api</code>
            <span>41 files</span>
          </li>
          <li>
            <code>web-app</code>
            <span>18 files</span>
          </li>
          <li>
            <code>mobile-bff</code>
            <span>9 files</span>
          </li>
        </ul>
        <p>3 more import it only in tests, so a breaking change shows up there first.</p>
      </>
    ),
    stats: { apiCallsReplaced: 900, nodes: 226, edges: 440 },
  },
];

const SETUP_AGENTS = new Set(["Claude Code", "Codex", "OpenCode", "Pi"]);

const setupPrompt = (agent: string | null) => {
  const name = agent ?? "this agent";
  const access = `GitLab Orbit needs GitLab Premium or Ultimate, and it must be turned on for my top-level group. If it is not, show me how: ${DOCS}/quickstart/.`;
  if (agent === "GitLab Duo")
    return `Help me use GitLab Orbit in GitLab Duo, so you can answer questions with the graph of my code and my GitLab data.

Read ${DOCS}/agents/duo/ first, and walk me through turning on GitLab Orbit for GitLab Duo. ${access}

Do not say that it works until GitLab Duo answers a question with GitLab Orbit.`;
  const install = `Install the GitLab Orbit CLI with glab (glab 1.126 or later), and let me sign in and authorize myself.`;
  const setup = !agent
    ? `${install} Then run glab orbit setup for this agent. If glab orbit setup does not list this agent, run glab orbit skills get orbit and follow it instead. If this agent cannot run shell commands at all, connect it to the GitLab Orbit MCP endpoint: ${DOCS}/agents/mcp/.`
    : SETUP_AGENTS.has(agent)
      ? `${install} Then run glab orbit setup for ${agent}. Keep any existing settings.`
      : `${install} ${agent} uses the same CLI from the shell: run glab orbit skills get orbit and follow it, so you know the glab orbit commands.`;
  return `Help me connect GitLab Orbit to ${name}, so you can answer questions with the graph of my code and my GitLab data.

Read the setup guide at ${DOCS}/agents/ first. ${setup}

${access}

Do not say that the connection works until glab orbit status confirms it. Then run /orbit-start. If /orbit-start is not available, run glab orbit skills get orbit-start and follow it.`;
};

const commandOf = (code: string) => code.trim().split(/\n\s*\n/)[0]!.replace(/^\$ /, "");

const USE_CASES: UseCase[] = [
  {
    title: "Review a merge request",
    icon: "merge-request",
    href: `${DOCS}/prompt-examples/#review-a-merge-request-with-proof`,
    text: "The callers of the code a merge request changes, inside the project and in the tests.",
    code: `
$ glab orbit query "
  MATCH (caller:Definition)-[:CALLS]->(d:Definition)
        <-[:DEFINES]-(f:File {project_id: 9438583})
  WHERE f.path = 'v2/log/log.go'
    AND d.fqn = 'log.NewWithConfig'
  RETURN caller.fqn, caller.file_path"`,
    copy: commandOf(`$ glab orbit query "
  MATCH (caller:Definition)-[:CALLS]->(d:Definition)
        <-[:DEFINES]-(f:File {project_id: 9438583})
  WHERE f.path = 'v2/log/log.go'
    AND d.fqn = 'log.NewWithConfig'
  RETURN caller.fqn, caller.file_path"`),
  },
  {
    title: "Fix a vulnerability everywhere",
    icon: "shield",
    href: `${DOCS}/prompt-examples/#fix-a-vulnerability-across-every-affected-project`,
    text: "Every project with a finding, its severity, and where to fix it next.",
    code: `
$ glab orbit query "
  MATCH (v:Vulnerability)-[:HAS_FINDING]->
        (o:VulnerabilityOccurrence)-[:HAS_IDENTIFIER]->
        (i:VulnerabilityIdentifier {external_id: 'CVE-2025-58183'})
  RETURN v.project_id, v.severity"`,
    copy: commandOf(`$ glab orbit query "
  MATCH (v:Vulnerability)-[:HAS_FINDING]->
        (o:VulnerabilityOccurrence)-[:HAS_IDENTIFIER]->
        (i:VulnerabilityIdentifier {external_id: 'CVE-2025-58183'})
  RETURN v.project_id, v.severity"`),
  },
  {
    title: "Change shared code safely",
    icon: "code",
    href: `${DOCS}/prompt-examples/#change-shared-code-safely`,
    text: "Every project that imports a package, before you change it.",
    code: `
$ glab orbit query "
  MATCH (s:ImportedSymbol {import_path:
          'gitlab.com/gitlab-org/labkit/v2/log'})
        <-[:IMPORTS]-(f:File)-[:ON_BRANCH]->(b:Branch)
        <-[:CONTAINS]-(p:Project)
  RETURN p.full_path, count(f) AS files
  ORDER BY files DESC"`,
    copy: commandOf(`$ glab orbit query "
  MATCH (s:ImportedSymbol {import_path:
          'gitlab.com/gitlab-org/labkit/v2/log'})
        <-[:IMPORTS]-(f:File)-[:ON_BRANCH]->(b:Branch)
        <-[:CONTAINS]-(p:Project)
  RETURN p.full_path, count(f) AS files
  ORDER BY files DESC"`),
  },
  {
    title: "Spot conflicting work",
    icon: "warning",
    href: `${DOCS}/prompt-examples/#review-a-merge-request-with-proof`,
    text: "Other open merge requests that change the same files as yours.",
    code: `
$ glab orbit query "
  MATCH (mr:MergeRequest)-[:HAS_LATEST_DIFF]->
        (d:MergeRequestDiff)-[:HAS_FILE]->
        (f:MergeRequestDiffFile {project_id: 9438583})
  WHERE mr.state = 'opened' AND f.old_path = 'v2/log/log.go'
  RETURN mr.iid, mr.title"`,
    copy: commandOf(`$ glab orbit query "
  MATCH (mr:MergeRequest)-[:HAS_LATEST_DIFF]->
        (d:MergeRequestDiff)-[:HAS_FILE]->
        (f:MergeRequestDiffFile {project_id: 9438583})
  WHERE mr.state = 'opened' AND f.old_path = 'v2/log/log.go'
  RETURN mr.iid, mr.title"`),
  },
];

const QUOTES: Record<string, ComponentProps<typeof Testimonial>> = {
  ctm: {
    quote: "GitLab Orbit gave us an AI code reviewer that actually understands our codebase, not just the diff in front of it.",
    author: "Ryan Harvey",
    role: "Head of AI Engineering, Compare the Market",
    stats: [{ value: "70%", label: "Comments in the right place, against 58% with RAG" }],
    href: "https://comparethemarketcareers.com/blog/comparing-context-retrieval-approaches-for-ai-code-review/",
    linkLabel: "Read the study of 79 merge requests",
  },
  insurer: {
    quote: "We were able to get responses back that used to take us a day or like a half a day. We got back in seven minutes.",
    author: "Engineering team member",
    role: "US insurer",
    stats: [{ value: "7 min", label: "Down from half a day through the API" }],
  },
  appsec: {
    quote: "It was a lot more precise than the search bar of GitLab. It saved me, I think, about one hour of time.",
    author: "Application security engineer",
    role: "Telecom company",
    stats: [{ value: "~50 hours", label: "A year back from one saved compliance prompt" }],
  },
  health: {
    quote: "The thing that y'all have and nobody else has is the ability to auth into this context graph ecosystem.",
    author: "Site reliability engineer",
    role: "Digital health company",
    stats: [{ value: "Stopped", label: "Their in-house graph project" }],
  },
  bugFix: {
    quote: "Same bug, same developer. Only the context changed.",
    quoted: false,
    author: "A routine bug fix",
    role: "Gartner, “The Mythical Token Month,” June 2026",
    stats: [{ value: "$70 → $15", label: "Per routine bug fix, with Orbit" }],
  },
  platform: {
    quote: "For the right person, it is really like a killer tool.",
    author: "Platform engineering lead",
    role: "Healthcare diagnostics company",
    stats: [{ value: "75 → 1", label: "Repository checks, down to one question" }],
  },
};

const SLIDES: Slide[] = [
  { id: "ctm", label: "Compare the Market", content: <Testimonial {...QUOTES.ctm!} /> },
  { id: "insurer", label: "US insurer", content: <Testimonial {...QUOTES.insurer!} /> },
  { id: "bug-fix", label: "The cost of one bug fix", content: <Testimonial {...QUOTES.bugFix!} /> },
  { id: "appsec", label: "Telecom company", content: <Testimonial {...QUOTES.appsec!} /> },
  { id: "health", label: "Digital health company", content: <Testimonial {...QUOTES.health!} /> },
  { id: "platform", label: "Healthcare diagnostics company", content: <Testimonial {...QUOTES.platform!} /> },
];

const AGENTS = [
  { name: "GitLab Duo", mark: "gitlab" },
  { name: "Claude Code", mark: "claude" },
  { name: "Codex", mark: "openai" },
  { name: "Cursor", mark: "cursor" },
  { name: "GitHub Copilot", mark: "githubcopilot" },
  { name: "Gemini CLI", mark: "googlegemini" },
];

const DEMO = "https://about.gitlab.com/sales/?contact-topic=request-demo";

function SalesLink({ label }: { label: string }) {
  return (
    <SiteLink href={DEMO} aria-label={`${label} (opens in a new tab)`} className="pill-link">
      {label}
      <Icon name="arrow-right" size={16} />
    </SiteLink>
  );
}

function QuickstartLink() {
  return (
    <SiteLink href={`${DOCS}/quickstart/`} aria-label="Quickstart (opens in a new tab)" className={segment(false, true)}>
      <span>Quickstart</span>
      <Icon name="external-link" size={12} className="hero-ext" />
    </SiteLink>
  );
}

function SetupPicker({ className, inline = false }: { className?: string; inline?: boolean }) {
  const [agent, setAgent] = useState<string | null>(null);
  const pick = (name: string) => {
    setAgent(name);
    void navigator.clipboard?.writeText(setupPrompt(name));
  };
  const copy = (
    <CopyButton text={setupPrompt(agent)} label={`Copy a setup prompt for ${agent ?? "your agent"}`} primary={!inline} tile={inline}>
      Copy setup prompt
    </CopyButton>
  );
  const logos = <AgentLogos label="Pick your agent" marksOnly reveal={inline} agents={AGENTS} selected={agent ?? undefined} onSelect={pick} />;
  if (inline)
    return (
      <div className={cn("flex flex-wrap items-center gap-x-5 gap-y-4", className)}>
        {logos}
        <Reveal from="right" delay={420}>
          {copy}
        </Reveal>
      </div>
    );
  return (
    <div className={cn("flex flex-col items-center gap-5", className)}>
      <div className="flex flex-wrap items-center justify-center gap-x-4 gap-y-3">
        {copy}
        <QuickstartLink />
      </div>
      {logos}
    </div>
  );
}

function Hero() {
  return (
    <OrbitHero size="lg" align="center" pinned className="page-dark page-dark-top gl-dark gl-dark-scope -mt-10 flex flex-col">
      <p className="orbit-hero-title">
        One <span className="orbit-fade">context layer</span>
        <br />
        for all your agents.
      </p>
      <div className="mt-12 flex justify-center">
        <AgentPrompt
          label="Ask an agent with GitLab Orbit"
          examples={CHAT}
          graphNodes={963_261_776}
          cta={{ label: "Request a demo", href: DEMO }}
          fallback={
            <>
              To ask that about your own projects, run <code>glab orbit setup</code> or{" "}
              <a href={DEMO} target="_blank" rel="noreferrer" className="gl-link">
                request a demo
              </a>
              .
            </>
          }
        />
      </div>
      <SetupPicker className="mt-20" />
    </OrbitHero>
  );
}

function Clip({ src, poster, width, height, label, caption }: { src: string; poster: string; width: number; height: number; label: string; caption?: string }) {
  return (
    <figure className="mx-auto my-0 max-w-[46rem]">
      <div className="overflow-hidden rounded-[var(--radius-card)]">
        <video src={asset(src)} poster={asset(poster)} width={width} height={height} autoPlay muted loop playsInline aria-label={label} className="block h-auto w-full" />
      </div>
      {caption && <figcaption className="mt-3 text-center text-sm text-subtle">{caption}</figcaption>}
    </figure>
  );
}

function HomePage() {
  const narrow = useNarrow();
  const ripple = narrow ? ZERO_DAY_NARROW : ZERO_DAY;
  return (
    <PageFrame path="" title={site.name} options={{ hideHeader: true, landing: true }}>
      <Hero />

      <Section id="why" eyebrow="Why a graph" title={<><span className="orbit-fade">Higher agent accuracy</span>, lower token cost</>} lede="An agent that reads one file at a time guesses at what connects. GitLab Orbit gives it the connections.">
        <Contrasts
          items={[
            {
              title: "Answers with proof",
              field: "path",
              text: "Answers follow real edges, not text matches, and come with the query that found them.",
            },
            {
              title: "Questions about your whole org",
              field: "rows",
              text: "Ask about every project in your group, not only the repository the agent has open.",
            },
            {
              title: "Code, and what happened to it",
              field: "ripple",
              text: "Merge requests, pipelines, deployments, and findings, linked to the code they touched.",
            },
          ]}
        />
      </Section>

      <Section id="explore" dark glow="right" dots eyebrow="The graph" title={<>The context graph for your <span className="orbit-fade">software lifecycle</span></>} lede="Code, merge requests, pipelines, deployments, and vulnerabilities in one graph that stays up to date.">
        <Clip src="home/graph-explorer.mp4" poster="home/graph-explorer.jpg" width={1440} height={850} label="The GitLab Orbit graph explorer with the projects and groups of a GitLab instance" />
      </Section>

      <Section id="query" dark roomy eyebrow="How a query runs" title={<><span className="orbit-fade">Your permissions</span>, on every query</>} lede="Orbit stores no permission data, and the graph is read-only.">
        <Flow
          steps={[
            { title: "Your agent", text: "CLI, MCP, REST, or GitLab Duo", icon: "terminal" },
            { title: "GitLab", text: "Applies your permissions", icon: "lock" },
            { title: "Query engine", text: "Compiles GQL to SQL", icon: "code" },
            { title: "Graph", text: "Runs the query", icon: "connected" },
          ]}
        />
      </Section>

      <Section id="workflows" backdrop={<DotWave drift={2} scroll />} backdropFade="pinned" eyebrow="Use cases" title={<>What you can do <span className="orbit-fade">with Orbit</span></>} lede="Each query runs on public gitlab-org projects. Each card opens the recipe in the docs, with the prompt to use.">
        <UseCases items={USE_CASES} />
      </Section>

      <Section
        id="graph" dark pin hold
        className="page-section-head page-section-tail"
        backdrop={<RippleField source={ripple.source} nodes={ripple.nodes} anchor="#graph .ripple-nodes" />}
        backdropFade="full"
        eyebrow="Zero-day response"
        title={<>From a zero-day to a <span className="orbit-fade">fix in production</span></>}
        lede="Your agent finds the vulnerable code, traces it to production, and ships the fix."
      >
        <div className="ripple-stage">
          <div className="ripple-nodes" aria-label="Your agent starts at a zero-day package, finds the three files that import it, follows them to the four projects that use them, and ships a fix to the three that run in production. A staging project is not fixed." role="img" />
          <RippleLog lines={ZERO_DAY.log} />
        </div>
      </Section>

      <FieldBand over>
        <Slideshow slides={SLIDES} label="What our users say" />
      </FieldBand>

      <Section id="cost" dark glow="right" dots roomy eyebrow="Token cost" title={<>Fewer retries, <span className="orbit-fade">fewer tokens</span></>} lede="With GitLab Orbit, your agent makes fewer retries and fewer API calls.">
        <CostCalculator
          runs={{ label: "Agent runs per day", min: 100, max: 10_000, step: 100, value: 2_000 }}
          models={[
            { label: "Claude Opus 5.5", price: 4 },
            { label: "GPT-6 Astra", price: 10 },
            { label: "GPT-6.1 Sol", price: 2 },
            { label: "Gemini 4 Carbon", price: 4 },
          ]}
          defaultModel={0}
          tokensPerRun={150_000}
          saving={[0.2, 0.35]}
          footer={
            <SiteLink href={DEMO} aria-label="Talk to sales (opens in a new tab)" className="pill-link">
              Talk to sales
              <Icon name="arrow-right" size={16} />
            </SiteLink>
          }
        />
      </Section>

      <Section id="connect" snug backdrop={<OrbitHalo />} backdropFade="band" eyebrow="Connect your agent" title={<>Works with <span className="orbit-fade">your agents</span></>} lede="Built into GitLab Duo. Connect your agent with the CLI or MCP.">
        <div className="inline-flex max-w-full flex-col gap-6">
          <SetupPicker inline />
          <Reveal from="right" delay={300} className="w-0 min-w-full">
            <Terminal title="GitLab Orbit CLI" code={SETUP} copy="glab orbit setup" />
          </Reveal>
        </div>
      </Section>

      <Section id="common-questions" dark glow="left" dots eyebrow="Questions" title={<>Common <span className="orbit-fade">questions</span></>}>
        <FeatureGrid columns={3}>
          {QUESTIONS.map(({ question, answer, link }) => (
            <Feature key={question} title={question}>
              <p>{answer.split(/(glab orbit setup)/).map((part) => (part === "glab orbit setup" ? <code key={part}>{part}</code> : part))}</p>
              {link && (
                <p>
                  <SiteLink href={link.href} className="gl-link">
                    {link.label}
                  </SiteLink>
                </p>
              )}
            </Feature>
          ))}
        </FeatureGrid>
      </Section>

      <Section id="start" dark className="page-dark-bottom page-end" title={<>Your agent already has the code. <span className="orbit-fade">Give it the rest.</span></>} lede="Answers from your real GitLab data, not guesses or digging by hand.">
        <div className="flex flex-col items-center gap-4">
          <div className="flex flex-wrap items-center justify-center gap-x-6 gap-y-3">
            <SalesLink label="Request a demo" />
            <QuickstartLink />
          </div>
        </div>
        {narrow ? (
          <div className="gravity-globe" aria-hidden="true">
            <DotWave className="gravity-globe-field" drift={2} speed={0.9} reach={1.12} />
          </div>
        ) : (
          <GravityGlobe globe={false} />
        )}
      </Section>
    </PageFrame>
  );
}
