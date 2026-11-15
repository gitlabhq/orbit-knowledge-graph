import { type ReactNode, useState } from "react";
import { AgentPrompt, chip, cn, type PromptChip, type PromptExample } from "@gitlab-org/orbit-site";

// Orbit Next has not shipped, so every answer is sample data and says so. The story follows the team's
// third-party demo world (a pricing cache, epic ORB-1, incident INC-77), moved onto this page's made-up projects.
const NEXT_SOURCES: PromptChip[] = [
  { label: "Code search", icon: "code" },
  { label: "Jira", icon: "work-item-issue" },
  { label: "Slack", icon: "comment" },
  { label: "incident.io", icon: "status-alert" },
  { label: "Grafana", icon: "chart" },
];

function Preview({ children }: { children: ReactNode }) {
  return <p className="hero-chat-preview">{children}</p>;
}

const NEXT: PromptExample[] = [
  {
    label: "Search all your code",
    icon: "code",
    prompt: "Find every cache.get call without a timeout, in all our repositories.",
    calls: [
      { tool: "Read Orbit guide", detail: "orbit" },
      { tool: "Explored code search", detail: "structural: $C.get($KEY)" },
    ],
    answer: (
      <>
        <p>
          A structural search for <code>$C.get($KEY)</code> across every repository found 23 calls with no timeout, in 4 projects:
        </p>
        <ul>
          <li>
            <code>checkout-api</code>
            <span>11 calls</span>
          </li>
          <li>
            <code>pricing-svc</code>
            <span>7 calls</span>
          </li>
          <li>
            <code>web-app</code>
            <span>3 calls</span>
          </li>
          <li>
            <code>mobile-bff</code>
            <span>2 calls</span>
          </li>
        </ul>
        <p>
          Start with <code>checkout-api/app/pricing.rb:57</code>. It runs on every checkout.
        </p>
        <Preview>Orbit Next preview, with sample data.</Preview>
      </>
    ),
    stats: { apiCallsReplaced: 1400, nodes: 23, edges: 27 },
  },
  {
    label: "Track a Jira epic",
    icon: "work-item-issue",
    prompt: "Is Jira epic ORB-1 on track? Show the merge request behind each story.",
    calls: [
      { tool: "Read Orbit guide", detail: "orbit" },
      { tool: "Read Jira", detail: "JiraIssue ORB-1 → children" },
      { tool: "Explored Orbit", detail: "JiraIssue → MergeRequest → Pipeline" },
    ],
    answer: (
      <>
        <p>ORB-1, “Checkout latency below 500 ms”, has 3 stories. One is done, one is in review, and one has no code yet:</p>
        <ul>
          <li>
            <span>ORB-12 Pricing cache</span>
            <span>done · checkout-api!42, !43</span>
          </li>
          <li>
            <span>ORB-14 Alert on checkout p95</span>
            <span>in review · !44, pipeline passed</span>
          </li>
          <li>
            <span>ORB-13 Tune the connection pool</span>
            <span>to do · no merge request</span>
          </li>
        </ul>
        <p>Checkout p95 is at 420 ms, so the goal holds. ORB-13 is the open risk.</p>
        <Preview>Jira data is coming soon. This answer uses sample data.</Preview>
      </>
    ),
    stats: { apiCallsReplaced: 180, nodes: 31, edges: 44 },
  },
  {
    label: "Explain an incident",
    icon: "status-alert",
    prompt: "What caused INC-77, and which change fixed it?",
    calls: [
      { tool: "Read Orbit guide", detail: "orbit" },
      { tool: "Read incident.io", detail: "Incident INC-77" },
      { tool: "Read Grafana", detail: "annotations on the checkout dashboard" },
      { tool: "Read Slack", detail: "messages that cite INC-77" },
      { tool: "Explored Orbit", detail: "MergeRequest → Pipeline → Deployment" },
    ],
    answer: (
      <>
        <p>
          The pricing cache in <code>checkout-api!42</code> caused it. <code>!43</code> fixed the cache stampede:
        </p>
        <ul>
          <li>
            <span>Checkout p95 latency spike, major</span>
            <span>incident.io</span>
          </li>
          <li>
            <span>p95 above 800 ms, 1 hour after the !42 deploy</span>
            <span>Grafana</span>
          </li>
          <li>
            <span>“Rolling back !42 now, see ORB-12.”</span>
            <span>Slack</span>
          </li>
          <li>
            <span>!43 deployed, p95 back to 420 ms</span>
            <span>GitLab</span>
          </li>
        </ul>
        <p>Orbit checks your access in each tool before it answers.</p>
        <Preview>incident.io, Grafana, and Slack data are coming soon. This answer uses sample data.</Preview>
      </>
    ),
    stats: { apiCallsReplaced: 340, nodes: 47, edges: 63 },
  },
];

type Mode = "orbit" | "next";

// The kit hides the chat's window bar from screen readers, so the switch is a sibling laid over it, not a
// child. The key remounts the chat so the input types the new examples from the start.
export function HeroChat({ examples, graphNodes, fallback, cta }: { examples: PromptExample[]; graphNodes: number; fallback: ReactNode; cta: { label: string; href: string } }) {
  const [mode, setMode] = useState<Mode>("orbit");
  const next = mode === "next";
  const option = (value: Mode, label: string) => (
    <button type="button" aria-pressed={mode === value} onClick={() => setMode(value)} className={cn(chip(mode === value), "hero-chat-option")}>
      {label}
    </button>
  );
  return (
    <div className="hero-chat">
      <div className="hero-chat-switch" role="group" aria-label="Version">
        {option("orbit", "GitLab Orbit")}
        {option("next", "Orbit Next")}
      </div>
      <AgentPrompt
        key={mode}
        label={next ? "Ask an agent with Orbit Next, a preview" : "Ask an agent with GitLab Orbit"}
        examples={next ? NEXT : examples}
        chips={next ? NEXT_SOURCES : []}
        graphNodes={graphNodes}
        cta={cta}
        fallback={
          next ? (
            <>
              Orbit Next is a preview. Pick an example below to see code search across all your repositories, and Jira, Slack, incident.io, and Grafana next to your GitLab data. These sources are coming soon.
            </>
          ) : (
            fallback
          )
        }
      />
    </div>
  );
}
