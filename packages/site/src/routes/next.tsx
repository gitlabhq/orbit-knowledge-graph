import { useState } from "react";
import { PageFrame, pageHead, RippleLog, type RippleLogLine, Section } from "@gitlab-org/orbit-site";
import { createFileRoute } from "@tanstack/react-router";
import { Capabilities } from "../next/Capabilities";
import { CodeSearch } from "../next/CodeSearch";
import { type Hop, HopField } from "../next/HopField";
import { SearchMap } from "../next/SearchMap";
import { CORPUS, LATENCY, type Search } from "../next/searches";
import { NEXT_PAGE } from "../sections";

export const Route = createFileRoute("/next")({
  head: () => pageHead(NEXT_PAGE.title, NEXT_PAGE.description),
  component: NextPage,
});

// The incident from the team's third-party demo world, followed from tool to tool. The names are made up.
// The field and the log share one loop.
const LOOP = 12000;
const HOPS: Hop[] = [
  { tool: "incident.io", label: "INC-77 latency spike", x: 0.1, y: 0.6, at: 0.03, tone: "risk" },
  { tool: "Grafana", label: "p95 alert", x: 0.27, y: 0.24, at: 0.11 },
  { tool: "GitLab", label: "!42 deployed", x: 0.46, y: 0.56, at: 0.19 },
  { tool: "Jira", label: "ORB-12 pricing cache", x: 0.28, y: 0.92, at: 0.27 },
  { tool: "Slack", label: "rollback thread", x: 0.64, y: 0.9, at: 0.35 },
  { tool: "Code search", label: "pricing.rb:57", x: 0.7, y: 0.26, at: 0.43 },
  { tool: "GitLab", label: "!43 deployed", x: 0.92, y: 0.58, at: 0.7, tone: "ok" },
];
// Every read goes through Orbit, which finds the next hop in each tool, as the Orbit tools show in an agent.
// The agent then ships the fix.
const LOG: RippleLogLine[] = [
  { at: 0.03, tool: true, text: "Explored Orbit · incident.io" },
  { at: 0.06, text: "INC-77: checkout p95 is at 2 s." },
  { at: 0.11, tool: true, text: "Explored Orbit · Grafana" },
  { at: 0.14, text: "The alert fired 1 hour after a deploy." },
  { at: 0.19, tool: true, text: "Explored Orbit · GitLab" },
  { at: 0.22, text: "That deploy shipped checkout-api!42." },
  { at: 0.27, tool: true, text: "Explored Orbit · Jira and Slack" },
  { at: 0.31, text: "!42 is ORB-12, the pricing cache. The team saw a cache stampede." },
  { at: 0.43, tool: true, text: "Explored Orbit · code search" },
  { at: 0.46, text: "cache.get in pricing.rb:57 has no timeout." },
  { at: 0.52, text: "Adding a timeout and request coalescing." },
  { at: 0.6, tone: "ok", text: "checkout-api!43 opened, pipeline passed" },
  { at: 0.7, tone: "ok", text: "!43 deployed to production" },
  { at: 0.78, tone: "ok", text: "p95 is back to 420 ms. INC-77 resolved." },
];

// Built like the home hero, with the search map behind it in place of the graph sphere. The map acts out
// each search the demo runs.
function Hero() {
  const [search, setSearch] = useState<Search | null>(null);
  return (
    <section className="orbit-hero orbit-hero-lg orbit-hero-center next-hero not-prose page-dark page-dark-top gl-dark gl-dark-scope -mt-10 flex flex-col">
      <div className="next-hero-map" aria-hidden="true">
        <SearchMap search={search} anchor=".next-query" />
      </div>
      <p className="orbit-hero-eyebrow">Coming soon</p>
      <p className="orbit-hero-title">
        Search all of your code,
        <br />
        <span className="orbit-fade">and every tool around it.</span>
      </p>
      <p className="mx-auto mt-5 max-w-[40rem] text-base text-balance text-subtle">
        What we are building next for GitLab Orbit: code search across every repository and branch, and your Jira, Slack, incident.io, and Grafana data next to your code.
      </p>
      <div className="mt-10 flex w-full justify-center">
        <CodeSearch onSearch={setSearch} />
      </div>
    </section>
  );
}

function NextPage() {
  return (
    <PageFrame path="next" title={NEXT_PAGE.title} options={{ hideHeader: true, landing: true }}>
      <Hero />

      <Section id="scale" eyebrow="Scale" title={<>Built for <span className="orbit-fade">scale</span></>} lede="The prototype already indexes 51,656 open source projects, every branch it keeps, and their history. A search reads only the few files that can match, so answers come back in about a tenth of a second.">
        <dl className="next-stats not-prose">
          {CORPUS.map((stat) => (
            <div key={stat.label} className="next-stat">
              <dt>{stat.label}</dt>
              <dd>{stat.value}</dd>
            </div>
          ))}
        </dl>
        <ul className="next-latency not-prose" aria-label="Warm query times">
          {LATENCY.map((row) => (
            <li key={row.label} className="next-latency-row">
              <span className="next-latency-label">
                {row.label}
                <span>{row.note}</span>
              </span>
              <span className="next-latency-track">
                <span className="next-latency-bar" style={{ width: `${(row.ms / 120) * 100}%` }} />
              </span>
              <span className="next-latency-ms">~{row.ms} ms</span>
            </li>
          ))}
        </ul>
        <p className="next-stats-note">Index size from the Orbit Next prototype on Nov 15, 2026. Query times are warm.</p>
      </Section>

      <Section id="search" eyebrow="Code search" title={<>Every branch, <span className="orbit-fade">every language</span></>} lede="The same search works on any repository you can see, in any file, on any branch.">
        <Capabilities />
      </Section>

      <Section
        id="sources" dark
        className="page-dark-bottom"
        backdrop={<HopField hops={HOPS} anchor="#sources .hop-nodes" loop={LOOP} />}
        backdropFade="full"
        eyebrow="Third-party data"
        title={<>Your other tools, <span className="orbit-fade">next to your code</span></>}
        lede="Orbit finds the path from tool to tool, checks your access in each one, and your agent ships the fix."
      >
        <div className="ripple-stage">
          <div className="ripple-nodes hop-nodes" role="img" aria-label="Your agent starts at an incident in incident.io, follows the alert in Grafana to a deploy in GitLab, reads the Jira story and the Slack thread behind it, finds the line of code with code search, and ships the merge request that fixes it." />
          <RippleLog lines={LOG} loop={LOOP} />
        </div>
      </Section>

    </PageFrame>
  );
}
