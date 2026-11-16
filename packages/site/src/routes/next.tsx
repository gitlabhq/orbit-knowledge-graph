import { useState } from "react";
import { FieldBand, Icon, PageFrame, pageHead, type RippleLogLine, Section } from "@gitlab-org/orbit-site";
import { createFileRoute } from "@tanstack/react-router";
import { AgentLog } from "../next/AgentLog";
import { Capabilities } from "../next/Capabilities";
import { CodeSearch, SOURCES } from "../next/CodeSearch";
import { type Hop, HopField } from "../next/HopField";
import { SearchMap } from "../next/SearchMap";
import { CORPUS, CROSS_SEARCHES, LATENCY, type Search } from "../next/searches";
import { NEXT_PAGE } from "../sections";

export const Route = createFileRoute("/next")({
  head: () => pageHead(NEXT_PAGE.title, NEXT_PAGE.description),
  component: NextPage,
});

const LOOP = 12000;
const INCIDENT = CROSS_SEARCHES.find((search) => search.id === "incident")!;
const AT = [0.03, 0.11, 0.19, 0.3, 0.41, 0.7];
const HOPS: Hop[] = INCIDENT.steps!.map((step, index) => ({ tool: step.tool, label: step.name, x: INCIDENT.points![index]![0], y: INCIDENT.points![index]![1], at: AT[index]!, tone: step.tone }));
const LOG: RippleLogLine[] = [
  { at: 0.03, tool: true, text: "Read the incident" },
  { at: 0.11, tool: true, text: "Checked Grafana alerts" },
  { at: 0.19, tool: true, text: "Traced GitLab deploys" },
  { at: 0.3, tool: true, text: "Searched Slack" },
  { at: 0.41, tool: true, text: "Searched code" },
  { at: 0.52, text: "Adding a timeout to cache.get." },
  { at: 0.6, tone: "ok", text: "checkout-api!43 opened, pipeline passed" },
  { at: 0.7, tone: "ok", text: "!43 deployed to production" },
  { at: 0.78, tone: "ok", text: "p95 is back to 420 ms. INC-77 resolved." },
];

function Hero() {
  const [search, setSearch] = useState<Search | null>(null);
  return (
    <section className="orbit-hero orbit-hero-lg orbit-hero-center next-hero not-prose page-dark page-dark-top gl-dark gl-dark-scope -mt-10 flex flex-col">
      <div className="next-hero-map" aria-hidden="true">
        <SearchMap search={search} anchor=".next-chat" region=".next-demo-viz" />
      </div>
      <p className="orbit-hero-title next-hero-title">
        Search all of your code,
        <br />
        <span className="orbit-fade">and every tool around it.</span>
      </p>
      <div className="next-hero-demo flex w-full justify-center">
        <CodeSearch onSearch={setSearch} />
      </div>
    </section>
  );
}

function NextPage() {
  return (
    <PageFrame path="next" title={NEXT_PAGE.title} options={{ hideHeader: true, landing: true }}>
      <Hero />

      <Section id="search" pin className="page-section-tail" eyebrow="Code search" title={<>Every branch, <span className="orbit-fade">every language</span></>} lede="The same search works on any repository you can see, in any file, on any branch.">
        <Capabilities />
      </Section>

      <FieldBand over eyebrow="Scale" className="next-scale">
        <h2 className="section-title">
          Built for <span className="orbit-fade">scale</span>
        </h2>
        <p className="section-lede">In our testing, we indexed and searched 51,656 open source projects with their branches and history.</p>
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
              <span className="next-latency-label">{row.label}</span>
              <span className="next-latency-track">
                <span className="next-latency-bar" style={{ width: `${(row.ms / 120) * 100}%` }} />
              </span>
              <span className="next-latency-ms">~{row.ms} ms</span>
            </li>
          ))}
        </ul>
      </FieldBand>

      <Section
        id="sources" dark
        className="page-dark-bottom next-sources"
        backdrop={<HopField hops={HOPS} anchor="#sources .hop-nodes" loop={LOOP} />}
        backdropFade="full"
        eyebrow="Third-party data"
        title={<>Your other tools, <span className="orbit-fade">next to your code</span></>}
        lede="Orbit finds the path from tool to tool, checks your access in each one, and your agent ships the fix."
      >
        <ul className="agent-prompt-chips next-section-tools" aria-label="Tools">
          {SOURCES.map((source) => (
            <li key={source.label} className="agent-prompt-chip">
              <Icon name={source.icon} size={12} />
              {source.label}
            </li>
          ))}
        </ul>
        <div className="ripple-stage">
          <div className="ripple-nodes hop-nodes" role="img" aria-label="Your agent starts at an incident in incident.io, follows the alert in Grafana to a deploy in GitLab, reads the Slack thread behind it, finds the line of code with code search, and ships the merge request that fixes it." />
          <AgentLog lines={LOG} loop={LOOP} />
        </div>
        <p className="next-notice">
          This page contains information related to upcoming products, features, and functionality. It is important to note that the information presented is for informational purposes only. Please do not rely on this information for purchasing or planning purposes. The development, release, and timing of any products, features, or functionality may be subject to change or delay and remain at the sole discretion of GitLab Inc.
        </p>
      </Section>

    </PageFrame>
  );
}
