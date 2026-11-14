import { Icon, LinkCard, LinkCards, OrbitHero, pageHead, SiteLink, segment, site } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Frame } from "../components/Frame";
import { Board } from "../roadmap/Board";
import { formatDate, quarterLabel, quarterMonths } from "../roadmap/calendar";
import { items, roadmapQuarter, roadmapUpdated, themes } from "../roadmap/model";
import { Upcoming } from "../roadmap/Upcoming";
import { USER_DOCS } from "../sections";

export const Route = createFileRoute("/")({
  head: () => pageHead(site.name),
  component: HomePage,
});

const OUTBOUND = [
  { label: "Documentation", href: USER_DOCS.href },
  { label: "Product page", href: "https://about.gitlab.com/gitlab-orbit/" },
  { label: "Click-through demo", href: "https://click-through-demo-generator-v-2-d63870.gitlab.io/demos/orbit-v2/" },
  { label: "Source code", href: "https://gitlab.com/gitlab-org/orbit/knowledge-graph" },
];

// As on the Orbit Portal home page, the negative bottom margin offsets the hero padding and the tile
// margin, so the buttons sit about as far from the tiles as the tiles sit from each other.
function Hero() {
  return (
    <OrbitHero className="-mb-[1.625rem]">
      <p className="orbit-hero-title">
        One context layer
        <br />
        <span className="orbit-fade">for all your agents.</span>
      </p>
      <p className="mt-5 max-w-[37rem] text-base text-balance text-subtle">
        GitLab Orbit is a context graph of your code and software delivery data. Your agents query it for fast, correct answers that respect GitLab permissions.
      </p>
      <div className="mt-16 flex flex-wrap gap-1">
        {OUTBOUND.map((link) => (
          <SiteLink key={link.href} href={link.href} className={segment(false, true)}>
            <span>{link.label}</span>
            <Icon name="external-link" size={12} className="hero-ext" />
          </SiteLink>
        ))}
      </div>
    </OrbitHero>
  );
}

function SectionHeading({ id, title, description, action }: { id: string; title: string; description: string; action?: { label: string; to: string } }) {
  return (
    <div className="mb-4 flex flex-wrap items-end justify-between gap-4">
      <div>
        <h2 id={id} className="m-0 text-xl font-semibold text-default">
          {title}
        </h2>
        <p className="mt-1 mb-0 text-sm text-subtle">{description}</p>
      </div>
      {action && (
        <Link to={action.to} className="gl-link text-sm">
          {action.label}
        </Link>
      )}
    </div>
  );
}

function Themes() {
  return (
    <ul className="card tile-quiet m-0 list-none divide-y divide-[var(--tile-border)] p-0">
      {themes.map((theme) => {
        const count = items.filter((item) => item.theme.id === theme.id && item.status !== "shipped").length;
        return (
          <li key={theme.id}>
            <Link to="/roadmap" search={{ theme: theme.id }} className="group flex items-start gap-4 px-5 py-3.5 text-default no-underline">
              <Icon name={theme.icon} size={14} className="tile-icon mt-1 shrink-0" />
              <span className="flex min-w-0 flex-1 flex-col gap-1 sm:flex-row sm:gap-4">
                <span className="text-sm font-semibold group-hover:underline sm:w-48 sm:shrink-0">{theme.title}</span>
                <span className="text-sm text-subtle">{theme.description}</span>
                <span className="shrink-0 text-xs text-subtle tabular-nums sm:ml-auto sm:pt-0.5">
                  {count} upcoming {count === 1 ? "item" : "items"}
                </span>
              </span>
            </Link>
          </li>
        );
      })}
    </ul>
  );
}

function HomePage() {
  return (
    <Frame title={site.name} hideHeader>
      <Hero />
      <LinkCards>
        <LinkCard title="Roadmap" href="/roadmap" icon="calendar" description="What we build next, by quarter and by theme." />
        <LinkCard title="Content" href="/content" icon="play" description="Videos, demos, and articles about Orbit." />
        <LinkCard title="Engineering" href="/engineering" icon="code" description="Architecture, runbooks, ADRs, and testing." />
        <LinkCard title="Team" href="/team" icon="group" description="The people who build GitLab Orbit." />
      </LinkCards>
      <Upcoming />
      <div className="mt-12 flex flex-col gap-12">
        <section aria-labelledby="horizons">
          <SectionHeading
            id="horizons"
            title="Now, next, and later"
            description={`The top items in each horizon. This quarter is ${quarterLabel(roadmapQuarter)} (${quarterMonths(roadmapQuarter)}). Updated ${formatDate(roadmapUpdated)}.`}
            action={{ label: "Full roadmap", to: "/roadmap" }}
          />
          <div className="page-bleed">
            <Board items={items} limit={3} compact headingLevel={3} />
          </div>
        </section>
        {themes.length > 0 && (
          <section aria-labelledby="themes">
            <SectionHeading id="themes" title="Themes" description="Roadmap items, grouped by area." />
            <Themes />
          </section>
        )}
      </div>
    </Frame>
  );
}
