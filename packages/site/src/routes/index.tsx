import { findPage, Icon, LinkCard, LinkCards, pageHead, pageLink, SEGMENTS, SiteLink, segment, site, Video, VideoGrid } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Frame } from "../components/Frame";
import { videos } from "../content/demos";
import { Board } from "../roadmap/Board";
import { formatDate, quarterLabel, quarterMonths } from "../roadmap/calendar";
import { items, roadmapQuarter, roadmapUpdated, themes } from "../roadmap/model";
import { ENGINEERING, USER_DOCS } from "../sections";

export const Route = createFileRoute("/")({
  head: () => pageHead(site.name),
  component: HomePage,
});

function Intro() {
  return (
    <section className="mb-10">
      <h1 className="m-0 text-3xl font-semibold text-default">GitLab Orbit</h1>
      <p className="mt-4 mb-0 max-w-2xl text-base text-subtle">
        Orbit is a context graph of your code and your software delivery data. Agents and engineers query the graph for fast, correct answers that respect GitLab permissions.
      </p>
      <div className={`mt-6 ${SEGMENTS}`}>
        <Link to="/roadmap" className={segment(false, true)}>
          See the roadmap
        </Link>
        <Link to="/content" className={segment(false, true)}>
          Content
        </Link>
        <Link {...pageLink("engineering")} className={segment(false, true)}>
          Engineering
        </Link>
        <SiteLink href={USER_DOCS.href} className={segment(false, true)}>
          User documentation
          <Icon name="external-link" size={12} className="tile-icon" />
        </SiteLink>
      </div>
      <p className="mt-6 mb-0 text-xs text-subtle">
        This quarter: <span className="font-semibold text-default">{quarterLabel(roadmapQuarter)}</span> ({quarterMonths(roadmapQuarter)}) · Updated {formatDate(roadmapUpdated)}
      </p>
    </section>
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
      <Intro />
      <div className="flex flex-col gap-12">
        <section aria-labelledby="horizons">
          <SectionHeading id="horizons" title="Now, next, and later" description="The top items in each horizon." action={{ label: "Full roadmap", to: "/roadmap" }} />
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
        <section aria-labelledby="engineering">
          <SectionHeading id="engineering" title="Engineering" description="How the Orbit team builds, tests, and runs Orbit." action={{ label: "Engineering docs", to: "/engineering" }} />
          <LinkCards>
            {ENGINEERING.map(({ route, label }) => (
              <LinkCard key={route} title={label} href={`/${route}`} icon="book" description={findPage(route)?.meta.description} />
            ))}
          </LinkCards>
        </section>
        {videos.length > 0 && (
          <section aria-labelledby="demos">
            <SectionHeading id="demos" title="Demos" description="Videos and talks that show how Orbit works." action={{ label: "All content", to: "/content" }} />
            <VideoGrid>
              {videos.slice(0, 2).map((video) => (
                <Video key={video.id} {...video} />
              ))}
            </VideoGrid>
          </section>
        )}
      </div>
    </Frame>
  );
}
