import { findPage, Icon, pageHead, pageLink, site, SiteLink } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Board } from "../roadmap/Board";
import { videos } from "../content/demos";
import { Video } from "../ui/Video";
import { ENGINEERING, USER_DOCS } from "../sections";
import { formatDate, quarterLabel, quarterMonths } from "../roadmap/calendar";
import { items, roadmapQuarter, roadmapUpdated, themes } from "../roadmap/model";


export const Route = createFileRoute("/")({
  head: () => pageHead(site.name),
  component: HomePage,
});

const PRIMARY = "gl-button btn-confirm no-underline";
const SECONDARY = "gl-button no-underline";

function Hero() {
  return (
    <section className="border-b border-border">
      <div className="page-container py-12">
        <h1 className="m-0 text-3xl font-semibold text-default">GitLab Orbit</h1>
        <p className="mt-4 mb-0 max-w-2xl text-base text-subtle">
          Orbit is a context graph of your code and your software delivery data. Agents and engineers query the graph for fast, correct answers that respect GitLab permissions.
        </p>
        <div className="mt-6 flex flex-wrap gap-2">
          <Link to="/roadmap" className={PRIMARY}>
            See the roadmap
          </Link>
          <Link to="/content" className={SECONDARY}>
            Content
          </Link>
          <Link {...pageLink("engineering")} className={SECONDARY}>
            Engineering
          </Link>
          <SiteLink href={USER_DOCS.href} className={SECONDARY}>
            User documentation
            <Icon name="external-link" size={12} />
          </SiteLink>
        </div>
        <p className="mt-6 mb-0 text-xs text-subtle">
          This quarter: <span className="font-semibold text-default">{quarterLabel(roadmapQuarter)}</span> ({quarterMonths(roadmapQuarter)}) · Updated {formatDate(roadmapUpdated)}
        </p>
      </div>
    </section>
  );
}

function SectionHeading({ title, description, action }: { title: string; description: string; action?: { label: string; to: string } }) {
  return (
    <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
      <div>
        <h2 className="m-0 text-xl font-semibold text-default">{title}</h2>
        <p className="mt-1 mb-0 text-subtle">{description}</p>
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
    <ul className="card m-0 list-none divide-y divide-border p-0">
      {themes.map((theme) => {
        const count = items.filter((item) => item.theme.id === theme.id && item.status !== "shipped").length;
        return (
          <li key={theme.id}>
            <Link to="/roadmap" search={{ theme: theme.id }} className="group flex items-start gap-4 p-4 text-default no-underline">
              <Icon name={theme.icon} size={16} className="mt-0.5 shrink-0 text-subtle" />
              <span className="flex min-w-0 flex-1 flex-col gap-1 sm:flex-row sm:gap-4">
                <span className="text-sm font-semibold group-hover:underline sm:w-56 sm:shrink-0">{theme.title}</span>
                <span className="text-sm text-subtle">{theme.description}</span>
              </span>
              <span className="shrink-0 text-xs text-subtle">{count} upcoming {count === 1 ? "item" : "items"}</span>
            </Link>
          </li>
        );
      })}
    </ul>
  );
}

function Engineering() {
  return (
    <ul className="m-0 grid list-none gap-4 p-0 sm:grid-cols-2 lg:grid-cols-4">
      {ENGINEERING.map(({ route, label }) => (
        <li key={route}>
          <Link {...pageLink(route)} className="card flex h-full flex-col gap-2 p-4">
            <span className="card-title text-base font-semibold text-default">{label}</span>
            <span className="text-sm text-subtle">{findPage(route)?.meta.description}</span>
          </Link>
        </li>
      ))}
    </ul>
  );
}

function HomePage() {
  return (
    <>
      <Hero />
      <div className="page-container flex flex-col gap-12 py-10">
        <section>
          <SectionHeading title="Now, next, and later" description="The top items in each horizon." action={{ label: "Full roadmap", to: "/roadmap" }} />
          <Board items={items} limit={3} compact headingLevel={3} />
        </section>
        {themes.length > 0 && (
          <section>
            <SectionHeading title="Themes" description="Roadmap items, grouped by area." />
            <Themes />
          </section>
        )}
        <section>
          <SectionHeading title="Engineering" description="How the Orbit team builds, tests, and runs Orbit." action={{ label: "Engineering docs", to: "/engineering" }} />
          <Engineering />
        </section>
        {videos.length > 0 && (
          <section>
            <SectionHeading title="Demos" description="Videos and talks that show how Orbit works." action={{ label: "All content", to: "/content" }} />
            <ul className="m-0 grid list-none gap-4 p-0 md:grid-cols-3">
              {videos.slice(0, 3).map((video) => (
                <li key={video.id}>
                  <Video video={video} />
                </li>
              ))}
            </ul>
          </section>
        )}
      </div>
    </>
  );
}
