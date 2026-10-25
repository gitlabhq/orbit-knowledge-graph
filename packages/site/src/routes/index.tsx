import { cn, findPage, Icon, pageHead, pageLink, site, SiteLink } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { PostCard } from "../content/PostCard";
import { Board } from "../roadmap/Board";
import { posts } from "../content/blog";
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
        <p className="mt-3 mb-0 max-w-2xl text-base text-subtle">
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
    <div className="mb-6 flex flex-wrap items-end justify-between gap-3">
      <div>
        <h2 className="m-0 text-xl font-semibold text-default">{title}</h2>
        <p className="mt-1 mb-0 text-subtle">{description}</p>
      </div>
      {action && (
        <Link to={action.to} className="inline-flex items-center gap-1.5 text-sm font-semibold no-underline hover:underline">
          {action.label}
          <Icon name="arrow-right" size={14} />
        </Link>
      )}
    </div>
  );
}

function Themes() {
  return (
    <ul className="m-0 grid list-none gap-3 p-0 sm:grid-cols-2 lg:grid-cols-5">
      {themes.map((theme) => {
        const count = items.filter((item) => item.theme.id === theme.id && item.status !== "shipped").length;
        return (
          <li key={theme.id}>
            <Link to="/roadmap" search={{ theme: theme.id }} className={cn("card group flex h-full flex-col gap-3 p-5 no-underline", `tone-${theme.id}`)}>
              <span className="tone-icon flex size-10 items-center justify-center rounded-control">
                <Icon name={theme.icon} size={20} />
              </span>
              <span className="text-base font-semibold text-default">{theme.title}</span>
              <span className="text-sm text-subtle">{theme.description}</span>
              <span className="mt-auto inline-flex items-center gap-1 pt-1 text-xs font-semibold text-subtle group-hover:text-default">
                {count} upcoming
                <Icon name="chevron-right" size={12} />
              </span>
            </Link>
          </li>
        );
      })}
    </ul>
  );
}

function Engineering() {
  return (
    <ul className="m-0 grid list-none gap-3 p-0 sm:grid-cols-2 lg:grid-cols-4">
      {ENGINEERING.map(({ route, label }) => (
        <li key={route}>
          <Link {...pageLink(route)} className="card group flex h-full flex-col gap-2 p-5 no-underline">
            <span className="text-base font-semibold text-default">{label}</span>
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
            <SectionHeading title="Demos" description="Videos and talks that show how Orbit works." action={{ label: "All demos", to: "/content/demos" }} />
            <ul className="m-0 grid list-none gap-4 p-0 md:grid-cols-3">
              {videos.slice(0, 3).map((video) => (
                <li key={video.id}>
                  <Video video={video} />
                </li>
              ))}
            </ul>
          </section>
        )}
        {posts.length > 0 && (
          <section>
            <SectionHeading title="From the blog" description="Engineering notes and product updates." action={{ label: "All content", to: "/content" }} />
            {posts.length === 1 ? (
              <PostCard post={posts[0]} featured />
            ) : (
              <ul className="m-0 grid list-none gap-4 p-0 md:grid-cols-3">
                {posts.slice(0, 3).map((post) => (
                  <li key={post.slug}>
                    <PostCard post={post} />
                  </li>
                ))}
              </ul>
            )}
          </section>
        )}
      </div>
    </>
  );
}
