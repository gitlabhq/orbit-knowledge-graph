import { Badge, Embed, EmbedGrid, findPage, PageFrame, pageHeadOf, Video, VideoGrid } from "@gitlab-org/orbit-site";
import { createFileRoute } from "@tanstack/react-router";
import { PageFooter } from "../components/PageFooter";
import { type DemoVideo, featured, links, videos } from "../content/demos";
import { Board } from "../roadmap/Board";
import { quarterIndex } from "../roadmap/calendar";
import { EmptyState } from "../roadmap/EmptyState";
import { ItemCard } from "../roadmap/ItemCard";
import { items, themeById, themes } from "../roadmap/model";
import { ThemeFilter } from "../roadmap/ThemeFilter";

type AboutSearch = { theme?: string };

export const Route = createFileRoute("/about/")({
  validateSearch: (search: Record<string, unknown>): AboutSearch => ({
    theme: themeById(typeof search.theme === "string" ? search.theme : undefined)?.id,
  }),
  head: () => pageHeadOf(findPage("about")),
  component: AboutPage,
});

const without = ({ featured: _, ...video }: DemoVideo) => video;

function AboutPage() {
  const { theme } = Route.useSearch();
  const page = findPage("about");
  const filtered = theme ? items.filter((item) => item.theme.id === theme) : items;
  const open = filtered.filter((item) => item.status !== "shipped");
  const shipped = filtered.filter((item) => item.status === "shipped").sort((a, b) => quarterIndex(b.end) - quarterIndex(a.end));
  return (
    <PageFrame path="about" title={page?.meta.title ?? "About GitLab Orbit"} description={page?.meta.description} footer={page && <PageFooter page={page} />}>
      {featured && <Video {...without(featured)} />}
      <section aria-labelledby="roadmap" className="mt-10">
        <h2 id="roadmap" className="m-0 text-xl font-semibold text-default">
          Roadmap
        </h2>
        {themes.length > 0 && (
          <div className="not-prose mt-3 mb-6 flex flex-col items-start gap-2">
            <ThemeFilter theme={theme} />
          </div>
        )}
        <div className="page-bleed page-glow">
          {items.length === 0 ? (
            <EmptyState message="The roadmap has no items." reset={false} />
          ) : open.length > 0 ? (
            <Board items={filtered} />
          ) : (
            <EmptyState message="This theme has no upcoming items." />
          )}
        </div>
        {shipped.length > 0 && (
          <div className="page-bleed mt-8">
            <section aria-labelledby="shipped" className="board-surface glass-surface rounded-[var(--radius-card)] p-2">
              <header className="flex items-center gap-2 px-2 pb-2">
                <h3 id="shipped" className="m-0 text-base font-semibold text-heading">
                  Recently shipped
                </h3>
                <Badge tone="neutral">
                  {shipped.length}
                  <span className="sr-only">{shipped.length === 1 ? " item" : " items"}</span>
                </Badge>
              </header>
              <ul className="m-0 grid list-none gap-2 p-0 md:grid-cols-3">
                {shipped.map((item) => (
                  <li key={item.slug}>
                    <ItemCard item={item} />
                  </li>
                ))}
              </ul>
            </section>
          </div>
        )}
      </section>
      {links.length > 0 && (
        <section aria-labelledby="related" className="mt-10">
          <h2 id="related" className="m-0 text-xl font-semibold text-default">
            Articles and demos
          </h2>
          <EmbedGrid>
            {links.map((link) => (
              <Embed key={link.href} {...link} />
            ))}
          </EmbedGrid>
        </section>
      )}
      {videos.length > 0 && (
        <section aria-labelledby="videos" className="mt-6">
          <h2 id="videos" className="m-0 text-xl font-semibold text-default">
            Videos
          </h2>
          <VideoGrid>
            {videos.map((video) => (
              <Video key={video.id} {...without(video)} />
            ))}
          </VideoGrid>
        </section>
      )}
    </PageFrame>
  );
}
