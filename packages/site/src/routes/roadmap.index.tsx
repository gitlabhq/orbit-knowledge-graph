import { Badge, findPage, PageFrame, pageHeadOf } from "@gitlab-org/orbit-site";
import { createFileRoute } from "@tanstack/react-router";
import { PageFooter } from "../components/PageFooter";
import { Board } from "../roadmap/Board";
import { quarterIndex } from "../roadmap/calendar";
import { EmptyState } from "../roadmap/EmptyState";
import { ItemCard } from "../roadmap/ItemCard";
import { items, themeById, themes } from "../roadmap/model";
import { ThemeFilter } from "../roadmap/ThemeFilter";

type RoadmapSearch = { theme?: string };

export const Route = createFileRoute("/roadmap/")({
  validateSearch: (search: Record<string, unknown>): RoadmapSearch => ({
    theme: themeById(typeof search.theme === "string" ? search.theme : undefined)?.id,
  }),
  head: () => pageHeadOf(findPage("roadmap")),
  component: RoadmapPage,
});

function RoadmapPage() {
  const { theme } = Route.useSearch();
  const page = findPage("roadmap");
  const filtered = theme ? items.filter((item) => item.theme.id === theme) : items;
  const open = filtered.filter((item) => item.status !== "shipped");
  const shipped = filtered.filter((item) => item.status === "shipped").sort((a, b) => quarterIndex(b.end) - quarterIndex(a.end));
  return (
    <PageFrame
      path="roadmap"
      title={page?.meta.title ?? "Roadmap"}
      description={page?.meta.description}
      options={{ hideDescription: true }}
      footer={page && <PageFooter page={page} />}
    >
      {themes.length > 0 && (
        <div className="not-prose -mt-3 mb-6 flex flex-col items-start gap-2">
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
              <h2 id="shipped" className="m-0 text-base font-semibold text-heading">
                Recently shipped
              </h2>
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
    </PageFrame>
  );
}
