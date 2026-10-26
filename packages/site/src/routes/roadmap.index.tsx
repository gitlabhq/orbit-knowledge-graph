import { findPage, Icon, pageHeadOf } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Board } from "../roadmap/Board";
import { formatDate, quarterLabel, quarterMonths } from "../roadmap/calendar";
import { EmptyState } from "../roadmap/EmptyState";
import { ItemCard } from "../roadmap/ItemCard";
import { items, roadmapQuarter, roadmapUpdated, themeById, themes } from "../roadmap/model";
import { EXACT, ThemeFilter } from "../roadmap/ThemeFilter";
import { Timeline } from "../roadmap/Timeline";

type RoadmapSearch = { view?: "board"; theme?: string };

export const Route = createFileRoute("/roadmap/")({
  validateSearch: (search: Record<string, unknown>): RoadmapSearch => ({
    view: search.view === "board" ? "board" : undefined,
    theme: themeById(typeof search.theme === "string" ? search.theme : undefined)?.id,
  }),
  head: () => pageHeadOf(findPage("roadmap")),
  component: RoadmapPage,
});

const VIEW = "gl-button no-underline";
const VIEW_ON = { className: "selected" };

function ViewSwitch() {
  return (
    <div role="group" aria-label="View" className="gl-button-group">
      <Link to="." search={(prev) => ({ ...prev, view: undefined })} activeOptions={EXACT} activeProps={VIEW_ON} className={VIEW}>
        <Icon name="calendar" size={16} />
        Timeline
      </Link>
      <Link to="." search={(prev) => ({ ...prev, view: "board" as const })} activeOptions={EXACT} activeProps={VIEW_ON} className={VIEW}>
        <Icon name="overview" size={16} />
        Board
      </Link>
    </div>
  );
}

function RoadmapPage() {
  const { view, theme } = Route.useSearch();
  const page = findPage("roadmap");
  const filtered = theme ? items.filter((item) => item.theme.id === theme) : items;
  const open = filtered.filter((item) => item.status !== "shipped");
  const shipped = filtered.filter((item) => item.status === "shipped").reverse();
  return (
    <div className="page-container py-8">
      <header className="mb-6 border-b border-border pb-6">
        <div className="flex flex-wrap items-end justify-between gap-6">
          <div className="min-w-0">
            <h1 className="m-0 text-3xl font-semibold text-default">{page?.meta.title}</h1>
            {page?.meta.description && <p className="mt-2 mb-0 max-w-2xl text-base text-subtle">{page.meta.description}</p>}
          </div>
          <ViewSwitch />
        </div>
        <p className="mt-4 mb-0 text-xs text-subtle">
          Updated {formatDate(roadmapUpdated)}. This quarter is {quarterLabel(roadmapQuarter)} ({quarterMonths(roadmapQuarter)}). The GitLab fiscal year starts on February 1. Dates are targets, not commitments.
        </p>
      </header>
      {themes.length > 0 && (
        <div className="mb-6">
          <ThemeFilter />
        </div>
      )}
      {items.length === 0 ? (
        <EmptyState message="The roadmap has no items." reset={false} />
      ) : view === "board" ? (
        open.length > 0 ? <Board items={filtered} /> : <EmptyState message="This theme has no upcoming items." />
      ) : filtered.length > 0 ? (
        <Timeline items={filtered} />
      ) : (
        <EmptyState message="No items match this theme." />
      )}
      {view === "board" && shipped.length > 0 && (
        <section aria-labelledby="shipped" className="mt-14">
          <h2 id="shipped" className="m-0 mb-1 flex items-center gap-2 text-xl font-semibold text-default">
            <Icon name="status-success" className="text-success" />
            Recently shipped
          </h2>
          <p className="mt-0 mb-4 text-sm text-subtle">Work that is available today.</p>
          <ul className="m-0 grid list-none gap-2 p-0 sm:grid-cols-2 lg:grid-cols-3">
            {shipped.map((item) => (
              <li key={item.slug}>
                <ItemCard item={item} />
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}
