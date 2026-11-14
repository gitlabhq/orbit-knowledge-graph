import { cn, findPage, Icon, pageHeadOf } from "@gitlab-org/orbit-site";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Frame } from "../components/Frame";
import { PageFooter } from "../components/PageFooter";
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

const VIEWS = [
  { id: undefined, label: "Timeline", icon: "calendar" },
  { id: "board" as const, label: "Board", icon: "overview" },
];

// One switcher, as in the Orbit Portal: a gray track with the open view raised on it.
function ViewSwitch({ view }: { view?: "board" }) {
  return (
    <nav aria-label="Roadmap view" className="inline-flex items-center gap-0.5 rounded-[var(--gl-nav-item-border-radius)] bg-[var(--gl-background-color-strong)] p-0.5">
      {VIEWS.map((option) => (
        <Link
          key={option.label}
          to="."
          search={(prev) => ({ ...prev, view: option.id })}
          activeOptions={EXACT}
          className={cn(
            "inline-flex items-center gap-1.5 rounded-[calc(var(--gl-nav-item-border-radius)-2px)] px-2.5 py-1 text-xs text-subtle no-underline transition-colors hover:text-default",
            view === option.id && "bg-page text-default shadow-[0_1px_2px_rgb(0_0_0/0.08)]",
          )}
        >
          <Icon name={option.icon} size={12} />
          {option.label}
        </Link>
      ))}
    </nav>
  );
}

function RoadmapPage() {
  const { view, theme } = Route.useSearch();
  const page = findPage("roadmap");
  const filtered = theme ? items.filter((item) => item.theme.id === theme) : items;
  const open = filtered.filter((item) => item.status !== "shipped");
  const shipped = filtered.filter((item) => item.status === "shipped").reverse();
  return (
    <Frame
      title={page?.meta.title ?? "Roadmap"}
      description={page?.meta.description}
      header={
        <p className="mt-3 mb-0 text-xs text-subtle">
          Updated {formatDate(roadmapUpdated)}. This quarter is {quarterLabel(roadmapQuarter)} ({quarterMonths(roadmapQuarter)}). The GitLab fiscal year starts on February 1. Dates are targets, not commitments.
        </p>
      }
      footer={page && <PageFooter page={page} />}
    >
      <div className="not-prose -mt-3 mb-6 flex flex-col items-start gap-2">
        <ViewSwitch view={view} />
        {themes.length > 0 && <ThemeFilter theme={theme} />}
      </div>
      <div className="page-bleed">
        {items.length === 0 ? (
          <EmptyState message="The roadmap has no items." reset={false} />
        ) : view === "board" ? (
          open.length > 0 ? <Board items={filtered} /> : <EmptyState message="This theme has no upcoming items." />
        ) : (
          <Timeline items={filtered} />
        )}
        {view === "board" && shipped.length > 0 && (
          <section aria-labelledby="shipped" className="mt-12">
            <h2 id="shipped" className="m-0 mb-1 flex items-center gap-2 text-xl font-semibold text-default">
              <Icon name="status-success" className="text-success" />
              Recently shipped
            </h2>
            <p className="mt-0 mb-4 text-sm text-subtle">Work that is available today.</p>
            <ul className="m-0 grid list-none gap-3 p-0 sm:grid-cols-2 lg:grid-cols-3">
              {shipped.map((item) => (
                <li key={item.slug}>
                  <ItemCard item={item} />
                </li>
              ))}
            </ul>
          </section>
        )}
      </div>
    </Frame>
  );
}
