import { Badge, cn, Icon } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { quarterFromIndex, quarterIndex, quarterLabel, quarterMonths } from "./calendar";
import { type RoadmapItem, roadmapQuarter, themes } from "./model";
import { statusIcon, statusLabel } from "./StatusBadge";
import { EmptyState } from "./EmptyState";
import { itemRange } from "./ItemCard";

const COLUMN_REM = 11;
const WINDOW_QUARTERS = 4;

function lanes(items: RoadmapItem[]): RoadmapItem[][] {
  const rows: RoadmapItem[][] = [];
  for (const item of items) {
    const row = rows.find((placed) => placed.every((other) => quarterIndex(other.end) < quarterIndex(item.start) || quarterIndex(other.start) > quarterIndex(item.end)));
    if (row) row.push(item);
    else rows.push([item]);
  }
  return rows;
}

export function Timeline({ items }: { items: RoadmapItem[] }) {
  const first = quarterIndex(roadmapQuarter);
  const last = first + WINDOW_QUARTERS - 1;
  const quarters = Array.from({ length: WINDOW_QUARTERS }, (_, offset) => quarterFromIndex(first + offset));
  const visible = items.filter((item) => quarterIndex(item.end) >= first && quarterIndex(item.start) <= last);
  const columns = { gridTemplateColumns: `repeat(${quarters.length}, minmax(${COLUMN_REM}rem, 1fr))` };
  if (visible.length === 0) return <EmptyState message="No items match this theme in these quarters." />;

  return (
    <div className="card relative overflow-hidden">
      <div className="overflow-x-auto">
        <div className="relative" style={{ minWidth: `${quarters.length * COLUMN_REM}rem` }}>
          <div className="pointer-events-none absolute inset-0 grid" style={columns} aria-hidden="true">
            {quarters.map((q) => (
              <span
                key={quarterLabel(q)}
                className={cn("border-l border-border first:border-l-0", quarterIndex(q) === first && "bg-surface-subtle")}
              />
            ))}
          </div>
          <div className="grid border-b border-border" style={columns}>
            {quarters.map((q) => (
              <div key={quarterLabel(q)} className="relative px-4 py-2">
                <span className="flex items-center gap-2 text-sm font-semibold text-default">
                  {quarterLabel(q)}
                  {quarterIndex(q) === first && <Badge tone="info">Now</Badge>}
                </span>
                <span className="block text-xs text-subtle">{quarterMonths(q)}</span>
              </div>
            ))}
          </div>
          {themes.map((theme) => {
            const themed = visible.filter((item) => item.theme.id === theme.id);
            if (themed.length === 0) return null;
            return (
              <section key={theme.id} aria-labelledby={`lane-${theme.id}`} className={cn("relative border-b border-border pb-2 last:border-b-0", `tone-${theme.id}`)}>
                <h2 id={`lane-${theme.id}`} className="sticky left-0 m-0 inline-flex items-center gap-2 px-4 pt-4 pb-2 text-xs font-semibold text-subtle">
                  <Icon name={theme.icon} size={16} />
                  {theme.title}
                </h2>
                {lanes(themed).map((row) => (
                  <div key={row.map((item) => item.slug).join()} className="grid items-start py-1" style={columns}>
                    {row.map((item) => (
                      <Link
                        key={item.slug}
                        to="/roadmap/$item"
                        params={{ item: item.slug }}
                        data-status={item.status}
                        className="tone-bar mx-2 flex min-w-0 items-start gap-2 rounded-control px-2 py-2 text-sm text-default no-underline hover:underline"
                        style={{ gridColumn: `${Math.max(quarterIndex(item.start), first) - first + 1} / ${Math.min(quarterIndex(item.end), last) - first + 2}` }}
                      >
                        <Icon name={statusIcon(item.status)} size={12} className="mt-1 shrink-0 text-[var(--tone)]" />
                        <span className="line-clamp-2">{item.title}</span>
                        <span className="sr-only">
                          , {statusLabel(item.status)}, {itemRange(item).months}
                        </span>
                      </Link>
                    ))}
                  </div>
                ))}
              </section>
            );
          })}
        </div>
      </div>
      <div aria-hidden="true" className="pointer-events-none absolute inset-y-0 right-0 w-8 bg-linear-to-l from-page to-transparent md:hidden" />
    </div>
  );
}
