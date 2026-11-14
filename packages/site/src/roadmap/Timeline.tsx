import { cn, Icon } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { quarterFromIndex, quarterIndex, quarterLabel, quarterMonths } from "./calendar";
import { EmptyState } from "./EmptyState";
import { itemRange } from "./ItemCard";
import { type RoadmapItem, roadmapQuarter, themes } from "./model";
import { statusIcon, statusLabel } from "./StatusBadge";

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

// Like the Orbit Portal timeline: the quarters sit on a gray track, and the rows are one white card
// on it. Bars take the theme color with the kit's topic fill and edge. Exploring items have a dashed
// edge, and shipped items are faded.
export function Timeline({ items }: { items: RoadmapItem[] }) {
  const first = quarterIndex(roadmapQuarter);
  const last = first + WINDOW_QUARTERS - 1;
  const quarters = Array.from({ length: WINDOW_QUARTERS }, (_, offset) => quarterFromIndex(first + offset));
  const visible = items.filter((item) => quarterIndex(item.end) >= first && quarterIndex(item.start) <= last);
  const columns = { gridTemplateColumns: `repeat(${quarters.length}, minmax(${COLUMN_REM}rem, 1fr))` };
  const width = { minWidth: `${quarters.length * COLUMN_REM}rem` };
  if (visible.length === 0) return <EmptyState message="No items match this theme in these quarters." />;

  return (
    <div className="board-surface relative rounded-[var(--radius-card)] bg-[var(--board-surface)] p-2">
      <div className="overflow-x-auto">
        <div style={width}>
          <div className="mb-2 grid" style={columns}>
            {quarters.map((q) => (
              <div key={quarterLabel(q)} className="px-3 pt-1">
                <span className="flex items-center gap-2 text-base font-semibold text-heading">
                  {quarterLabel(q)}
                  {quarterIndex(q) === first && (
                    <span className="rounded bg-page px-1.5 text-[0.56rem] leading-3.5 font-bold tracking-wider text-default uppercase">Now</span>
                  )}
                </span>
                <span className="block text-xs text-subtle tabular-nums">{quarterMonths(q)}</span>
              </div>
            ))}
          </div>
          <div className="relative overflow-hidden rounded-[var(--radius-card)] bg-page">
            <div className="pointer-events-none absolute inset-0 grid" style={columns} aria-hidden="true">
              {quarters.map((q) => (
                <span key={quarterLabel(q)} className="border-l border-[var(--gl-border-color-subtle)] first:border-l-0" />
              ))}
            </div>
            {themes.map((theme) => {
              const themed = visible.filter((item) => item.theme.id === theme.id);
              if (themed.length === 0) return null;
              return (
                <section key={theme.id} aria-labelledby={`lane-${theme.id}`} className={cn("relative border-b border-[var(--gl-border-color-subtle)] pb-2 last:border-b-0", `tone-${theme.id}`)}>
                  <h2 id={`lane-${theme.id}`} className="sticky left-0 m-0 inline-flex items-center gap-2 px-4 pt-3 pb-1 text-sm font-semibold text-default">
                    <Icon name={theme.icon} size={14} className="tile-icon" />
                    {theme.title}
                  </h2>
                  {lanes(themed).map((row) => (
                    <div key={row.map((item) => item.slug).join()} className="grid items-start py-1" style={columns}>
                      {row.map((item) => (
                        <Link
                          key={item.slug}
                          to="/roadmap/$item"
                          params={{ item: item.slug }}
                          className={cn(
                            "topic-fill topic-edge mx-2 flex min-w-0 items-start gap-2 rounded px-2 py-1.5 text-[0.8125rem] text-default no-underline hover:underline",
                            item.status === "shipped" && "opacity-60",
                          )}
                          style={{
                            gridColumn: `${Math.max(quarterIndex(item.start), first) - first + 1} / ${Math.min(quarterIndex(item.end), last) - first + 2}`,
                            ...(item.status === "exploring" && { borderStyle: "dashed" }),
                          }}
                        >
                          <Icon name={statusIcon(item.status)} size={12} className="mt-1 shrink-0 text-subtle" />
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
      </div>
    </div>
  );
}
