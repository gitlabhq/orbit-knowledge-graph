import { cn, Icon } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { useEffect, useRef } from "react";
import { quarterFromIndex, quarterIndex, quarterLabel, quarterMonths } from "./calendar";
import { type RoadmapItem, roadmapQuarter, themes } from "./model";
import { statusIcon, statusLabel } from "./StatusBadge";
import { itemRange } from "./ItemCard";

const COLUMN_REM = 11;

function lanes(items: RoadmapItem[]): RoadmapItem[][] {
  const rows: RoadmapItem[][] = [];
  for (const item of items) {
    const row = rows.find((placed) => placed.every((other) => quarterIndex(other.end) < quarterIndex(item.start) || quarterIndex(other.start) > quarterIndex(item.end)));
    if (row) row.push(item);
    else rows.push([item]);
  }
  return rows;
}

export function Timeline({ items, allItems }: { items: RoadmapItem[]; allItems: RoadmapItem[] }) {
  const scroller = useRef<HTMLDivElement>(null);
  const first = Math.min(...allItems.map((item) => quarterIndex(item.start)));
  const last = Math.max(...allItems.map((item) => quarterIndex(item.end)));
  const quarters = Array.from({ length: last - first + 1 }, (_, offset) => quarterFromIndex(first + offset));
  const now = quarterIndex(roadmapQuarter);
  const columns = { gridTemplateColumns: `repeat(${quarters.length}, minmax(${COLUMN_REM}rem, 1fr))` };

  useEffect(() => {
    const element = scroller.current;
    if (!element || element.scrollWidth <= element.clientWidth) return;
    element.scrollLeft = (now - first) * COLUMN_REM * parseFloat(getComputedStyle(document.documentElement).fontSize);
  }, [now, first]);

  return (
    <div className="card relative overflow-hidden">
      <div ref={scroller} className="overflow-x-auto">
        <div className="relative" style={{ minWidth: `${quarters.length * COLUMN_REM}rem` }}>
          <div className="pointer-events-none absolute inset-0 grid" style={columns} aria-hidden="true">
            {quarters.map((q) => (
              <span
                key={quarterLabel(q)}
                className={cn("border-l border-border first:border-l-0", quarterIndex(q) === now && "bg-[color-mix(in_srgb,var(--gl-status-info-background-color)_45%,transparent)]")}
              />
            ))}
          </div>
          <div className="grid border-b border-border" style={columns}>
            {quarters.map((q) => (
              <div key={quarterLabel(q)} className="relative px-4 py-3">
                <span className="flex items-center gap-2 text-sm font-semibold text-default">
                  {quarterLabel(q)}
                  {quarterIndex(q) === now && <span className="rounded-full bg-info-bg px-1.5 text-xs font-semibold text-info">Now</span>}
                </span>
                <span className="block text-xs text-subtle">{quarterMonths(q)}</span>
              </div>
            ))}
          </div>
          {themes.map((theme) => {
            const themed = items.filter((item) => item.theme.id === theme.id);
            if (themed.length === 0) return null;
            return (
              <section key={theme.id} aria-labelledby={`lane-${theme.id}`} className={cn("relative border-b border-border pb-3 last:border-b-0", `tone-${theme.id}`)}>
                <h2 id={`lane-${theme.id}`} className="sticky left-0 m-0 inline-flex items-center gap-2 px-4 pt-4 pb-2 text-xs font-semibold text-subtle">
                  <span className="tone-dot" />
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
                        className="tone-bar mx-2 flex min-w-0 items-start gap-2 rounded-control px-3 py-2 text-sm leading-snug font-medium text-default no-underline transition-transform hover:-translate-y-px hover:shadow-card motion-reduce:transition-none"
                        style={{ gridColumn: `${quarterIndex(item.start) - first + 1} / ${quarterIndex(item.end) - first + 2}` }}
                      >
                        <Icon name={statusIcon(item.status)} size={14} className="mt-0.5 shrink-0 text-[var(--tone)]" />
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
