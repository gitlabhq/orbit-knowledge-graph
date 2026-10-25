import { cn } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { HORIZONS, horizon, type RoadmapItem } from "./model";
import { ItemCard } from "./ItemCard";

export function Board({ items, limit, compact = false, headingLevel = 2 }: { items: RoadmapItem[]; limit?: number; compact?: boolean; headingLevel?: 2 | 3 }) {
  const Heading = `h${headingLevel}` as const;
  return (
    <div className="grid gap-4 md:grid-cols-3">
      {HORIZONS.map((column, index) => {
        const all = items.filter((item) => horizon(item) === column.id);
        const shown = limit === undefined ? all : all.slice(0, limit);
        return (
          <section key={column.id} aria-labelledby={`horizon-${column.id}`} className="flex flex-col rounded-[1rem] bg-surface-subtle p-3">
            <header className="flex items-baseline gap-2 px-1 pt-1 pb-3">
              <span
                aria-hidden="true"
                className={cn("size-2 self-center rounded-full", index === 0 ? "bg-[var(--gl-status-info-icon-color)] shadow-[0_0_0_4px_var(--gl-status-info-background-color)]" : "bg-border-strong")}
              />
              <Heading id={`horizon-${column.id}`} className="m-0 text-base font-semibold text-default">
                {column.title}
              </Heading>
              <span className="text-xs text-subtle">{column.description}</span>
              <span className="ml-auto rounded-full bg-page px-2 py-0.5 text-xs font-semibold text-subtle">{all.length}</span>
            </header>
            <ul className="m-0 flex list-none flex-col gap-2.5 p-0">
              {shown.map((item) => (
                <li key={item.slug}>
                  <ItemCard item={item} compact={compact} />
                </li>
              ))}
              {all.length === 0 && <li className="px-1 py-6 text-center text-sm text-subtle">Nothing here yet.</li>}
            </ul>
            {all.length > shown.length && (
              <Link to="/roadmap" search={{ view: "board" }} className="mt-auto px-1 pt-3 text-sm font-semibold no-underline hover:underline">
                View all {all.length}
              </Link>
            )}
          </section>
        );
      })}
    </div>
  );
}
