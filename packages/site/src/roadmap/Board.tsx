import { Badge } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { HORIZONS, horizon, type RoadmapItem } from "./model";
import { ItemCard } from "./ItemCard";

export function Board({ items, limit, compact = false, headingLevel = 2 }: { items: RoadmapItem[]; limit?: number; compact?: boolean; headingLevel?: 2 | 3 }) {
  const Heading = `h${headingLevel}` as const;
  return (
    <div className="grid gap-8 md:grid-cols-3 md:gap-6">
      {HORIZONS.map((column) => {
        const all = items.filter((item) => horizon(item) === column.id);
        const shown = limit === undefined ? all : all.slice(0, limit);
        return (
          <section key={column.id} aria-labelledby={`horizon-${column.id}`} className="flex flex-col">
            <header className="mb-4 border-b border-border pb-2">
              <div className="flex items-center gap-2">
                <Heading id={`horizon-${column.id}`} className="m-0 text-base font-semibold text-heading">
                  {column.title}
                </Heading>
                <Badge tone="neutral">
                  {all.length}
                  <span className="sr-only">{all.length === 1 ? " item" : " items"}</span>
                </Badge>
              </div>
              <p className="m-0 text-sm text-subtle">{column.description}</p>
            </header>
            <ul className="m-0 flex list-none flex-col gap-2 p-0">
              {shown.map((item) => (
                <li key={item.slug}>
                  <ItemCard item={item} compact={compact} />
                </li>
              ))}
              {all.length === 0 && <li className="py-6 text-center text-sm text-subtle">Nothing here yet.</li>}
            </ul>
            {all.length > shown.length && (
              <Link to="/roadmap" search={{ view: "board" }} className="gl-link mt-4 text-sm">
                View all {all.length} items
              </Link>
            )}
          </section>
        );
      })}
    </div>
  );
}
