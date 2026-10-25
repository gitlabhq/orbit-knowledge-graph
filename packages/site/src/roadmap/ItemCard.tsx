import { cn } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { quarterRange } from "./calendar";
import type { RoadmapItem } from "./model";
import { StatusBadge } from "./StatusBadge";

export const itemRange = (item: RoadmapItem) => quarterRange(item.start, item.end);

export function ItemCard({ item, compact = false }: { item: RoadmapItem; compact?: boolean }) {
  const range = itemRange(item);
  return (
    <Link
      to="/roadmap/$item"
      params={{ item: item.slug }}
      className={cn("card group flex h-full flex-col gap-2.5 p-4 no-underline", `tone-${item.theme.id}`)}
    >
      <span className="flex items-center gap-2 text-xs text-subtle">
        <span className="tone-dot" />
        <span className="truncate">{item.theme.title}</span>
        <span className="ml-auto shrink-0 font-mono" title={range.months}>
          {range.label}
        </span>
      </span>
      <span className="text-[15px] leading-snug font-semibold text-default">{item.title}</span>
      {!compact && <span className="line-clamp-2 text-sm text-subtle">{item.description}</span>}
      <StatusBadge status={item.status} className="mt-auto self-start" />
    </Link>
  );
}
