import { cn, Icon } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { quarterRange } from "./calendar";
import type { RoadmapItem } from "./model";
import { StatusBadge } from "./StatusBadge";

export const itemRange = (item: RoadmapItem) => quarterRange(item.start, item.end);

// On a board, a white card with no border and a faint lift. Elsewhere, a quiet tile.
const ON_BOARD = "border-transparent bg-page shadow-[0_1px_2px_rgb(0_0_0/0.06)]";

export function ItemCard({ item, compact = false, onBoard = false }: { item: RoadmapItem; compact?: boolean; onBoard?: boolean }) {
  const range = itemRange(item);
  return (
    <Link to="/roadmap/$item" params={{ item: item.slug }} className={cn("card flex h-full flex-col gap-2 px-4 pt-3.5 pb-3", onBoard ? ON_BOARD : "tile-quiet")}>
      <span className="card-title line-clamp-2 text-[0.8125rem] font-semibold text-default">{item.title}</span>
      {!compact && <span className="line-clamp-2 text-[0.8125rem] text-subtle">{item.description}</span>}
      <span className="mt-auto flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-subtle">
        <StatusBadge status={item.status} iconSize="sm" />
        <span title={range.months} className="tabular-nums">
          {range.label}
        </span>
        <span className="inline-flex items-center gap-1">
          <Icon name={item.theme.icon} size={12} className="tile-icon" />
          {item.theme.title}
        </span>
      </span>
    </Link>
  );
}
