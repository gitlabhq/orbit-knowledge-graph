import { Icon } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { quarterRange } from "./calendar";
import type { RoadmapItem } from "./model";
import { StatusBadge } from "./StatusBadge";

export const itemRange = (item: RoadmapItem) => quarterRange(item.start, item.end);

// Cards are frosted glass, on the board and elsewhere.

export function ItemCard({ item }: { item: RoadmapItem }) {
  return (
    <Link to="/roadmap/$item" params={{ item: item.slug }} className="card glass flex h-full flex-col gap-2 px-4 pt-3.5 pb-3">
      <span className="card-title line-clamp-2 text-[0.8125rem] font-semibold text-default">{item.title}</span>
      <span className="line-clamp-2 text-[0.8125rem] text-subtle">{item.description}</span>
      <span className="mt-auto flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-subtle">
        <StatusBadge status={item.status} iconSize="sm" />
        <span className="inline-flex items-center gap-1">
          <Icon name={item.theme.icon} size={12} className="tile-icon" />
          {item.theme.title}
        </span>
      </span>
    </Link>
  );
}
