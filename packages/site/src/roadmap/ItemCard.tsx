import { Icon } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { quarterRange } from "./calendar";
import type { RoadmapItem } from "./model";
import { StatusBadge } from "./StatusBadge";

export const itemRange = (item: RoadmapItem) => quarterRange(item.start, item.end);

export function ItemCard({ item, compact = false }: { item: RoadmapItem; compact?: boolean }) {
  const range = itemRange(item);
  return (
    <Link to="/roadmap/$item" params={{ item: item.slug }} className="card flex h-full flex-col gap-2 p-4">
      <span className="card-title line-clamp-2 text-sm font-semibold text-default">{item.title}</span>
      {!compact && <span className="line-clamp-2 text-sm text-subtle">{item.description}</span>}
      <span className="mt-auto flex flex-wrap items-center gap-2 text-xs text-subtle">
        <StatusBadge status={item.status} iconSize="sm" />
        <span title={range.months}>{range.label}</span>
        <span className="inline-flex items-center gap-1">
          <Icon name={item.theme.icon} size={16} />
          {item.theme.title}
        </span>
      </span>
    </Link>
  );
}
