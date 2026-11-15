import { cn, Icon, SiteLink } from "@gitlab-org/orbit-site";
import type { RoadmapItem } from "./model";
import { StatusBadge } from "./StatusBadge";

// Cards are frosted glass, on the board and elsewhere. A card links to the item's docs, or to the work
// item that tracks it, when it has one.
export function ItemCard({ item }: { item: RoadmapItem }) {
  const href = item.link ?? item.tracking;
  const body = (
    <>
      <span className="flex items-start justify-between gap-2">
        <span className="card-title text-[0.8125rem] font-semibold text-default">{item.title}</span>
        {href && <Icon name="external-link" size={12} className="mt-1 shrink-0 text-subtle" />}
      </span>
      <span className="text-[0.8125rem] text-subtle">{item.description}</span>
      <span className="mt-auto flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-subtle">
        <StatusBadge status={item.status} iconSize="sm" />
        <span className="inline-flex items-center gap-1">
          <Icon name={item.theme.icon} size={12} className="tile-icon" />
          {item.theme.title}
        </span>
      </span>
    </>
  );
  const className = cn("card glass flex h-full flex-col gap-2 px-4 pt-3.5 pb-3", href && "no-underline");
  return href ? (
    <SiteLink href={href} className={className}>
      {body}
    </SiteLink>
  ) : (
    <div className={className}>{body}</div>
  );
}
