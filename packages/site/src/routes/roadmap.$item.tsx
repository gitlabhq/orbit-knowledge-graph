import { cn, Icon, NotFound, pageHead, SiteLink } from "@gitlab-org/orbit-site";
import { createFileRoute, notFound } from "@tanstack/react-router";
import { PageView } from "../components/PageView";
import { itemRange } from "../roadmap/ItemCard";
import { findItem } from "../roadmap/model";
import { trackingLabel } from "../roadmap/schema";
import { StatusBadge } from "../roadmap/StatusBadge";

export const Route = createFileRoute("/roadmap/$item")({
  loader: async ({ params }) => {
    const item = findItem(params.item);
    if (!item) throw notFound();
    return { item, body: await item.page.load() };
  },
  head: ({ loaderData }) => pageHead(loaderData?.item.title ?? "Page not found"),
  component: ItemPage,
  notFoundComponent: () => <NotFound />,
});

function linkLabel(url: string) {
  if (url.includes("/merge_requests/")) return "View the merge request";
  if (url.includes("/epics/")) return "Follow the epic";
  if (url.includes("/work_items/") || url.includes("/issues/")) return "Follow the work item";
  if (url.includes("docs.gitlab.com")) return "Read the documentation";
  return "View details";
}

function ItemPage() {
  const { item, body } = Route.useLoaderData();
  const range = itemRange(item);
  return (
    <PageView page={item.page} body={body}>
      <div className={cn("mt-4 flex flex-wrap items-center gap-x-5 gap-y-2 text-sm text-subtle", `tone-${item.theme.id}`)}>
        <StatusBadge status={item.status} />
        <span className="inline-flex items-center gap-1.5">
          <Icon name="calendar" size={14} />
          <span className="font-semibold text-default">{range.label}</span>
          <span>({range.months})</span>
        </span>
        <span className="inline-flex items-center gap-1.5">
          <span className="tone-dot" />
          {item.theme.title}
        </span>
        {item.tracking && (
          <SiteLink href={item.tracking} className="inline-flex items-center gap-1.5 font-medium text-link no-underline hover:underline">
            Tracking: {trackingLabel(item.tracking)}
            <Icon name="external-link" size={12} />
          </SiteLink>
        )}
        {item.link && (
          <SiteLink href={item.link} className="inline-flex items-center gap-1.5 font-medium text-link no-underline hover:underline">
            {linkLabel(item.link)}
            <Icon name="external-link" size={12} />
          </SiteLink>
        )}
      </div>
    </PageView>
  );
}
