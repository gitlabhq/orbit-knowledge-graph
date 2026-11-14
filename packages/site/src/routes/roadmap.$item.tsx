import { Badge, Icon, NotFound, PageContent, pageHead, SiteLink, type Tone, useHeadingLinks } from "@gitlab-org/orbit-site";
import { createFileRoute, notFound } from "@tanstack/react-router";
import { useRef } from "react";
import { Frame } from "../components/Frame";
import { PageFooter } from "../components/PageFooter";
import type { Health, Tracker } from "../../build/trackers";
import { formatDate } from "../roadmap/calendar";
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

const HEALTH: Record<Health, { label: string; tone: Tone }> = {
  onTrack: { label: "On track", tone: "success" },
  needsAttention: { label: "Needs attention", tone: "warning" },
  atRisk: { label: "At risk", tone: "danger" },
};

// A shipped item can keep an open tracker with a health status. That health is about the remaining
// work item, not the shipped feature, so a shipped item does not show it.
function TrackerStatus({ tracker, shipped }: { tracker: Tracker; shipped: boolean }) {
  return (
    <>
      {tracker.state === "CLOSED" && <Badge tone="neutral">Closed</Badge>}
      {tracker.health && !shipped && <Badge tone={HEALTH[tracker.health].tone}>{HEALTH[tracker.health].label}</Badge>}
      {tracker.dueDate && <span>Due {formatDate(tracker.dueDate)}</span>}
    </>
  );
}

function ItemPage() {
  const { item, body } = Route.useLoaderData();
  const range = itemRange(item);
  const content = useRef<HTMLDivElement>(null);
  useHeadingLinks(content, item.page.path);
  return (
    <Frame
      title={item.title}
      description={item.description}
      footer={<PageFooter page={item.page} />}
      header={
        <div className="mt-4 flex flex-wrap items-center gap-x-6 gap-y-2 text-sm text-subtle">
          <StatusBadge status={item.status} />
          <span className="inline-flex items-center gap-1.5">
            <Icon name="calendar" size={16} className="tile-icon" />
            <span className="font-semibold text-default">{range.label}</span>
            <span>({range.months})</span>
          </span>
          <span className="inline-flex items-center gap-1.5">
            <Icon name={item.theme.icon} size={16} className="tile-icon" />
            {item.theme.title}
          </span>
          {item.tracking && (
            <SiteLink href={item.tracking} className="gl-link inline-flex items-center gap-1.5">
              Tracking: {trackingLabel(item.tracking)}
              <Icon name="external-link" size={12} />
            </SiteLink>
          )}
          {item.tracker && <TrackerStatus tracker={item.tracker} shipped={item.status === "shipped"} />}
          {item.link && (
            <SiteLink href={item.link} className="gl-link inline-flex items-center gap-1.5">
              {linkLabel(item.link)}
              <Icon name="external-link" size={12} />
            </SiteLink>
          )}
        </div>
      }
    >
      <div ref={content}>
        <PageContent body={body} />
      </div>
    </Frame>
  );
}
