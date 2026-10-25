import { type Page, pages } from "@gitlab-org/orbit-site";
import data from "@data/roadmap/roadmap.yaml";
import trackers from "@data/roadmap/trackers.yaml";
import type { Tracker } from "../../build/trackers";
import { parseQuarter, type Quarter, quarterIndex, quarterOf } from "./calendar";
import { type Horizon, horizonOf, type Status } from "./schema";

export const HORIZONS: { id: Horizon; title: string; description: string }[] = [
  { id: "now", title: "Now", description: "In progress" },
  { id: "next", title: "Next", description: "Planned soon" },
  { id: "later", title: "Later", description: "Further out, or in exploration" },
];

export type RoadmapTheme = { id: string; title: string; description: string; icon: string };

export type RoadmapItem = {
  slug: string;
  page: Page;
  title: string;
  description: string;
  theme: RoadmapTheme;
  status: Status;
  start: Quarter;
  end: Quarter;
  link?: string;
  tracking?: string;
  tracker?: Tracker;
};

const roadmap = data as { updated: string; themes: RoadmapTheme[] };

export const themes = roadmap.themes;
export const roadmapUpdated = roadmap.updated;
export const roadmapQuarter = quarterOf(new Date(`${roadmapUpdated}T00:00:00Z`));

const themeIds = new Map(themes.map((theme) => [theme.id, theme]));

const STATUS_ORDER: Record<Status, number> = { "in-progress": 0, planned: 1, exploring: 2, shipped: 3 };

export const items: RoadmapItem[] = pages
  .flatMap((page) => (page.meta.item ? [{ page, ...page.meta.item }] : []))
  .map(({ page, theme, status, start, end, link, tracking }) => ({
    slug: page.path.slice("roadmap/".length),
    page,
    title: page.meta.title,
    description: page.meta.description ?? "",
    theme: themeIds.get(theme)!,
    status,
    start: parseQuarter(start),
    end: parseQuarter(end),
    link,
    tracking,
    tracker: tracking ? (trackers as Record<string, Tracker>)[tracking] : undefined,
  }))
  .sort(
    (a, b) =>
      quarterIndex(a.start) - quarterIndex(b.start) ||
      STATUS_ORDER[a.status] - STATUS_ORDER[b.status] ||
      themes.indexOf(a.theme) - themes.indexOf(b.theme) ||
      a.title.localeCompare(b.title),
  );

export const horizon = (item: RoadmapItem): Horizon | "shipped" =>
  item.status === "shipped" ? "shipped" : horizonOf(item.status, item.start, roadmapQuarter);

const bySlug = new Map(items.map((item) => [item.slug, item]));

export const findItem = (slug: string) => bySlug.get(slug);

export const themeById = (id: string | undefined) => (id ? themeIds.get(id) : undefined);
