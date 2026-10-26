import { parseQuarter, type Quarter, quarterIndex } from "./calendar";

export const STATUSES = ["shipped", "in-progress", "planned", "exploring"] as const;
export type Status = (typeof STATUSES)[number];
export type Horizon = "now" | "next" | "later";

export type ItemFields = { theme: string; status: Status; start: string; end: string; link?: string; tracking?: string };

type Frontmatter = Record<string, unknown>;

export function itemFields(data: Frontmatter, themes: string[]): ItemFields {
  if (typeof data.description !== "string") throw new Error("a roadmap item needs a description");
  if (!themes.includes(String(data.theme))) throw new Error(`theme must be one of ${themes.join(", ")}, got ${JSON.stringify(data.theme)}`);
  if (!STATUSES.includes(data.status as Status)) throw new Error(`status must be one of ${STATUSES.join(", ")}, got ${JSON.stringify(data.status)}`);
  const end = data.end ?? data.start;
  if (quarterIndex(parseQuarter(end)) < quarterIndex(parseQuarter(data.start))) throw new Error(`end ${String(end)} is before start ${String(data.start)}`);
  if (data.link !== undefined && !String(data.link).startsWith("https://")) throw new Error(`link must be an https URL, got ${JSON.stringify(data.link)}`);
  if (data.tracking !== undefined && !String(data.tracking).startsWith("https://gitlab.com/")) throw new Error(`tracking must be a https://gitlab.com/ URL, got ${JSON.stringify(data.tracking)}`);
  return {
    theme: String(data.theme),
    status: data.status as Status,
    start: String(data.start),
    end: String(end),
    ...(data.link === undefined ? {} : { link: String(data.link) }),
    ...(data.tracking === undefined ? {} : { tracking: String(data.tracking) }),
  };
}

export function horizonOf(status: Exclude<Status, "shipped">, start: Quarter, now: Quarter): Horizon {
  if (status === "in-progress") return "now";
  if (status === "exploring") return "later";
  return quarterIndex(start) <= quarterIndex(now) + 1 ? "next" : "later";
}

export type TrackingRef = { namespace: string; iid: string; label: string };

export function trackingRef(url: string): TrackingRef | undefined {
  const { pathname } = new URL(url);
  const group = /^\/groups\/(.+)\/-\/(?:epics|work_items)\/(\d+)/.exec(pathname);
  if (group?.[1] && group[2]) return { namespace: group[1], iid: group[2], label: `${group[1].split("/").pop()}&${group[2]}` };
  const project = /^\/(.+)\/-\/(?:issues|work_items)\/(\d+)/.exec(pathname);
  if (project?.[1] && project[2]) return { namespace: project[1], iid: project[2], label: `${project[1].split("/").pop()}#${project[2]}` };
  return undefined;
}

export const trackingLabel = (url: string) => trackingRef(url)?.label ?? "Tracking work item";
