import { parseQuarter, type Quarter, quarterIndex } from "./calendar";

export const STATUSES = ["shipped", "in-progress", "planned", "exploring"] as const;
export type Status = (typeof STATUSES)[number];
export type Horizon = "now" | "next" | "later";

export type ItemFields = { theme: string; status: Status; start: string; end: string; link?: string };
export type PostFields = { date: string; authors: string[]; tags: string[] };

type Frontmatter = Record<string, unknown>;

const list = (value: unknown) => [value ?? []].flat().map(String);

export function itemFields(data: Frontmatter, themes: string[]): ItemFields {
  if (typeof data.description !== "string") throw new Error("a roadmap item needs a description");
  if (!themes.includes(String(data.theme))) throw new Error(`theme must be one of ${themes.join(", ")}, got ${JSON.stringify(data.theme)}`);
  if (!STATUSES.includes(data.status as Status)) throw new Error(`status must be one of ${STATUSES.join(", ")}, got ${JSON.stringify(data.status)}`);
  const end = data.end ?? data.start;
  if (quarterIndex(parseQuarter(end)) < quarterIndex(parseQuarter(data.start))) throw new Error(`end ${String(end)} is before start ${String(data.start)}`);
  if (data.link !== undefined && !String(data.link).startsWith("https://")) throw new Error(`link must be an https URL, got ${JSON.stringify(data.link)}`);
  return {
    theme: String(data.theme),
    status: data.status as Status,
    start: String(data.start),
    end: String(end),
    ...(data.link === undefined ? {} : { link: String(data.link) }),
  };
}

export function postFields(data: Frontmatter, authors: string[]): PostFields {
  const date = String(data.date ?? "");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error(`date must look like 2026-10-01, got ${JSON.stringify(data.date)}`);
  const names = list(data.authors);
  if (names.length === 0) throw new Error("authors must list at least one author");
  const unknown = names.filter((name) => !authors.includes(name));
  if (unknown.length > 0) throw new Error(`unknown authors ${unknown.join(", ")}; add them to data/authors.yaml`);
  return { date, authors: names, tags: list(data.tags) };
}

export function horizonOf(status: Exclude<Status, "shipped">, start: Quarter, now: Quarter): Horizon {
  if (status === "in-progress") return "now";
  if (status === "exploring") return "later";
  return quarterIndex(start) <= quarterIndex(now) + 1 ? "next" : "later";
}
