export type Quarter = { fiscalYear: number; quarter: number };

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

export function parseQuarter(value: unknown): Quarter {
  const match = typeof value === "string" ? /^FY(\d{2}) Q([1-4])$/.exec(value) : null;
  if (!match) throw new Error(`Expected a quarter like "FY27 Q3", got ${JSON.stringify(value)}`);
  return { fiscalYear: Number(match[1]), quarter: Number(match[2]) };
}

export const quarterIndex = (q: Quarter) => q.fiscalYear * 4 + q.quarter - 1;

export const quarterFromIndex = (index: number): Quarter => ({ fiscalYear: Math.floor(index / 4), quarter: (index % 4) + 1 });

export const quarterLabel = (q: Quarter) => `FY${q.fiscalYear} Q${q.quarter}`;

const monthOf = (q: Quarter, offset: number) => new Date(Date.UTC(2000 + q.fiscalYear - 1, 1 + (q.quarter - 1) * 3 + offset, 1));

const monthYear = (date: Date) => `${MONTHS[date.getUTCMonth()]} ${date.getUTCFullYear()}`;

export function quarterMonths(q: Quarter): string {
  const first = monthOf(q, 0);
  const last = monthOf(q, 2);
  return first.getUTCFullYear() === last.getUTCFullYear()
    ? `${MONTHS[first.getUTCMonth()]}–${monthYear(last)}`
    : `${monthYear(first)}–${monthYear(last)}`;
}

export function quarterRange(start: Quarter, end: Quarter): { label: string; months: string } {
  if (quarterIndex(start) === quarterIndex(end)) return { label: quarterLabel(start), months: quarterMonths(start) };
  return { label: `${quarterLabel(start)}–${quarterLabel(end)}`, months: `${monthYear(monthOf(start, 0))}–${monthYear(monthOf(end, 2))}` };
}

export function quarterOf(date: Date): Quarter {
  const fiscalMonth = (date.getUTCMonth() + 11) % 12;
  const fiscalYear = date.getUTCFullYear() - 2000 + (date.getUTCMonth() >= 1 ? 1 : 0);
  return { fiscalYear, quarter: Math.floor(fiscalMonth / 3) + 1 };
}

export const formatDate = (iso: string) =>
  new Date(`${iso}T00:00:00Z`).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
