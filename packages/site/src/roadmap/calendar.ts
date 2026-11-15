export type Quarter = { fiscalYear: number; quarter: number };

export function parseQuarter(value: unknown): Quarter {
  const match = typeof value === "string" ? /^FY(\d{2}) Q([1-4])$/.exec(value) : null;
  if (!match) throw new Error(`Expected a quarter like "FY27 Q3", got ${JSON.stringify(value)}`);
  return { fiscalYear: Number(match[1]), quarter: Number(match[2]) };
}

export const quarterIndex = (q: Quarter) => q.fiscalYear * 4 + q.quarter - 1;

export function quarterOf(date: Date): Quarter {
  const fiscalMonth = (date.getUTCMonth() + 11) % 12;
  const fiscalYear = date.getUTCFullYear() - 2000 + (date.getUTCMonth() >= 1 ? 1 : 0);
  return { fiscalYear, quarter: Math.floor(fiscalMonth / 3) + 1 };
}
