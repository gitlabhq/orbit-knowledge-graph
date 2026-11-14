import { Icon } from "@gitlab-org/orbit-site";
import { Link } from "@tanstack/react-router";
import { quarterLabel, type Quarter } from "./calendar";
import { itemRange } from "./ItemCard";
import { items, type RoadmapItem } from "./model";
import "./upcoming.css";

const DAY_MS = 86_400_000;

// The first day of a fiscal quarter, and the first day after it. The fiscal year starts on Feb 1.
const quarterStart = (q: Quarter) => Date.UTC(2000 + q.fiscalYear - 1, 1 + (q.quarter - 1) * 3, 1);
const quarterEnd = (q: Quarter) => Date.UTC(2000 + q.fiscalYear - 1, 1 + q.quarter * 3, 1);

// The work in progress on one shared track, like the Upcoming features tile in the Orbit Portal: a
// bar per item across its quarters, filled up to today. The public roadmap plans in quarters, so the
// bars end on quarter boundaries and carry no launch milestones.
export function Upcoming({ today = Date.now() }: { today?: number }) {
  const lines = items.filter((item) => item.status === "in-progress");
  if (lines.length === 0) return null;
  const min = today - 30 * DAY_MS;
  const max = Math.max(today, ...lines.map((item) => quarterEnd(item.end))) + 7 * DAY_MS;
  const pct = (time: number) => Math.min(100, Math.max(0, ((time - min) / (max - min)) * 100));
  const months: { label: string; pct: number }[] = [];
  const cursor = new Date(min);
  cursor.setUTCDate(1);
  cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  while (cursor.getTime() <= max) {
    months.push({ label: cursor.toLocaleDateString("en-US", { month: "short", timeZone: "UTC" }), pct: pct(cursor.getTime()) });
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  }
  return (
    <figure className="upcoming not-prose card tile-quiet relative -mt-3 mb-0 flex flex-col gap-4 px-5 pt-4 pb-3">
      <h2 className="m-0 text-base font-semibold text-default">
        <Link to="/roadmap" className="card-title text-default no-underline hover:underline">
          Upcoming features
        </Link>
      </h2>
      <div className="upcoming-grid">
        {lines.map((item) => (
          <Line key={item.slug} item={item} pct={pct} today={today} />
        ))}
        <span />
        <div className="upcoming-lane upcoming-month-lane" aria-hidden="true">
          {months.map((month) => (
            <span key={`${month.label}-${month.pct}`} className="upcoming-month" style={{ left: `${month.pct}%` }}>
              {month.label}
            </span>
          ))}
        </div>
      </div>
    </figure>
  );
}

function Line({ item, pct, today }: { item: RoadmapItem; pct: (time: number) => number; today: number }) {
  const left = pct(quarterStart(item.start));
  const width = Math.max(pct(quarterEnd(item.end)) - left, 0.6);
  const fill = Math.min(100, Math.max(0, ((pct(today) - left) / width) * 100));
  const range = itemRange(item);
  return (
    <>
      <Link to="/roadmap/$item" params={{ item: item.slug }} className="upcoming-name" title={`${item.title}, ${range.label} (${range.months})`}>
        <Icon name={item.theme.icon} size={14} className="tile-icon shrink-0" />
        <span className="upcoming-name-text">{item.title}</span>
      </Link>
      <div className="upcoming-lane" aria-hidden="true">
        <div className="upcoming-segment" style={{ left: `${left}%`, width: `${width}%` }}>
          <div className="upcoming-track" />
          <div className="upcoming-fill" style={{ width: `${fill}%` }} />
        </div>
        <span className="upcoming-end" style={{ left: `${left + width}%` }}>
          {quarterLabel(item.end)}
        </span>
      </div>
    </>
  );
}
