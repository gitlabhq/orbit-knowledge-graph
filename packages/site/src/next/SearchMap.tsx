import { useEffect, useRef } from "react";
import { CODE_MS, type Search, STEP_MS } from "./searches";

const GAP = 18;
const RING_MS = 1800;
const BURST_MS = 900;
const MAP_TALL = 380;
const MAP_LIFT = 20;
const COLORS = {
  light: { rest: "120 125 140", path: "110 126 224", risk: "214 64 64", ok: "40 160 90" },
  dark: { rest: "200 205 225", path: "160 182 255", risk: "240 110 110", ok: "110 205 140" },
};

const LOGO_FILL_MS = 900;

const LOGO: [number, number][] = [
  [0.5, 0.99],
  [0.1, 0.7],
  [0.02, 0.4],
  [0.19, 0.01],
  [0.31, 0.32],
  [0.69, 0.32],
  [0.81, 0.01],
  [0.98, 0.4],
  [0.9, 0.7],
];
const LOGO_STOPS = [2, 3, 6, 7];
const ORANGE = "252 109 38";

const inside = (x: number, y: number, points: [number, number][]) => {
  let hit = false;
  for (let i = 0, j = points.length - 1; i < points.length; j = i++) {
    const [xi, yi] = points[i]!;
    const [xj, yj] = points[j]!;
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) hit = !hit;
  }
  return hit;
};

const seeded = (text: string) => {
  let state = [...text].reduce((total, char) => (total * 31 + char.charCodeAt(0)) >>> 0, 2166136261);
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
};

const logoBox = (left: number, wide: number, top: number, tall: number) => {
  const size = Math.min(wide * 0.7, tall);
  return { x: left + (wide - size) / 2, y: top + (tall - size * 0.9) / 2, w: size, h: size * 0.9 };
};

type Stop = { name: string; tone: "path" | "risk" | "ok"; from: number; at: number; px: number; py: number; ox: number; oy: number };


const toSegment = (x: number, y: number, ax: number, ay: number, bx: number, by: number) => {
  const dx = bx - ax;
  const dy = by - ay;
  const t = Math.max(0, Math.min(1, ((x - ax) * dx + (y - ay) * dy) / (dx * dx + dy * dy || 1)));
  return { d: Math.hypot(x - ax - t * dx, y - ay - t * dy), t };
};

// `tall` and `lift` set the height of the map inside `region` and its gap from the top, so a small region,
// such as a scroll scene on a phone, fits the whole route.
export function SearchMap({ search, anchor, region, tall = MAP_TALL, lift = MAP_LIFT }: { search: Search | null; anchor: string; region: string; tall?: number; lift?: number }) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const state = useRef<{ search: Search | null; since: number }>({ search, since: 0 });

  useEffect(() => {
    state.current = { search, since: performance.now() };
  }, [search]);

  useEffect(() => {
    const element = canvas.current;
    const ctx = element?.getContext("2d");
    if (!element || !ctx) return;
    let width = 0;
    let height = 0;
    let frame = 0;
    let visible = true;

    const dot = (x: number, y: number, lit: number, color: string, scale = 1) => {
      const size = (0.9 + lit * 1.7) * scale;
      ctx.fillStyle = `rgb(${color})`;
      ctx.globalAlpha = Math.min(1, 0.12 + lit * 0.86);
      ctx.beginPath();
      ctx.arc(x, y, size, 0, Math.PI * 2);
      ctx.fill();
      if (lit > 0.4) {
        ctx.globalAlpha = 0.14 * lit;
        ctx.beginPath();
        ctx.arc(x, y, size * 3.4, 0, Math.PI * 2);
        ctx.fill();
      }
    };

    const stopsOf = (search: Search, area: DOMRect, own: DOMRect): Stop[] => {
      const snap = (value: number) => Math.round(value / GAP) * GAP;
      const left = area.left - own.left;
      const wide = Math.max(area.width, own.width - left - 60);
      const top = area.top - own.top + lift;
      const random = seeded(search.id);
      if (search.steps) {
        const steps = search.steps;
        if (search.logo) {
          const box = logoBox(left, wide, top, tall);
          const corners = [...LOGO.keys(), 0];
          const leg = (steps.length * STEP_MS) / (corners.length - 1);
          const route: Stop[] = [];
          corners.forEach((corner, index) => {
            const named = LOGO_STOPS.indexOf(corner);
            const step = named >= 0 && index < corners.length - 1 ? steps[named] : undefined;
            const px = snap(box.x + LOGO[corner]![0] * box.w);
            const py = snap(box.y + LOGO[corner]![1] * box.h);
            const prev = route.at(-1);
            route.push({ name: step?.name ?? "", tone: step?.tone ?? "path", from: Math.max(0, (index - 1) * leg), at: Math.max(1, index * leg), px, py, ox: prev?.px ?? px, oy: prev?.py ?? py });
          });
          return route;
        }
        const stops = steps.map((step, index) => {
          const point = search.points?.[index];
          const px = snap(left + wide * (point ? point[0] : 0.1 + (0.8 * index) / Math.max(1, steps.length - 1)));
          const py = snap(top + tall * (point ? point[1] : index % 2 ? 0.1 + 0.3 * random() : 0.55 + 0.3 * random()));
          return { name: step.name, tone: step.tone ?? ("path" as const), from: index * STEP_MS, at: (index + 1) * STEP_MS, px, py, ox: px, oy: py };
        });
        stops.forEach((stop, index) => index > 0 && Object.assign(stop, { ox: stops[index - 1]!.px, oy: stops[index - 1]!.py }));
        return stops;
      }
      const names = [...search.files.map((hit) => `${hit.path.split("/").at(-1)}:${hit.lines.find((line) => line.match)!.n}`), ...(search.more ?? [])];
      const cells = Array.from({ length: 15 }, (_, cell) => ({ cell, order: random() })).sort((a, b) => a.order - b.order);
      return names.slice(0, cells.length).map((name, index) => {
        const { cell } = cells[index]!;
        return {
          name,
          tone: "path" as const,
          from: 0,
          at: CODE_MS,
          px: snap(left + 60 + ((wide - 60) * ((cell % 3) + 0.4 + random() * 0.2)) / 3),
          py: snap(top + (tall * (Math.floor(cell / 3) + 0.1 + (cell % 3 === 1 ? 0.5 : 0) + random() * 0.25)) / 5.5),
          ox: 0,
          oy: 0,
        };
      });
    };

    const draw = (now: number) => {
      const dpr = window.devicePixelRatio || 1;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);
      const palette = COLORS[element.closest(".gl-dark") ? "dark" : "light"];
      const style = getComputedStyle(element);
      const own = element.getBoundingClientRect();
      const chat = document.querySelector(anchor)?.getBoundingClientRect();
      const area = document.querySelector(region)?.getBoundingClientRect();
      const { search, since } = state.current;
      const age = now - since;
      const stops = search && chat && area && area.width > 120 ? stopsOf(search, area, own) : [];
      const legs = search?.steps ? stops.map((stop) => ({ stop, progress: Math.max(0, Math.min(1, (age - stop.from) / (stop.at - stop.from))) })) : [];
      const heads = legs.filter((leg) => leg.progress > 0 && leg.progress < 1).map(({ stop, progress }) => ({ x: stop.ox + (stop.px - stop.ox) * progress, y: stop.oy + (stop.py - stop.oy) * progress }));

      const code = search !== null && !search.steps;
      const cx = area ? (area.left - own.left + width) / 2 : width / 2;
      const cy = area ? area.top - own.top + lift + tall / 2 : height / 2;
      const reach = Math.hypot(Math.max(cx, width - cx), Math.max(cy, height - cy));
      const ring = code ? (age / RING_MS) * reach : -1e4;
      const fade = code ? Math.max(0, 1 - age / (RING_MS * 1.1)) : 0;
      const fillAge = search?.logo && stops.length ? age - stops.at(-1)!.at : -1;
      const fill = fillAge > 0 ? Math.min(1, fillAge / LOGO_FILL_MS) : 0;
      const box = fill > 0 && chat && area ? logoBox(area.left - own.left, Math.max(area.width, own.width - (area.left - own.left) - 60), area.top - own.top + lift, tall) : null;

      for (let y = 0; y < height + GAP; y += GAP)
        for (let x = 0; x < width + GAP; x += GAP) {
          let lit = fade * Math.exp(-((Math.hypot(x - cx, y - cy) - ring) ** 2) / 300) * 0.7;
          for (const head of heads) lit = Math.max(lit, Math.exp(-(Math.hypot(x - head.x, y - head.y) ** 2) / 420));
          let color = palette.path;
          for (const { stop, progress } of legs) {
            if (progress <= 0) continue;
            const { d, t } = toSegment(x, y, stop.ox, stop.oy, stop.px, stop.py);
            if (d < GAP * 0.6 && t <= progress) lit = Math.max(lit, 0.32);
            const since = age - stop.at;
            if (since < 0 || since > BURST_MS) continue;
            const burst = Math.exp(-((Math.hypot(x - stop.px, y - stop.py) - since * 0.12) ** 2) / 200) * (1 - since / BURST_MS) * 0.8;
            if (burst > lit) {
              lit = burst;
              color = palette[stop.tone];
            }
          }
          if (box && inside((x - box.x) / box.w, (y - box.y) / box.h, LOGO)) {
            dot(x, y, Math.max(lit, 0.55 * fill), ORANGE);
            continue;
          }
          dot(x, y, lit, lit > 0.1 ? color : palette.rest);
        }

      ctx.textAlign = "center";
      ctx.font = `600 12px ${style.fontFamily}`;
      const shown = code ? stops.filter((stop) => Math.hypot(stop.px - cx, stop.py - cy) <= ring) : legs.filter((leg) => leg.progress >= 1).map((leg) => leg.stop);
      for (const stop of shown) {
        dot(stop.px, stop.py, 1, palette[stop.tone], stop.name ? 1.5 : 1.1);
        if (!stop.name) continue;
        ctx.fillStyle = code ? `rgb(${palette.rest})` : style.color;
        ctx.globalAlpha = code ? 0.7 : 0.9;
        const half = ctx.measureText(stop.name).width / 2 + 4;
        ctx.fillText(stop.name, Math.min(Math.max(stop.px, half), width - half), stop.py + 22);
      }
    };

    const tick = (now: number) => {
      draw(now);
      if (visible) frame = requestAnimationFrame(tick);
    };
    const resize = () => {
      const rect = element.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      width = rect.width;
      height = rect.height;
      element.width = Math.round(width * dpr);
      element.height = Math.round(height * dpr);
      draw(performance.now());
    };
    const sized = new ResizeObserver(resize);
    sized.observe(element);
    const seen = new IntersectionObserver(([entry]) => {
      visible = entry?.isIntersecting ?? true;
      cancelAnimationFrame(frame);
      if (visible) frame = requestAnimationFrame(tick);
    });
    seen.observe(element);
    resize();
    return () => {
      cancelAnimationFrame(frame);
      sized.disconnect();
      seen.disconnect();
    };
  }, [anchor, region, tall, lift]);

  return <canvas ref={canvas} aria-hidden="true" className="next-map" />;
}
