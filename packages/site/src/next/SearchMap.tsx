import { useEffect, useRef } from "react";
import { type Search, STEP_MS } from "./searches";

// The hero's backdrop: a dot grid in the style of the kit's RippleField. While a search runs, links run out
// from the search bar, as in the third-party section below. A code search fans out to the projects with
// matches during its scan. A cross-tool question hops from tool to tool, one step at a time. Each stop
// shows only once a link reaches it, named below its dot. With no search, only the grid shows.

const GAP = 18;
const COLORS = {
  light: { rest: "120 125 140", path: "110 126 224", risk: "214 64 64", ok: "40 160 90" },
  dark: { rest: "200 205 225", path: "160 182 255", risk: "240 110 110", ok: "110 205 140" },
};

// Where stops sit, as shares of the map's width and height: down the sides, so the links cross behind the
// search panel, in the order a path visits them.
const SLOTS = [
  { x: 0.12, y: 0.3 },
  { x: 0.88, y: 0.26 },
  { x: 0.1, y: 0.56 },
  { x: 0.9, y: 0.52 },
  { x: 0.14, y: 0.8 },
  { x: 0.86, y: 0.78 },
];

type Stop = { name: string; tone: "path" | "risk" | "ok"; from: number; at: number; px: number; py: number; ox: number; oy: number };

const still = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const toSegment = (x: number, y: number, ax: number, ay: number, bx: number, by: number) => {
  const dx = bx - ax;
  const dy = by - ay;
  const t = Math.max(0, Math.min(1, ((x - ax) * dx + (y - ay) * dy) / (dx * dx + dy * dy || 1)));
  return { d: Math.hypot(x - ax - t * dx, y - ay - t * dy), t };
};

export function SearchMap({ search, anchor }: { search: Search | null; anchor: string }) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const state = useRef<{ search: Search | null; since: number }>({ search, since: 0 });
  const redraw = useRef<() => void>(() => {});

  useEffect(() => {
    state.current = { search, since: performance.now() };
    if (still()) redraw.current();
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

    // The stops of the current search, and when the link to each starts and arrives, in milliseconds.
    const stopsOf = (search: Search, ox: number, oy: number): Stop[] => {
      const snap = (value: number) => Math.round(value / GAP) * GAP;
      const place = (index: number) => ({ px: snap(SLOTS[index % SLOTS.length]!.x * width), py: snap(SLOTS[index % SLOTS.length]!.y * height) });
      if (search.steps) {
        const stops: Stop[] = [];
        search.steps.slice(0, SLOTS.length).forEach((step, index) => {
          const prev = stops[index - 1];
          stops.push({ name: step.name, tone: step.tone ?? "path", from: index * STEP_MS, at: (index + 1) * STEP_MS, ...place(index), ox: prev?.px ?? ox, oy: prev?.py ?? oy });
        });
        return stops;
      }
      const projects = [...new Set(search.files.map((hit) => hit.project))];
      return projects.slice(0, SLOTS.length).map((project, index) => ({ name: project.split("/").at(-1)!, tone: "ok", from: 2 * STEP_MS + index * 120, at: 4 * STEP_MS, ...place(index), ox, oy }));
    };

    const draw = (now: number) => {
      const dpr = window.devicePixelRatio || 1;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);
      const palette = COLORS[element.closest(".gl-dark") ? "dark" : "light"];
      const style = getComputedStyle(element);
      const own = element.getBoundingClientRect();
      const box = document.querySelector(anchor)?.getBoundingClientRect();
      const ox = box ? box.left - own.left + box.width / 2 : width / 2;
      const oy = box ? box.top - own.top + box.height / 2 : height * 0.4;
      const { search, since } = state.current;
      const age = still() ? 1e6 : now - since;
      const stops = search ? stopsOf(search, ox, oy) : [];
      const legs = stops.map((stop) => ({ stop, progress: Math.max(0, Math.min(1, (age - stop.from) / (stop.at - stop.from))) }));
      const heads = legs.filter((leg) => leg.progress > 0 && leg.progress < 1).map(({ stop, progress }) => ({ x: stop.ox + (stop.px - stop.ox) * progress, y: stop.oy + (stop.py - stop.oy) * progress }));

      for (let y = 0; y < height + GAP; y += GAP)
        for (let x = 0; x < width + GAP; x += GAP) {
          let lit = 0;
          for (const head of heads) lit = Math.max(lit, Math.exp(-(Math.hypot(x - head.x, y - head.y) ** 2) / 420));
          for (const { stop, progress } of legs) {
            if (progress <= 0) continue;
            const { d, t } = toSegment(x, y, stop.ox, stop.oy, stop.px, stop.py);
            if (d < GAP * 0.6 && t <= progress) lit = Math.max(lit, 0.32);
          }
          dot(x, y, lit, lit > 0.1 ? palette.path : palette.rest);
        }

      ctx.textAlign = "center";
      ctx.font = `600 12px ${style.fontFamily}`;
      for (const { stop, progress } of legs) {
        if (progress < 1) continue;
        dot(stop.px, stop.py, 1, palette[stop.tone], 1.5);
        ctx.fillStyle = style.color;
        ctx.globalAlpha = 0.9;
        ctx.fillText(stop.name, stop.px, stop.py + 22);
      }
    };

    const tick = (now: number) => {
      draw(now);
      if (visible && !still()) frame = requestAnimationFrame(tick);
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
    redraw.current = () => draw(performance.now());
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
  }, [anchor]);

  return <canvas ref={canvas} aria-hidden="true" className="next-map" />;
}
