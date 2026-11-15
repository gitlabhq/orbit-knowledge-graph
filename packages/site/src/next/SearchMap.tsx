import { useEffect, useRef } from "react";
import { CODE_MS, type Search, STEP_MS } from "./searches";

// The hero's backdrop: a dot grid in the style of the kit's RippleField. Results land on the right of the
// chat, out to the window edge, from the `region` column on. A code search sends one ring out from the
// middle of that side, and marks each matching file as a finding when the ring reaches it. A question across tools runs from tool to tool down the region, as in
// the third-party section below. Nothing shows before a link reaches it, and nothing shows when the region
// column is hidden, as on phones.

const GAP = 18;
const RING_MS = 1800;
const LOGO_FILL_MS = 900;
const ORANGE = "252 109 38";

// The GitLab tanuki as an outline, from 0 to 1: chin, left cheek, left ear, the dip between the ears,
// right ear, right cheek. The logo path stops at the cheeks and the ears.
const LOGO: [number, number][] = [
  [0.5, 0.98],
  [0.03, 0.62],
  [0.17, 0.06],
  [0.31, 0.4],
  [0.69, 0.4],
  [0.83, 0.06],
  [0.97, 0.62],
];
const LOGO_STOPS = [1, 2, 5, 6];

const inside = (x: number, y: number, points: [number, number][]) => {
  let hit = false;
  for (let i = 0, j = points.length - 1; i < points.length; j = i++) {
    const [xi, yi] = points[i]!;
    const [xj, yj] = points[j]!;
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) hit = !hit;
  }
  return hit;
};
const COLORS = {
  light: { rest: "120 125 140", path: "110 126 224", risk: "214 64 64", ok: "40 160 90" },
  dark: { rest: "200 205 225", path: "160 182 255", risk: "240 110 110", ok: "110 205 140" },
};

type Stop = { name: string; tone: "path" | "risk" | "ok"; from: number; at: number; px: number; py: number; ox: number; oy: number };

const still = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const toSegment = (x: number, y: number, ax: number, ay: number, bx: number, by: number) => {
  const dx = bx - ax;
  const dy = by - ay;
  const t = Math.max(0, Math.min(1, ((x - ax) * dx + (y - ay) * dy) / (dx * dx + dy * dy || 1)));
  return { d: Math.hypot(x - ax - t * dx, y - ay - t * dy), t };
};

export function SearchMap({ search, anchor, region }: { search: Search | null; anchor: string; region: string }) {
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

    // Where each result lands, and when its link leaves and arrives. Positions depend only on the chat's
    // place and the window, so nothing moves while a search runs.
    const stopsOf = (search: Search, chat: DOMRect, area: DOMRect, own: DOMRect): Stop[] => {
      const snap = (value: number) => Math.round(value / GAP) * GAP;
      const left = area.left - own.left;
      const wide = Math.max(area.width, own.width - left - 60);
      const top = chat.top - own.top + 30;
      const tall = chat.height - 60;
      const ox = chat.right - own.left;
      const oy = top + tall / 2;
      if (search.steps) {
        const steps = search.steps;
        // The logo path visits the tanuki's cheeks and ears; any other path zigzags down the side.
        const size = Math.min(wide * 0.8, tall);
        const at = (index: number) =>
          search.logo
            ? { px: snap(left + (wide - size) / 2 + LOGO[LOGO_STOPS[index % LOGO_STOPS.length]!]![0] * size), py: snap(top + (tall - size * 0.9) / 2 + LOGO[LOGO_STOPS[index % LOGO_STOPS.length]!]![1] * size * 0.9) }
            : { px: snap(left + wide * (index % 2 ? 0.7 : 0.25)), py: snap(top + (steps.length === 1 ? tall / 3 : (index * tall) / (steps.length - 1))) };
        return steps.map((step, index) => ({ name: step.name, tone: step.tone ?? "path", from: index * STEP_MS, at: (index + 1) * STEP_MS, ...at(index), ox, oy }));
      }
      // Code findings sit on a loose grid of three columns, offset by column so labels never touch.
      const names = [...search.files.map((hit) => `${hit.path.split("/").at(-1)}:${hit.lines.find((line) => line.match)!.n}`), ...(search.more ?? [])];
      return names.map((name, index) => ({
        name,
        tone: "path" as const,
        from: 0,
        at: CODE_MS,
        px: snap(left + wide * (0.14 + 0.36 * (index % 3))),
        py: snap(top + (tall * (Math.floor(index / 3) + 0.3 + (index % 3) * 0.3)) / Math.ceil(names.length / 3)),
        ox,
        oy,
      }));
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
      const age = still() ? 1e6 : now - since;
      const stops = search && chat && area && area.width > 120 ? stopsOf(search, chat, area, own) : [];
      // A cross-tool path chains its stops with links. A code search uses only the ring.
      if (search?.steps) stops.forEach((stop, index) => index > 0 && Object.assign(stop, { ox: stops[index - 1]!.px, oy: stops[index - 1]!.py }));
      const legs = search?.steps ? stops.map((stop) => ({ stop, progress: Math.max(0, Math.min(1, (age - stop.from) / (stop.at - stop.from))) })) : [];
      const heads = legs.filter((leg) => leg.progress > 0 && leg.progress < 1).map(({ stop, progress }) => ({ x: stop.ox + (stop.px - stop.ox) * progress, y: stop.oy + (stop.py - stop.oy) * progress }));

      const code = search !== null && !search.steps;
      // The logo fills once the path reaches its last stop, scaled to the same box as its stops.
      const logoAge = search?.logo && stops.length ? age - stops.at(-1)!.at : -1;
      const logoFill = logoAge > 0 ? Math.min(1, logoAge / LOGO_FILL_MS) : 0;
      const logoBox = search?.logo && chat && area ? (() => {
        const left = area.left - own.left;
        const wide = Math.max(area.width, own.width - left - 60);
        const top = chat.top - own.top + 30;
        const tall = chat.height - 60;
        const size = Math.min(wide * 0.8, tall);
        return { x: left + (wide - size) / 2, y: top + (tall - size * 0.9) / 2, w: size, h: size * 0.9 };
      })() : null;
      const cx = area ? (area.left - own.left + width) / 2 : width / 2;
      const cy = chat ? chat.top - own.top + chat.height / 2 : height / 2;
      const reach = Math.hypot(Math.max(cx, width - cx), Math.max(cy, height - cy));
      const ring = code ? (age / RING_MS) * reach : -1e4;
      const fade = code ? Math.max(0, 1 - age / (RING_MS * 1.1)) : 0;
      for (let y = 0; y < height + GAP; y += GAP)
        for (let x = 0; x < width + GAP; x += GAP) {
          let lit = fade * Math.exp(-((Math.hypot(x - cx, y - cy) - ring) ** 2) / 300) * 0.7;
          for (const head of heads) lit = Math.max(lit, Math.exp(-(Math.hypot(x - head.x, y - head.y) ** 2) / 420));
          for (const { stop, progress } of legs) {
            if (progress <= 0) continue;
            const { d, t } = toSegment(x, y, stop.ox, stop.oy, stop.px, stop.py);
            if (d < GAP * 0.6 && t <= progress) lit = Math.max(lit, 0.32);
          }
          if (logoBox && logoFill > 0 && inside((x - logoBox.x) / logoBox.w, (y - logoBox.y) / logoBox.h, LOGO)) {
            dot(x, y, 0.55 * logoFill, ORANGE);
            continue;
          }
          dot(x, y, lit, lit > 0.1 ? palette.path : palette.rest);
        }

      ctx.textAlign = "center";
      ctx.font = `600 12px ${style.fontFamily}`;
      const shown = code ? stops.filter((stop) => Math.hypot(stop.px - cx, stop.py - cy) <= ring) : legs.filter((leg) => leg.progress >= 1).map((leg) => leg.stop);
      for (const stop of shown) {
        dot(stop.px, stop.py, 1, palette[stop.tone], 1.5);
        ctx.fillStyle = `rgb(${palette.rest})`;
        ctx.globalAlpha = 0.7;
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
  }, [anchor, region]);

  return <canvas ref={canvas} aria-hidden="true" className="next-map" />;
}
