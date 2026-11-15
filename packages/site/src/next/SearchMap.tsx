import { useEffect, useRef } from "react";
import type { Search } from "./searches";

// The hero's backdrop, in the style of the kit's RippleField: a dot grid with public gitlab-org projects
// on it. When a search starts, a ring spreads from the search bar across the map. Projects the ring
// reaches light up, and those with matches stay lit, labeled with the matching file and line. The rest
// fade back. With no search, the map rests.

const GAP = 18;
const RING_MS = 2400;
const COLORS = {
  light: { rest: "120 125 140", ring: "110 126 224", ok: "40 160 90" },
  dark: { rest: "200 205 225", ring: "160 182 255", ok: "110 205 140" },
};

// Where projects sit on the map, as shares of its width and height, around the copy in the middle. The
// first slots sit beside the search bar, so the projects with matches show next to the results.
const SLOTS = [
  { x: 0.13, y: 0.42 },
  { x: 0.87, y: 0.44 },
  { x: 0.07, y: 0.62 },
  { x: 0.93, y: 0.64 },
  { x: 0.1, y: 0.18 },
  { x: 0.9, y: 0.2 },
  { x: 0.13, y: 0.84 },
  { x: 0.87, y: 0.86 },
  { x: 0.28, y: 0.07 },
  { x: 0.72, y: 0.08 },
];

// Real public gitlab-org projects that fill the slots no match needs.
const OTHERS = ["gitlab-org/cli", "gitlab-org/gitaly", "gitlab-org/gitlab-runner", "gitlab-org/gitlab-pages", "gitlab-org/gitlab-shell", "gitlab-org/gitlab-ui", "gitlab-org/release-tools"];
const DEFAULTS = ["gitlab-org/gitlab", "gitlab-org/orbit/knowledge-graph", "gitlab-org/orbit/documentation/site-kit"];

const still = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const short = (name: string) => name.split("/").at(-1)!;

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

    const draw = (now: number) => {
      const dpr = window.devicePixelRatio || 1;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);
      const palette = COLORS[element.closest(".gl-dark") ? "dark" : "light"];
      const style = getComputedStyle(element);
      const own = element.getBoundingClientRect();
      const box = document.querySelector(anchor)?.getBoundingClientRect();
      const ox = box ? box.left - own.left + box.width / 2 : width / 2;
      const oy = box ? box.top - own.top + box.height / 2 : height * 0.6;
      const snap = (value: number) => Math.round(value / GAP) * GAP;
      const { search, since } = state.current;
      const age = search ? (still() ? RING_MS * 2 : now - since) : -1;
      const reach = Math.hypot(Math.max(ox, width - ox), Math.max(oy, height - oy));
      const ring = age < 0 ? -1e4 : (age / RING_MS) * reach;

      // Projects show only while a search is on: a code search puts the projects with matches first, and a
      // third-party search shows the GitLab work its records link to.
      const links = search?.records ? [...new Set(search.records.flatMap((record) => record.links))] : [];
      const matched = search ? (search.records ? links : [...new Set(search.files.map((hit) => hit.project))]) : [];
      const names = search ? [...matched, ...DEFAULTS.filter((name) => !matched.includes(name)), ...OTHERS].slice(0, SLOTS.length) : [];
      const nodes = names.map((name, index) => ({ name, px: snap(SLOTS[index]!.x * width), py: snap(SLOTS[index]!.y * height) }));

      for (let y = 0; y < height + GAP; y += GAP)
        for (let x = 0; x < width + GAP; x += GAP) {
          const d = Math.hypot(x - ox, y - oy);
          const lit = age < 0 || age > RING_MS * 1.3 ? 0 : Math.exp(-((d - ring) ** 2) / 260) * 0.75 * (1 - Math.min(1, age / (RING_MS * 1.3)));
          dot(x, y, lit, lit > 0.08 ? palette.ring : palette.rest);
        }

      ctx.textAlign = "center";
      for (const node of nodes) {
        const reached = Math.hypot(node.px - ox, node.py - oy) < ring;
        const hit = search?.files.find((file) => file.project === node.name) ?? (links.includes(node.name) ? { link: node.name } : undefined);
        const lit = reached ? (hit ? 1 : Math.max(0.2, 1 - (age - (Math.hypot(node.px - ox, node.py - oy) / reach) * RING_MS) / 900)) : 0.2;
        dot(node.px, node.py, lit, reached && hit ? palette.ok : reached ? palette.ring : palette.rest, 1.4);
        // On narrow screens the labels would sit on the copy, so only projects with matches show one.
        if (width < 900 && !(reached && hit)) continue;
        ctx.fillStyle = style.color;
        ctx.globalAlpha = reached && hit ? 0.9 : 0.35;
        ctx.font = `600 12px ${style.fontFamily}`;
        ctx.fillText(short(node.name), node.px, node.py - 14);
        if (reached && hit && "lines" in hit) {
          const line = hit.lines.find((entry) => entry.match)!;
          ctx.globalAlpha = 0.6;
          ctx.font = `500 11px ${style.getPropertyValue("--font-mono") || "monospace"}`;
          ctx.fillText(`${hit.path.split("/").at(-1)}:${line.n}`, node.px, node.py + 22);
        }
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
