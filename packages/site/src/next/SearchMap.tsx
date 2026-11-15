import { useEffect, useRef } from "react";
import type { Search } from "./searches";

// The hero's backdrop: a dot grid in the style of the kit's RippleField. When a search starts, one soft
// ring spreads from the chat across the grid. The path itself shows in the chat's answer.

const GAP = 18;
const RING_MS = 1800;
const COLORS = { light: { rest: "120 125 140", ring: "110 126 224" }, dark: { rest: "200 205 225", ring: "160 182 255" } };

const still = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;

export function SearchMap({ search, anchor }: { search: Search | null; anchor: string }) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const since = useRef(-1e6);

  useEffect(() => {
    if (search) since.current = performance.now();
  }, [search]);

  useEffect(() => {
    const element = canvas.current;
    const ctx = element?.getContext("2d");
    if (!element || !ctx) return;
    let width = 0;
    let height = 0;
    let frame = 0;
    let visible = true;

    const draw = (now: number) => {
      const dpr = window.devicePixelRatio || 1;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);
      const palette = COLORS[element.closest(".gl-dark") ? "dark" : "light"];
      const own = element.getBoundingClientRect();
      const box = document.querySelector(anchor)?.getBoundingClientRect();
      const ox = box ? box.left - own.left + box.width / 2 : width / 2;
      const oy = box ? box.top - own.top + box.height / 2 : height / 2;
      const age = still() ? 1e6 : now - since.current;
      const ring = (age / RING_MS) * Math.hypot(width, height) * 0.6;
      const fade = Math.max(0, 1 - age / RING_MS);
      for (let y = 0; y < height + GAP; y += GAP)
        for (let x = 0; x < width + GAP; x += GAP) {
          const lit = fade * Math.exp(-((Math.hypot(x - ox, y - oy) - ring) ** 2) / 300) * 0.7;
          ctx.fillStyle = `rgb(${lit > 0.08 ? palette.ring : palette.rest})`;
          ctx.globalAlpha = 0.12 + lit * 0.86;
          ctx.beginPath();
          ctx.arc(x, y, 0.9 + lit * 1.7, 0, Math.PI * 2);
          ctx.fill();
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
  }, [anchor]);

  return <canvas ref={canvas} aria-hidden="true" className="next-map" />;
}
