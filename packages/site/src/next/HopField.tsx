import { useEffect, useRef } from "react";
import { cn } from "@gitlab-org/orbit-site";

export type Hop = { tool: string; label: string; x: number; y: number; at: number; tone?: "risk" | "ok" };

const GAP = 18;
const COLORS = {
  light: { rest: "120 125 140", risk: "214 64 64", ok: "40 160 90", path: "110 126 224" },
  dark: { rest: "200 205 225", risk: "240 110 110", ok: "110 205 140", path: "160 182 255" },
};

export const phaseAt = (loop: number) => (performance.now() % loop) / loop;

const toSegment = (x: number, y: number, ax: number, ay: number, bx: number, by: number) => {
  const dx = bx - ax;
  const dy = by - ay;
  const t = Math.max(0, Math.min(1, ((x - ax) * dx + (y - ay) * dy) / (dx * dx + dy * dy || 1)));
  return { d: Math.hypot(x - ax - t * dx, y - ay - t * dy), t };
};

export function HopField({ hops, anchor, loop = 12000, start, className }: { hops: Hop[]; anchor: string; loop?: number; start?: number; className?: string }) {
  const canvas = useRef<HTMLCanvasElement>(null);

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
      ctx.globalAlpha = Math.min(1, 0.14 + lit * 0.86);
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

    const draw = () => {
      const dpr = window.devicePixelRatio || 1;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);
      const palette = COLORS[element.closest(".gl-dark") ? "dark" : "light"];
      const style = getComputedStyle(element);
      const own = element.getBoundingClientRect();
      const box = document.querySelector(anchor)?.getBoundingClientRect() ?? own;
      const snap = (value: number) => Math.round(value / GAP) * GAP;
      const stops = hops.map((hop) => ({ ...hop, px: snap(box.left - own.left + hop.x * box.width), py: snap(box.top - own.top + hop.y * box.height) }));
      const f = start === undefined ? phaseAt(loop) : Math.min(1, (performance.now() - start) / loop);
      const toneOf = (hop: Hop) => (hop.tone ? palette[hop.tone] : palette.path);

      const legs = stops.slice(1).map((to, index) => {
        const from = stops[index]!;
        const progress = Math.max(0, Math.min(1, (f - from.at) / (to.at - from.at)));
        return { from, to, progress };
      });
      const head = legs.find((leg) => leg.progress > 0 && leg.progress < 1);
      const hx = head ? head.from.px + (head.to.px - head.from.px) * head.progress : -1e4;
      const hy = head ? head.from.py + (head.to.py - head.from.py) * head.progress : -1e4;

      for (let y = 0; y < height + GAP; y += GAP)
        for (let x = 0; x < width + GAP; x += GAP) {
          let lit = Math.exp(-(Math.hypot(x - hx, y - hy) ** 2) / 420);
          let color = palette.path;
          for (const leg of legs) {
            if (leg.progress <= 0) continue;
            const { d, t } = toSegment(x, y, leg.from.px, leg.from.py, leg.to.px, leg.to.py);
            if (d < GAP * 0.6 && t <= leg.progress) lit = Math.max(lit, 0.32);
          }
          for (const stop of stops) {
            const age = f - stop.at;
            if (age < 0 || age > 0.08) continue;
            const ring = Math.exp(-((Math.hypot(x - stop.px, y - stop.py) - age * 1400) ** 2) / 200) * (1 - age / 0.08) * 0.8;
            if (ring > lit) {
              lit = ring;
              color = toneOf(stop);
            }
          }
          dot(x, y, lit, lit > 0.1 ? color : palette.rest);
        }

      ctx.textAlign = "center";
      ctx.font = `600 12px ${style.fontFamily}`;
      for (const stop of stops) {
        if (f < stop.at) continue;
        dot(stop.px, stop.py, 1, toneOf(stop), 1.5);
        ctx.fillStyle = style.color;
        ctx.globalAlpha = 0.9;
        ctx.fillText(stop.label, stop.px, stop.py + 22);
      }
    };

    const tick = () => {
      draw();
      if (visible) frame = requestAnimationFrame(tick);
    };
    const resize = () => {
      const rect = element.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      width = rect.width;
      height = rect.height;
      element.width = Math.round(width * dpr);
      element.height = Math.round(height * dpr);
      draw();
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
  }, [hops, anchor, loop, start]);

  return <canvas ref={canvas} aria-hidden="true" className={cn("ripple-field", className)} />;
}
