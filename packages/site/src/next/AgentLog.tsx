import { useEffect, useRef, useState } from "react";
import { cn, Icon, type RippleLogLine } from "@gitlab-org/orbit-site";
import { phaseAt } from "./HopField";

export function AgentLog({ lines, loop, title = "Your agent" }: { lines: RippleLogLine[]; loop: number; title?: string }) {
  const [phase, setPhase] = useState(0);
  const box = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let frame = 0;
    let visible = true;
    const tick = () => {
      setPhase(phaseAt(loop));
      if (visible) frame = window.setTimeout(tick, 100);
    };
    const seen = new IntersectionObserver(([entry]) => {
      visible = entry?.isIntersecting ?? true;
      window.clearTimeout(frame);
      if (visible) tick();
    });
    if (box.current) seen.observe(box.current);
    return () => {
      window.clearTimeout(frame);
      seen.disconnect();
    };
  }, [loop]);

  return (
    <div ref={box} className="trace-log terminal ripple-log">
      <div className="terminal-head">
        <span className="terminal-lights" aria-hidden="true">
          <span />
          <span />
          <span />
        </span>
        <span className="terminal-title">{title}</span>
      </div>
      <ul className="trace-log-lines">
        {lines.map((line, index) => (
          <li
            key={`${index}-${phase >= line.at}`}
            aria-hidden={phase < line.at || undefined}
            className={cn("trace-log-line", phase < line.at && "trace-log-pending", line.tool && "trace-log-call", line.tone === "ok" && "trace-log-ok")}
          >
            {line.tool ? (
              <>
                <Icon name="orbit" size={12} className="agent-call-mark" />
                <span className="agent-call-tool">{line.text}</span>
              </>
            ) : (
              line.text
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}
