import { useEffect, useMemo, useRef, useState } from "react";
import { Icon } from "@gitlab-org/orbit-site";
import { iconOf, stepsOf, textOf, Thread } from "./CodeSearch";
import { SearchMap } from "./SearchMap";
import { CODE_MS, CROSS_SEARCHES, type Search, SEARCHES, STEP_MS } from "./searches";

const SCENES = [
  { label: "Code search", search: SEARCHES.find((item) => item.id === "error")!, chat: true },
  { label: "Every branch", search: SEARCHES.find((item) => item.id === "unwrap")!, chat: false },
  { label: "Why the code changed", search: CROSS_SEARCHES.find((item) => item.id === "why")!, chat: false },
];

// On a phone, each example plays on its own as it scrolls into view, with its map above it, instead of the
// clickable demo. A finished thread sits hidden under the live one, so the scene keeps its height while it plays.
// Without `chat`, a scene is only its question, a taller map, and the answer in one line once the map is done.
function Scene({ label, search, chat }: { label: string; search: Search; chat: boolean }) {
  const root = useRef<HTMLElement>(null);
  const [start, setStart] = useState<number | null>(null);
  const [stage, setStage] = useState(0);

  useEffect(() => {
    const element = root.current;
    if (!element) return;
    const seen = new IntersectionObserver(
      ([entry]) => {
        if (!entry?.isIntersecting) return;
        setStart(performance.now());
        setStage(0);
      },
      { threshold: 0.6 },
    );
    seen.observe(element);
    return () => seen.disconnect();
  }, []);

  useEffect(() => {
    if (start === null || stage >= stepsOf(search)) return;
    const timer = window.setTimeout(() => setStage((n) => n + 1), search.steps ? STEP_MS : CODE_MS);
    return () => window.clearTimeout(timer);
  }, [search, start, stage]);

  // A copy for each start, so the map replays the route each time the scene comes back into view.
  const shown = useMemo(() => (start === null ? null : { ...search }), [search, start]);
  const id = `scene-${search.id}`;
  return (
    <article ref={root} id={id} className="next-scene">
      <p className="next-scene-label">
        <Icon name={iconOf(search)} size={14} />
        {label}
      </p>
      {!chat && <p className="next-scene-ask">{textOf(search)}</p>}
      <div className={chat ? "next-scene-viz" : "next-scene-viz next-scene-viz-tall"} aria-hidden="true">
        <SearchMap search={shown} anchor={`#${id}`} region={`#${id} .next-scene-viz`} tall={chat ? 180 : 250} lift={12} />
      </div>
      {chat ? (
        <div className="agent-chat next-chat next-scene-chat">
          <div className="next-scene-thread">
            <div className="next-scene-ghost" aria-hidden="true" inert>
              <Thread search={search} stage={stepsOf(search)} />
            </div>
            {start === null ? (
              <div className="agent-thread">
                <p className="agent-message">{textOf(search)}</p>
              </div>
            ) : (
              <Thread key={start} search={search} stage={stage} />
            )}
          </div>
        </div>
      ) : (
        <p className="next-scene-answer" aria-live="polite" data-done={start !== null && stage >= stepsOf(search) ? "" : undefined}>
          {search.summary}
        </p>
      )}
    </article>
  );
}

// `only` picks the scenes by search, such as the third-party one in its own section.
export function Scenes({ only }: { only: string[] }) {
  return (
    <div className="next-scenes not-prose">
      {SCENES.filter((scene) => only.includes(scene.search.id)).map((scene) => (
        <Scene key={scene.search.id} {...scene} />
      ))}
    </div>
  );
}
