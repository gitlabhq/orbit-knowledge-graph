import { type FormEvent, useEffect, useState } from "react";
import { cn, Icon } from "@gitlab-org/orbit-site";
import { CODE_MS, CROSS_SEARCHES, type FileHit, type Line, type Mode, MODES, type Search, SEARCHES, STEP_MS } from "./searches";

const still = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const TYPE_MS = 18;
const stepsOf = (search: Search) => (search.steps ? search.steps.length : 1);
const iconOf = (search: Search) => (search.kind === "cross" ? search.steps![0]!.icon : search.branches === "all" ? "branch" : search.mode === "structural" ? "code" : "search");

// Code searches from the simplest mode to the richest, and questions that use third-party connectors.
const GROUPS = [
  { label: "Code search", items: ["security-context", "branch", "flag", "unwrap"].map((id) => SEARCHES.find((item) => item.id === id)!) },
  { label: "Third-party connectors", items: CROSS_SEARCHES.filter((item) => item.id !== "epic") },
];
const ALL = [...SEARCHES, ...CROSS_SEARCHES];

function Code({ line }: { line: Line }) {
  const at = line.match ? line.text.indexOf(line.match) : -1;
  if (at < 0) return <>{line.text}</>;
  return (
    <>
      {line.text.slice(0, at)}
      <mark>{line.match}</mark>
      {line.text.slice(at + line.match!.length)}
    </>
  );
}

function File({ hit }: { hit: FileHit }) {
  return (
    <div className="next-file">
      <div className="next-file-head">
        <span className="next-file-path">
          {hit.project.split("/").at(-1)} / {hit.path}
        </span>
        <span className="next-branch">
          <Icon name="branch" size={12} />
          {hit.branch}
        </span>
      </div>
      <pre className="next-file-code">
        {hit.lines.map((line) => (
          <span key={line.n} className={cn("next-file-line", line.match && "next-file-line-hit")}>
            <span className="next-file-n">{line.n}</span>
            <Code line={line} />
          </span>
        ))}
      </pre>
    </div>
  );
}

// The thread for one example: the question, one Orbit tool line that grows as the search runs, and the
// answer. The path and the findings show on the map behind the chat.
// The tool lines for a question across tools: tools read in a row share one "Explore" line, as the Orbit
// plugin shows them, and each code search gets its own line.
const segmentsOf = (search: Search) =>
  search.steps!.reduce<{ code: boolean; tools: string[]; first: number; last: number }[]>((list, step, index) => {
    const code = step.tool === "Code search";
    const last = list.at(-1);
    if (last && !last.code && !code) {
      if (!last.tools.includes(step.tool)) last.tools.push(step.tool);
      last.last = index;
    } else list.push({ code, tools: [step.tool], first: index, last: index });
    return list;
  }, []);

function Thread({ search, stage }: { search: Search; stage: number }) {
  const done = stage >= stepsOf(search);
  const lines = search.steps
    ? segmentsOf(search)
        .filter((segment) => segment.first <= stage)
        .map((segment) => {
          const running = stage <= segment.last && !done;
          const seen = segment.code ? segment.tools : [...new Set(search.steps!.slice(segment.first, Math.min(stage, segment.last) + 1).map((step) => step.tool))];
          return { running, text: segment.code ? (running ? "Searching code…" : "Searched code") : running ? `Exploring ${seen.join(" → ")}…` : `Explored ${segment.tools.join(" → ")}` };
        })
    : [{ running: !done, text: done ? `Searched code${search.funnel ? ` in ${search.funnel.ms} ms` : ""}` : "Searching code…" }];
  return (
    <div className="agent-thread">
      <p className={cn("agent-message", search.kind === "code" && "next-message-code")}>
        {search.kind === "code" && <span className="next-message-meta">{[search.mode, search.language, search.branches === "all" ? "all branches" : null].filter(Boolean).join(" · ")}</span>}
        {search.pattern}
      </p>
      <div className="agent-reply" aria-live="polite">
        <ul className="agent-calls">
          {lines.map((line, index) => (
            <li key={index} className={cn("agent-call", line.running && "agent-call-running")}>
              <Icon name="orbit" size={12} className="agent-call-mark" />
              <span className="agent-call-tool">{line.text}</span>
            </li>
          ))}
        </ul>
        {done && (
          <div className="agent-answer">
            <div className="agent-answer-text">
              <p>{search.summary}</p>
              {search.files.length > 0 && (
                <div className="next-files">
                  {search.files.map((hit) => (
                    <File key={`${hit.project}/${hit.path}`} hit={hit} />
                  ))}
                </div>
              )}
              {search.steps && <p className="next-sample">Sample data. Third-party sources are coming soon.</p>}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// The Orbit Next demo: example searches on the left, the chat in the middle, and room on the right where the
// map behind the hero shows the results. Picking an example runs it in the window.
// Any other pattern gets a note, because live search is not public yet.
export function CodeSearch({ onSearch }: { onSearch?: (search: Search | null) => void }) {
  const [run, setRun] = useState<{ search: Search; start: number } | null>(null);
  const [mode, setMode] = useState<Mode>("structural");
  const [pattern, setPattern] = useState("");
  const [stage, setStage] = useState(0);
  const [unknown, setUnknown] = useState(false);
  useEffect(() => {
    if (!run || stage >= stepsOf(run.search)) return;
    const timer = window.setTimeout(() => setStage((n) => n + 1), run.search.steps ? STEP_MS : CODE_MS);
    return () => window.clearTimeout(timer);
  }, [run, stage]);

  // An example types itself into the input, then sends. The length comes from the time since it started,
  // so a slow timer only skips ahead.
  const [typing, setTyping] = useState<{ search: Search; since: number; length: number } | null>(null);

  useEffect(() => {
    if (!typing) return;
    const total = typing.search.pattern.length;
    if (typing.length >= total) {
      const timer = window.setTimeout(() => {
        setTyping(null);
        start(typing.search);
      }, 220);
      return () => window.clearTimeout(timer);
    }
    // Long patterns type faster, so every example types in about a second at most.
    const per = Math.min(TYPE_MS, 1000 / total);
    const timer = window.setTimeout(() => setTyping({ ...typing, length: Math.min(total, Math.floor((performance.now() - typing.since) / per) + 1) }), per);
    return () => window.clearTimeout(timer);
  }, [typing]);

  const start = (search: Search) => {
    setRun({ search, start: performance.now() });
    onSearch?.(search);
    if (search.kind === "code") setMode(search.mode);
    setPattern("");
    setUnknown(false);
    setStage(still() ? stepsOf(search) : 0);
  };

  const pick = (search: Search) => {
    if (search.kind === "code") setMode(search.mode);
    setUnknown(false);
    if (still()) start(search);
    else setTyping({ search, since: performance.now(), length: 0 });
  };

  const submit = (event: FormEvent) => {
    event.preventDefault();
    const known = ALL.find((item) => item.pattern === pattern.trim());
    if (known) start(known);
    else if (pattern.trim()) setUnknown(true);
  };

  return (
    <div className="next-demo not-prose">
      <div className="next-demo-examples">
        {GROUPS.map((group) => (
          <div key={group.label} role="group" aria-label={group.label} className="next-demo-group">
            <span className="next-demo-label">{group.label}</span>
            {group.items.map((item) => (
              <button key={item.id} type="button" onClick={() => pick(item)} className={cn("next-demo-example", item.id === run?.search.id && "next-demo-example-on")}>
                <Icon name={iconOf(item)} size={14} />
                <span>{item.label}</span>
              </button>
            ))}
          </div>
        ))}
      </div>
      <div className="agent-chat next-chat">
        <div className="agent-chat-head" aria-hidden="true">
          <span className="terminal-lights">
            <span />
            <span />
            <span />
          </span>
          <span className="agent-chat-title">Orbit Next</span>
        </div>
        {run ? <Thread key={run.start} search={run.search} stage={stage} /> : <div className="next-chat-space" />}
        <form onSubmit={submit} className="agent-prompt-box" aria-label="Search">
          <ul className="agent-prompt-chips" aria-label="Search mode">
            {MODES.map((item) => (
              <li key={item.mode}>
                <button type="button" aria-pressed={mode === item.mode} onClick={() => setMode(item.mode)} className={cn("agent-prompt-chip", mode === item.mode && "next-chip-on")}>
                  {item.mode}
                </button>
              </li>
            ))}
          </ul>
          <div className="agent-prompt-row">
            <textarea
              aria-label="Pattern or question"
              rows={1}
              value={typing ? typing.search.pattern.slice(0, typing.length) : pattern}
              placeholder={MODES.find((item) => item.mode === mode)!.placeholder}
              spellCheck={false}
              onChange={(event) => {
                setPattern(event.target.value);
                setUnknown(false);
              }}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) submit(event);
              }}
              className={cn("agent-prompt-input", (typing ? typing.search.kind : "code") === "code" && (typing || pattern) && "next-input-code")}
            />
            <button type="submit" aria-label="Send" title="Send" className="agent-prompt-send">
              <Icon name="arrow-up" size={16} />
            </button>
          </div>
          {unknown && <p className="next-note">Live search is coming soon. Pick an example to see what it does.</p>}
        </form>
      </div>
      {/* The map behind the hero draws each search's findings and path in this column. */}
      <div className="next-demo-viz" aria-hidden="true" />
    </div>
  );
}
