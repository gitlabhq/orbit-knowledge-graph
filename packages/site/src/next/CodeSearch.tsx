import { type FormEvent, useEffect, useState } from "react";
import { chip, cn, Icon } from "@gitlab-org/orbit-site";
import { type Hop, HopField } from "./HopField";
import { CROSS_SEARCHES, type FileHit, type Line, type Mode, MODES, type Search, SEARCHES, STEP_MS } from "./searches";

const still = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const stepsOf = (search: Search) => (search.steps ? search.steps.length : 4);
const toolsOf = (search: Search) => [...new Set(search.steps!.map((step) => step.tool))];
const iconOf = (search: Search) => (search.kind === "cross" ? search.steps![0]!.icon : search.branches === "all" ? "branch" : search.mode === "structural" ? "code" : "search");

// Three code searches and three questions across tools.
const EXAMPLES = [...["unwrap", "branch", "flag"].map((id) => SEARCHES.find((item) => item.id === id)!), ...CROSS_SEARCHES.filter((item) => item.id !== "epic")];
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

// The path of a cross-tool question, as a small map in the answer: the same pulse as the third-party
// section, from tool to tool, timed with the tool line above it.
function PathMap({ search, start }: { search: Search; start: number }) {
  const steps = search.steps!;
  const id = `next-path-${search.id}`;
  const hops: Hop[] = steps.map((step, index) => ({
    tool: step.tool,
    label: step.name,
    x: 0.1 + (index * 0.8) / Math.max(1, steps.length - 1),
    y: index % 2 ? 0.3 : 0.72,
    at: (index + 0.6) / steps.length,
    tone: step.tone,
  }));
  return (
    <div id={id} className="next-pathmap" role="img" aria-label={steps.map((step) => `${step.tool}: ${step.name}`).join(", then ")}>
      <HopField hops={hops} anchor={`#${id}`} loop={steps.length * STEP_MS} start={start} />
    </div>
  );
}

// The thread for one example: the question, one Orbit tool line that grows as the search runs, and the
// answer, all inside the chat window.
function Thread({ search, stage, start }: { search: Search; stage: number; start: number }) {
  const done = stage >= stepsOf(search);
  const tools = search.steps ? toolsOf(search) : [];
  const seen = search.steps ? [...new Set(search.steps.slice(0, Math.min(stage + 1, search.steps.length)).map((step) => step.tool))] : [];
  const tool = search.steps ? (done ? `Explored ${tools.join(" → ")}` : `Exploring ${seen.join(" → ")}…`) : done ? `Searched code, ${search.mode}` : "Searching code…";
  return (
    <div className="agent-thread">
      <p className={cn("agent-message", search.kind === "code" && "next-message-code")}>{search.pattern}</p>
      <div className="agent-reply" aria-live="polite">
        <ul className="agent-calls">
          <li className={cn("agent-call", !done && "agent-call-running")}>
            <Icon name="orbit" size={12} className="agent-call-mark" />
            <span className="agent-call-tool">{tool}</span>
          </li>
        </ul>
        {search.steps && <PathMap search={search} start={start} />}
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

// The Orbit Next demo: example searches beside a chat window. Picking an example runs it in the window.
// Any other pattern gets a note, because live search is not public yet.
export function CodeSearch({ onSearch }: { onSearch?: (search: Search | null) => void }) {
  const [run, setRun] = useState<{ search: Search; start: number } | null>(null);
  const [mode, setMode] = useState<Mode>("structural");
  const [pattern, setPattern] = useState("");
  const [stage, setStage] = useState(0);
  const [unknown, setUnknown] = useState(false);

  useEffect(() => {
    if (!run || stage >= stepsOf(run.search)) return;
    const timer = window.setTimeout(() => setStage((n) => n + 1), STEP_MS);
    return () => window.clearTimeout(timer);
  }, [run, stage]);

  const start = (search: Search) => {
    setRun({ search, start: performance.now() });
    onSearch?.(search);
    if (search.kind === "code") setMode(search.mode);
    setPattern("");
    setUnknown(false);
    setStage(still() ? stepsOf(search) : 0);
  };

  const submit = (event: FormEvent) => {
    event.preventDefault();
    const known = ALL.find((item) => item.pattern === pattern.trim());
    if (known) start(known);
    else if (pattern.trim()) setUnknown(true);
  };

  return (
    <div className="next-demo not-prose">
      <div className="next-demo-examples" role="group" aria-label="Examples">
        <span className="next-demo-label">Try an example</span>
        {EXAMPLES.map((item) => (
          <button key={item.id} type="button" onClick={() => start(item)} className={cn("next-demo-example", item.id === run?.search.id && "next-demo-example-on")}>
            <Icon name={iconOf(item)} size={14} />
            <span>{item.label}</span>
          </button>
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
        {run ? (
          <Thread key={run.start} search={run.search} stage={stage} start={run.start} />
        ) : (
          <p className="next-chat-empty">Search code across every repository and branch, or ask a question that crosses your tools.</p>
        )}
        <form onSubmit={submit} className="next-chat-form" aria-label="Search">
          <div role="group" aria-label="Search mode" className="next-modes">
            {MODES.map((item) => (
              <button key={item.mode} type="button" aria-pressed={mode === item.mode} onClick={() => setMode(item.mode)} className={chip(mode === item.mode)}>
                {item.mode}
              </button>
            ))}
          </div>
          <div className="agent-prompt-row">
            <input
              aria-label="Pattern or question"
              value={pattern}
              placeholder={MODES.find((item) => item.mode === mode)!.placeholder}
              spellCheck={false}
              onChange={(event) => {
                setPattern(event.target.value);
                setUnknown(false);
              }}
              className="agent-prompt-input next-chat-input"
            />
            <button type="submit" aria-label="Send" title="Send" className="agent-prompt-send">
              <Icon name="arrow-up" size={16} />
            </button>
          </div>
          {unknown && <p className="next-note">Live search is coming soon. Pick an example to see what it does.</p>}
        </form>
      </div>
    </div>
  );
}
