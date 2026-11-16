import { type FormEvent, useEffect, useLayoutEffect, useRef, useState } from "react";
import { cn, Icon } from "@gitlab-org/orbit-site";
import { CODE_MS, CROSS_SEARCHES, type FileHit, type Line, type Search, SEARCHES, STEP_MS } from "./searches";

const still = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
// What you say to the agent: a code example asks in words, and its tool line shows the search it runs.
const textOf = (search: Search) => search.ask ?? (search.kind === "code" ? search.label : search.pattern);
const TYPE_MS = 22;
// A chat in use fits the window with this much room below it.
const BOTTOM_ROOM_PX = 48;
// What the agent does in each tool, while it runs and once it is done.
const VERBS: Record<string, [string, string]> = {
  "incident.io": ["Reading the incident…", "Read the incident"],
  Grafana: ["Checking Grafana alerts…", "Checked Grafana alerts"],
  GitLab: ["Tracing GitLab deploys…", "Traced GitLab deploys"],
  Jira: ["Reading Jira issues…", "Read Jira issues"],
  Slack: ["Searching Slack…", "Searched Slack"],
  "Code search": ["Searching code…", "Searched code"],
};
const MAX_CHAT_PX = 420;
const SEND_PAUSE_MS = 250;
const stepsOf = (search: Search) => (search.steps ? search.steps.length : 1);
// A code example shows its mode: text, regex, or structural. A question across tools shows its first tool.
const MODE_ICONS = { text: "text-description", regex: "regular-expression", structural: "file-tree" } as const;
const iconOf = (search: Search) => (search.kind === "cross" ? search.steps![0]!.icon : MODE_ICONS[search.mode]);

// One code search per mode, text, regex, then structural, and questions that use third-party connectors.
const GROUPS = [
  { label: "Code search", items: ["error", "flag", "unwrap"].map((id) => SEARCHES.find((item) => item.id === id)!) },
  { label: "Third-party connectors", items: CROSS_SEARCHES.filter((item) => item.id !== "epic") },
];
const ALL = [...SEARCHES, ...CROSS_SEARCHES];
// What the agent can query, as chips on the input and in the third-party section.
export const SOURCES = [
  { label: "GitLab", icon: "tanuki" },
  { label: "Jira", icon: "work-item-issue" },
  { label: "Slack", icon: "comment" },
  { label: "incident.io", icon: "status-alert" },
  { label: "Grafana", icon: "chart" },
];

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

// A file's header: the project's short name, the last two parts of the path, and the branch when it is
// not the default one.
const SHORT_PATH = (path: string) => path.split("/").slice(-2).join("/");
const DEFAULT_BRANCHES = new Set(["main", "master"]);

function File({ hit }: { hit: FileHit }) {
  return (
    <div className="next-file">
      <div className="next-file-head">
        <span className="next-file-path">
          <span className="next-file-project">{hit.project.split("/").at(-1)}</span> {SHORT_PATH(hit.path)}
        </span>
        {!DEFAULT_BRANCHES.has(hit.branch) && (
          <span className="next-branch">
            <Icon name="branch" size={12} />
            {hit.branch}
          </span>
        )}
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

// The thread for one example: the question, the Orbit tool lines as the search runs, and the answer. The path and the findings show on the map behind the chat.
function Thread({ search, stage }: { search: Search; stage: number }) {
  const done = stage >= stepsOf(search);
  const code = done ? "Searched code" : "Searching code…";
  return (
    <div className="agent-thread">
      <p className="agent-message">{textOf(search)}</p>
      <div className="agent-reply" aria-live="polite">
        <ul className="agent-calls">
          {search.steps ? (
            // One line per tool, in the order the path first reaches it, as in the agent log of the
            // third-party section on this page. A tool the path returns to spins on its existing line.
            [...new Set(search.steps.slice(0, Math.min(stage + 1, search.steps.length)).map((step) => step.tool))].map((tool) => {
              const running = !done && search.steps![stage]?.tool === tool;
              return (
                <li key={tool} className={cn("agent-call", running && "agent-call-running")}>
                  <Icon name="orbit" size={12} className="agent-call-mark" />
                  <span className="agent-call-tool">{(VERBS[tool] ?? [`Exploring ${tool}…`, `Explored ${tool}`])[running ? 0 : 1]}</span>
                </li>
              );
            })
          ) : (
            <li className={cn("agent-call", !done && "agent-call-running")}>
              <Icon name="orbit" size={12} className="agent-call-mark" />
              <span className="agent-call-tool">{code}</span>
            </li>
          )}
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
  const [pattern, setPattern] = useState("");
  const [stage, setStage] = useState(0);
  const [unknown, setUnknown] = useState(false);
  // An example types itself into the input, then sends. The length comes from the time since it started,
  // so a slow timer only skips ahead.
  const [typing, setTyping] = useState<{ search: Search; since: number; length: number } | null>(null);

  useEffect(() => {
    if (!typing) return;
    const total = textOf(typing.search).length;
    if (typing.length >= total) {
      const timer = window.setTimeout(() => {
        setTyping(null);
        start(typing.search);
      }, SEND_PAUSE_MS);
      return () => window.clearTimeout(timer);
    }
    // Long patterns type faster, so every example types in about a second at most.
    const per = Math.min(TYPE_MS, 1000 / total);
    const timer = window.setTimeout(() => setTyping({ ...typing, length: Math.min(total, Math.floor((performance.now() - typing.since) / per) + 1) }), per);
    return () => window.clearTimeout(timer);
  }, [typing]);

  // The thread gets the height that keeps the whole chat in the window, measured from the top of the page,
  // and scrolls inside when an answer is longer.
  const chat = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => {
    const element = chat.current;
    if (!element || !run) return;
    const fit = () => {
      const thread = element.querySelector<HTMLElement>(".agent-thread");
      const main = document.getElementById("main");
      if (!thread) return;
      const top = element.getBoundingClientRect().top + (main?.scrollTop ?? 0);
      const rest = element.offsetHeight - thread.offsetHeight;
      // The whole chat keeps one height across examples, whether or not it shows tool chips, and fits the
      // window with room below it.
      const total = Math.min(MAX_CHAT_PX, window.innerHeight - BOTTOM_ROOM_PX - top);
      element.style.setProperty("--thread-h", `${Math.max(140, total - rest)}px`);
    };
    fit();
    window.addEventListener("resize", fit);
    return () => window.removeEventListener("resize", fit);
  }, [run]);

  useEffect(() => {
    if (!run || stage >= stepsOf(run.search)) return;
    const timer = window.setTimeout(() => setStage((n) => n + 1), run.search.steps ? STEP_MS : CODE_MS);
    return () => window.clearTimeout(timer);
  }, [run, stage]);

  const start = (search: Search) => {
    setRun({ search, start: performance.now() });
    onSearch?.(search);
    setPattern("");
    setUnknown(false);
    setStage(still() ? stepsOf(search) : 0);
  };

  const pick = (search: Search) => {
    setUnknown(false);
    if (still()) start(search);
    else setTyping({ search, since: performance.now(), length: 0 });
  };

  const submit = (event: FormEvent) => {
    event.preventDefault();
    const known = ALL.find((item) => textOf(item) === pattern.trim() || item.pattern === pattern.trim());
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
              <button key={item.id} type="button" onClick={() => pick(item)} aria-current={item.id === run?.search.id ? "true" : undefined} className="next-demo-example">
                <Icon name={iconOf(item)} size={14} />
                <span>{item.label}</span>
              </button>
            ))}
          </div>
        ))}
      </div>
      <div ref={chat} className="agent-chat next-chat">
        <div className="agent-chat-head" aria-hidden="true">
          <span className="terminal-lights">
            <span />
            <span />
            <span />
          </span>
          <span className="agent-chat-title">
            <span className="next-chat-name">
              <Icon name="orbit" size={12} />
              Orbit Next
            </span>
          </span>
        </div>
        {run && <Thread key={run.start} search={run.search} stage={stage} />}
        <form onSubmit={submit} className="agent-prompt-box" aria-label="Search">
          {run?.search.steps && (
            // A third-party example shows the tools it reads, as chips above the input.
            <ul className="agent-prompt-chips" aria-label="Tools">
              {[...new Map(run.search.steps.map((step) => [step.tool, step.icon])).entries()].map(([tool, icon]) => (
                <li key={tool} className="agent-prompt-chip">
                  <Icon name={icon} size={12} />
                  {tool}
                </li>
              ))}
            </ul>
          )}
          <div className="agent-prompt-row">
            <textarea
              aria-label="Pattern or question"
              rows={1}
              value={typing ? textOf(typing.search).slice(0, typing.length) : pattern}
              placeholder="Ask about your code, its branches, and your tools"
              spellCheck={false}
              onChange={(event) => {
                setPattern(event.target.value);
                setUnknown(false);
              }}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) submit(event);
              }}
              className="agent-prompt-input"
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
