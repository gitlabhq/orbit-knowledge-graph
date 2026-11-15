import { type FormEvent, useEffect, useState } from "react";
import { chip, cn, Icon } from "@gitlab-org/orbit-site";
import { CROSS_SEARCHES, type FileHit, type Line, type Mode, MODES, type Search, SEARCHES, STEP_MS } from "./searches";

const still = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
export const stagesOf = (search: Search) => (search.steps ? search.steps.map((step) => `Explore ${step.tool}`) : ["Plan", "Your permissions", "Prefilter", "Scan"]);
const ALL = [...SEARCHES, ...CROSS_SEARCHES];
const iconOf = (search: Search) => (search.kind === "cross" ? search.steps![0]!.icon : search.branches === "all" ? "branch" : search.mode === "structural" ? "code" : "search");

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
        <span className="next-file-path">{hit.path}</span>
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

// Matches grouped by project, then by file, as on the Orbit Next results page. A cross-tool question
// lists the path it took instead: one finding per tool, with the GitLab work each links to.
function Results({ search }: { search: Search }) {
  if (search.steps) {
    return (
      <div className="next-results">
        <section className="next-project">
          <header className="next-project-head">
            <span className="next-project-name">Path across {new Set(search.steps.map((step) => step.tool)).size} tools</span>
            <span className="next-sample-tag">Sample data</span>
          </header>
          <ol className="next-path">
            {search.steps.map((step, index) => (
              <li key={index} className={cn("next-path-step", step.tone && `next-path-${step.tone}`)}>
                <span className="next-path-tool">
                  <Icon name={step.icon} size={12} />
                  {step.tool}
                </span>
                <span className="next-path-finding">{step.finding}</span>
                {step.links && (
                  <span className="next-record-links">
                    {step.links.map((link) => (
                      <span key={link} className="next-branch">
                        <Icon name="merge-request" size={12} />
                        {link}
                      </span>
                    ))}
                  </span>
                )}
              </li>
            ))}
          </ol>
        </section>
      </div>
    );
  }
  const projects = [...new Set(search.files.map((hit) => hit.project))];
  return (
    <div className="next-results">
      {projects.map((project) => {
        const files = search.files.filter((hit) => hit.project === project);
        return (
          <section key={project} className="next-project">
            <header className="next-project-head">
              <span className="next-project-name">{project}</span>
              <span className="next-count">{files.length}</span>
            </header>
            {files.map((hit) => (
              <File key={hit.path} hit={hit} />
            ))}
          </section>
        );
      })}
    </div>
  );
}

// Three code searches and three questions across tools, in one row.
const EXAMPLES = [...["unwrap", "branch", "flag"].map((id) => SEARCHES.find((item) => item.id === id)!), ...CROSS_SEARCHES.filter((item) => item.id !== "epic")];

// The Orbit Next search page, rebuilt in this site's look: a query bar and example searches, for code and
// for questions that cross tools. Picking an example runs its passes and shows its results. Any other
// pattern gets a note, because live search is not public yet.
export function CodeSearch({ onSearch }: { onSearch?: (search: Search | null) => void }) {
  const [search, setSearch] = useState<Search | null>(null);
  const [mode, setMode] = useState<Mode>("structural");
  const [pattern, setPattern] = useState("");
  const [stage, setStage] = useState(0);
  const [unknown, setUnknown] = useState(false);

  useEffect(() => {
    if (!search || stage >= stagesOf(search).length) return;
    const timer = window.setTimeout(() => setStage((n) => n + 1), STEP_MS);
    return () => window.clearTimeout(timer);
  }, [search, stage]);

  const run = (next: Search) => {
    setSearch(next);
    onSearch?.(next);
    if (next.kind === "code") setMode(next.mode);
    setPattern(next.pattern);
    setUnknown(false);
    setStage(still() ? stagesOf(next).length : 0);
  };

  const reset = () => {
    setSearch(null);
    onSearch?.(null);
    setPattern("");
    setUnknown(false);
  };

  const submit = (event: FormEvent) => {
    event.preventDefault();
    const known = ALL.find((item) => item.pattern === pattern.trim());
    if (known) run(known);
    else if (pattern.trim()) setUnknown(true);
  };

  const done = search !== null && stage >= stagesOf(search).length;
  const code = search?.kind !== "cross";
  return (
    <div className="next-search not-prose">
      <form onSubmit={submit} className="next-query" aria-label="Search">
        <div role="group" aria-label="Search mode" className="next-modes">
          {MODES.map((item) => (
            <button key={item.mode} type="button" aria-pressed={code && mode === item.mode} onClick={() => setMode(item.mode)} className={chip(code && mode === item.mode)}>
              {item.mode}
            </button>
          ))}
        </div>
        <div className="next-query-row">
          <Icon name="search" size={16} className="next-query-icon" />
          <input
            aria-label="Pattern"
            value={pattern}
            placeholder={MODES.find((item) => item.mode === mode)!.placeholder}
            spellCheck={false}
            onChange={(event) => {
              setPattern(event.target.value);
              setUnknown(false);
            }}
            className={cn("next-query-input", pattern && code && "next-query-code")}
          />
          <button type="submit" aria-label="Search" title="Search" className="agent-prompt-send">
            <Icon name="arrow-right" size={16} />
          </button>
        </div>
      </form>

      <div className="agent-prompt-presets" role="group" aria-label="Examples">
        {EXAMPLES.map((item) => (
          <button key={item.id} type="button" onClick={() => run(item)} className={cn("agent-prompt-preset", item.id === search?.id && "next-preset-on")}>
            <Icon name={iconOf(item)} size={14} />
            {item.label}
          </button>
        ))}
      </div>

      {unknown && <p className="next-note">Live search is coming soon. Pick an example to see recorded results.</p>}

      {search && (
        <div className="next-panel" aria-live="polite">
          <div className="next-panel-bar">
            <button type="button" onClick={reset} aria-label="Close results" className={chip(false)}>
              <Icon name="close" size={12} />
            </button>
            <span className="next-panel-summary">{done ? search.summary : search.pattern}</span>
          </div>
          <div className="next-panel-body">{done ? <Results search={search} /> : <p className="next-progress">{stagesOf(search)[stage]}…</p>}</div>
        </div>
      )}
    </div>
  );
}
