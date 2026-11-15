import { type FormEvent, useEffect, useState } from "react";
import { chip, cn, Icon } from "@gitlab-org/orbit-site";
import { type FileHit, type Line, type Mode, MODES, SAMPLE_SEARCHES, type Search, SEARCHES, type Source, SOURCES } from "./searches";

const STAGE_MS = 650;

const still = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const count = (n: number) => n.toLocaleString("en-US");
const sourceOf = (source: Source) => SOURCES.find((item) => item.source === source)!;
const stagesOf = (search: Search) => {
  const tool = sourceOf(search.source).label;
  return search.source === "code" ? ["Plan", "Your permissions", "Prefilter", "Scan"] : ["Plan", `Your access in ${tool}`, `Search ${tool}`, "Link to GitLab"];
};
const ALL = [...SEARCHES, ...SAMPLE_SEARCHES];

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

// Matches grouped by project, then by file, as on the Orbit Next results page. A third-party search lists
// its records instead, each with the GitLab work it links to.
function Results({ search }: { search: Search }) {
  if (search.records) {
    const source = sourceOf(search.source);
    return (
      <div className="next-results">
        <section className="next-project">
          <header className="next-project-head">
            <Icon name={source.icon} size={14} />
            <span className="next-project-name">{source.label}</span>
            <span className="next-count">{search.records.length}</span>
            <span className="next-sample-tag">Sample data</span>
          </header>
          {search.records.map((record, index) => (
            <div key={index} className="next-record">
              <div className="next-record-head">
                <span className="next-record-title">{record.title}</span>
                <span className="next-record-meta">{record.meta}</span>
              </div>
              {record.text && <p className="next-record-text">{record.text}</p>}
              {record.links.length > 0 && (
                <div className="next-record-links">
                  {record.links.map((link) => (
                    <span key={link} className="next-branch">
                      <Icon name="merge-request" size={12} />
                      {link}
                    </span>
                  ))}
                </div>
              )}
            </div>
          ))}
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

// The side panel: the query, its passes as they run, and, for the measured search, how far each pass cut
// the files down.
function Inspector({ search, stage }: { search: Search; stage: number }) {
  const stages = stagesOf(search);
  const f = search.funnel;
  const rows = f && [
    { label: "files in that index", value: f.files },
    { label: "candidates", value: f.candidates },
    { label: "scanned", value: f.scanned },
    { label: "matches", value: f.matches },
  ];
  return (
    <aside className="next-inspector" aria-label="Query details">
      <dl className="next-inspector-query">
        <dt>Source</dt>
        <dd>{sourceOf(search.source).label}</dd>
        {search.source === "code" && (
          <>
            <dt>Mode</dt>
            <dd>{search.mode}</dd>
            {search.language && (
              <>
                <dt>Language</dt>
                <dd>{search.language}</dd>
              </>
            )}
            <dt>Branches</dt>
            <dd>{search.branches === "all" ? "all" : "default"}</dd>
          </>
        )}
      </dl>
      <ol className="next-stages" aria-label="Search passes">
        {stages.map((name, index) => (
          <li key={name} className={cn("next-stage", index < stage && "next-stage-done")}>
            <Icon name={index < stage ? "check" : "dash"} size={12} />
            {name}
          </li>
        ))}
      </ol>
      {rows && stage >= stages.length && (
        <ol className="next-funnel" aria-label="How the search narrowed the files">
          {rows.map((row) => (
            <li key={row.label} className="next-funnel-row">
              <span className="next-funnel-bar" style={{ width: `${Math.max(3, (Math.log10(row.value) / Math.log10(f!.files)) * 100)}%` }} />
              <span className="next-funnel-value">{count(row.value)}</span>
              <span className="next-funnel-label">{row.label}</span>
            </li>
          ))}
        </ol>
      )}
    </aside>
  );
}

// The Orbit Next search page, rebuilt in this site's look: sources, a query bar, and example searches.
// Picking an example runs its passes and shows its recorded results. Any other pattern gets a note,
// because live search is not public yet.
export function CodeSearch({ onSearch }: { onSearch?: (search: Search | null) => void }) {
  const [source, setSource] = useState<Source>("code");
  const [search, setSearch] = useState<Search | null>(null);
  const [mode, setMode] = useState<Mode>("structural");
  const [pattern, setPattern] = useState("");
  const [stage, setStage] = useState(0);
  const [unknown, setUnknown] = useState(false);

  useEffect(() => {
    if (!search || stage >= stagesOf(search).length) return;
    const timer = window.setTimeout(() => setStage((n) => n + 1), STAGE_MS);
    return () => window.clearTimeout(timer);
  }, [search, stage]);

  const run = (next: Search) => {
    setSearch(next);
    onSearch?.(next);
    setSource(next.source);
    setMode(next.mode);
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

  const pick = (next: Source) => {
    setSource(next);
    reset();
  };

  const submit = (event: FormEvent) => {
    event.preventDefault();
    const known = ALL.find((item) => item.source === source && item.pattern === pattern.trim());
    if (known) run(known);
    else if (pattern.trim()) setUnknown(true);
  };

  const examples = ALL.filter((item) => item.source === source);
  const done = search !== null && stage >= stagesOf(search).length;
  const placeholder = source === "code" ? MODES.find((item) => item.mode === mode)!.placeholder : `Search ${sourceOf(source).label}`;
  return (
    <div className="next-search not-prose">
      <form onSubmit={submit} className="next-query" aria-label="Search">
        <div className="next-query-controls">
          <div role="group" aria-label="Source" className="next-modes">
            {SOURCES.map((item) => (
              <button key={item.source} type="button" aria-pressed={source === item.source} onClick={() => pick(item.source)} className={chip(source === item.source)}>
                <Icon name={item.icon} size={12} className="mr-1 inline" />
                {item.label}
              </button>
            ))}
          </div>
          {source === "code" && (
            <div role="group" aria-label="Search mode" className="next-modes">
              {MODES.map((item) => (
                <button key={item.mode} type="button" aria-pressed={mode === item.mode} onClick={() => setMode(item.mode)} className={chip(mode === item.mode)}>
                  {item.mode}
                </button>
              ))}
            </div>
          )}
        </div>
        <div className="next-query-row">
          <Icon name="search" size={16} className="next-query-icon" />
          <input
            aria-label="Pattern"
            value={pattern}
            placeholder={placeholder}
            spellCheck={false}
            onChange={(event) => {
              setPattern(event.target.value);
              setUnknown(false);
            }}
            className={cn("next-query-input", pattern && "next-query-code")}
          />
          <button type="submit" aria-label="Search" title="Search" className="agent-prompt-send">
            <Icon name="arrow-right" size={16} />
          </button>
        </div>
      </form>

      <div className="agent-prompt-presets" role="group" aria-label="Example searches">
        {examples.map((item) => (
          <button key={item.id} type="button" onClick={() => run(item)} className={cn("agent-prompt-preset", item.id === search?.id && "next-preset-on")}>
            <Icon name={item.source !== "code" ? sourceOf(item.source).icon : item.branches === "all" ? "branch" : item.mode === "structural" ? "code" : "search"} size={14} />
            {item.label}
          </button>
        ))}
      </div>

      {unknown && <p className="next-note">Live search is coming soon. Pick an example to see recorded results.</p>}
      {!search && source !== "code" && <p className="next-note">{sourceOf(source).label} search is coming soon. The examples use sample data.</p>}

      {search && (
        <div className="next-panel" aria-live="polite">
          <div className="next-panel-bar">
            <button type="button" onClick={reset} aria-label="Close results" className={chip(false)}>
              <Icon name="close" size={12} />
            </button>
            <span className="next-panel-summary">{done ? search.summary : "Searching…"}</span>
          </div>
          <div className="next-panel-body">
            <div className="min-w-0">{done && <Results search={search} />}</div>
            <Inspector search={search} stage={stage} />
          </div>
        </div>
      )}
    </div>
  );
}
