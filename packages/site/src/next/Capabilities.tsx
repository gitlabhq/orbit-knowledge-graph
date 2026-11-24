import type { CSSProperties } from "react";
import { SEARCHES } from "./searches";

function Branches() {
  const rows = [
    { branch: "main", matches: 0 },
    { branch: "add-quota-checks", matches: 0 },
    { branch: "cursor-base-pagination-draft", matches: 14 },
  ];
  return (
    <figure className="next-context">
      <figcaption>
        <span>CursorPaginator</span> · all branches
      </figcaption>
      <ul className="next-branch-rows">
        {rows.map((row) => (
          <li key={row.branch} className={row.matches ? "next-context-hit" : undefined}>
            <span>{row.branch}</span>
            <span>{row.matches} {row.matches === 1 ? "match" : "matches"}</span>
          </li>
        ))}
      </ul>
    </figure>
  );
}

const SHOWN = ["typescript", "python", "ruby", "go", "vue", "terraform", "kotlin", "java", "rust", "scala", "swift", "yaml", "php"];

function LanguageWall() {
  return (
    <ul className="next-langs" aria-label="Languages: any language for text and regex search">
      {SHOWN.map((name, index) => (
        <li key={name} className="next-lang" style={{ "--lang-delay": `${(index * 4) % SHOWN.length}00ms` } as CSSProperties}>
          {name}
        </li>
      ))}
      <li className="next-lang next-lang-more">any text file</li>
    </ul>
  );
}

function Context() {
  const hit = SEARCHES.find((search) => search.id === "flag")!.files[0]!;
  return (
    <figure className="next-context">
      <figcaption>
        <span>{hit.project.split("/").at(-1)}</span> {hit.path.split("/").slice(-2).join("/")}
      </figcaption>
      <pre>
        {hit.lines.map((line) => {
          const at = line.match ? line.text.indexOf(line.match) : -1;
          return (
            <span key={line.n} className={line.match ? "next-context-hit" : undefined}>
              <span className="next-file-n">{line.n}</span>
              {at < 0 ? (
                line.text
              ) : (
                <>
                  {line.text.slice(0, at)}
                  <mark>{line.match}</mark>
                  {line.text.slice(at + line.match!.length)}
                </>
              )}
            </span>
          );
        })}
      </pre>
    </figure>
  );
}

export function Capabilities() {
  return (
    <div className="next-bento not-prose">
      <article className="next-card">
        <Branches />
        <h3>Every branch and commit</h3>
        <p>Search every branch and commit, including work that has not merged.</p>
      </article>
      <article className="next-card">
        <LanguageWall />
        <h3>Every language</h3>
        <p>Text and regex read any file. Structural search parses 20 languages.</p>
      </article>
      <article className="next-card">
        <Context />
        <h3>With file context</h3>
        <p>Each match shows the lines around it, its branch, and its project.</p>
      </article>
    </div>
  );
}
