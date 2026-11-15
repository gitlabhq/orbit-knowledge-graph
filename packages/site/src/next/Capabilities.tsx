import type { CSSProperties } from "react";
import { SEARCHES } from "./searches";

// The branch search from the demo, as one row per branch: main and another branch have no match, and the
// unmerged branch has all 14. The counts are real.
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

// Text and regex search read any file, so the wall ends in "any text file". The first ones are among the
// 20 languages that structural search also parses.
const SHOWN = ["ruby", "go", "rust", "typescript", "python", "java", "kotlin", "vue", "terraform", "yaml", "php", "swift", "scala"];

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

// One recorded match with the lines around it, as a search returns it.
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
        <p>Search any branch or commit, not only the default branch. Find work that has not merged yet, or a bug that is fixed on main but still lives on a release branch.</p>
      </article>
      <article className="next-card">
        <LanguageWall />
        <h3>Every language</h3>
        <p>Text and regex search read every file, in any language. Structural search also parses 20 languages, so a pattern matches code, not comments or strings.</p>
      </article>
      <article className="next-card">
        <Context />
        <h3>With file context</h3>
        <p>Each match comes with the lines around it, the branch it is on, and every project that has the same file.</p>
      </article>
    </div>
  );
}
