const SEARCH_PROGRAMS = new Set(["rg", "grep", "egrep", "fgrep", "ag", "ack"]);
const VALUE_FLAGS_SHORT = {
  rg: new Set(["e", "f", "g", "t", "T", "A", "B", "C", "m", "M", "j", "E", "d", "r"]),
  grep: new Set(["e", "f", "m", "A", "B", "C", "d", "D"])
};
const VALUE_FLAGS_LONG = new Set([
  "regexp",
  "file",
  "glob",
  "iglob",
  "type",
  "type-not",
  "type-add",
  "max-count",
  "after-context",
  "before-context",
  "context",
  "max-depth",
  "threads",
  "encoding",
  "ignore-file",
  "pre",
  "include",
  "exclude",
  "exclude-dir",
  "max-filesize",
  "sort",
  "sortr",
  "replace",
  "color",
  "colors"
]);
const VENDORED = /(^|\/)(node_modules|vendor|vendored|third_party|dist|build|target|\.git|\.venv|venv|__pycache__|\.next|coverage)(\/|$)/;
const NON_CODE_EXTENSIONS = new Set([
  "md",
  "markdown",
  "txt",
  "rst",
  "adoc",
  "log",
  "json",
  "jsonl",
  "lock",
  "csv",
  "tsv",
  "svg",
  "xml",
  "yml",
  "yaml",
  "toml",
  "ini",
  "cfg",
  "env",
  "map",
  "min",
  "html",
  "css",
  "snap",
  "patch",
  "diff"
]);
const NON_CODE_TYPES = new Set(["md", "markdown", "txt", "json", "yaml", "yml", "toml", "xml", "html", "css", "log"]);
const STOPWORDS = new Set([
  "string",
  "number",
  "boolean",
  "object",
  "array",
  "error",
  "result",
  "value",
  "values",
  "index",
  "null",
  "undefined",
  "true",
  "false",
  "this",
  "self",
  "return",
  "function",
  "const",
  "import",
  "export",
  "class",
  "default",
  "async",
  "await",
  "public",
  "private",
  "static",
  "Option",
  "Some",
  "None",
  "Promise",
  "Map",
  "Set",
  "Vec",
  "Test",
  "Main",
  "Config",
  "Data",
  "Type",
  "Name",
  "List",
  "Item",
  "Node",
  "Event",
  "Date",
  "Json"
].map((word) => word.toLowerCase()));
const IDENTIFIER = /^[A-Za-z_$][\w$]*(?:(?:::|\.|#)[A-Za-z_$][\w$]*)*$/;
const WORD_SHAPE = /_|[a-z][A-Z]|^[A-Z][a-z]{3,}/;
/** Lookups ignore case, `_` and `-`, so the same term in another spelling counts as handled. */
export function normalizeTerm(term) {
  return term.toLowerCase().replace(/[_-]/g, "");
}
function shellWords(text) {
  const words = [];
  let current = "";
  let started = false;
  let quote;
  for (let i = 0;i < text.length; i++) {
    const char = text[i];
    if (quote) {
      if (char === quote)
        quote = undefined;
      else if (char === "\\" && quote === '"' && i + 1 < text.length)
        current += text[++i];
      else
        current += char;
    } else if (char === "'" || char === '"') {
      quote = char;
      started = true;
    } else if (char === "\\" && i + 1 < text.length) {
      current += text[++i];
      started = true;
    } else if (/\s/.test(char)) {
      if (started)
        words.push(current);
      current = "";
      started = false;
    } else {
      current += char;
      started = true;
    }
  }
  if (quote)
    return;
  if (started)
    words.push(current);
  return words;
}
function pipelines(command) {
  const result = [[""]];
  let quote;
  const stage = () => result[result.length - 1];
  const append = (text) => {
    stage()[stage().length - 1] += text;
  };
  for (let i = 0;i < command.length; i++) {
    const char = command[i];
    if (char === "\\" && quote !== "'" && i + 1 < command.length) {
      append(char + command[++i]);
    } else if (quote) {
      if (char === quote)
        quote = undefined;
      append(char);
    } else if (char === "'" || char === '"') {
      quote = char;
      append(char);
    } else if (char === "|" && command[i + 1] !== "|") {
      stage().push("");
    } else if (char === "&" && (command[i - 1] === ">" || command[i - 1] === "<" || command[i + 1] === ">")) {
      append(char);
    } else if (char === "|" || char === "&" || char === ";" || char === `
`) {
      if (command[i + 1] === char && char !== ";" && char !== `
`)
        i++;
      result.push([""]);
    } else {
      append(char);
    }
  }
  return result;
}
function extension(path) {
  const base = path.split("/").pop() ?? "";
  const dot = base.lastIndexOf(".");
  return dot > 0 ? base.slice(dot + 1).toLowerCase() : "";
}
function parseSearchArguments(program, words) {
  const patterns = [];
  const positionals = [];
  const globs = [];
  const types = [];
  let regexpFlag = false;
  let patternFile = false;
  for (let i = 0;i < words.length; i++) {
    const word = words[i];
    if (word === "--") {
      positionals.push(...words.slice(i + 1));
      break;
    }
    if (word.startsWith("--")) {
      const [name, inline] = word.slice(2).split(/=(.*)/s);
      if (name === "file")
        patternFile = true;
      const value = inline ?? (VALUE_FLAGS_LONG.has(name) ? words[++i] : undefined);
      if (name === "regexp")
        patterns.push(value ?? "");
      if (name === "glob" || name === "iglob" || name === "include")
        globs.push(value ?? "");
      if (name === "type")
        types.push(value ?? "");
      continue;
    }
    if (word.startsWith("-") && word.length > 1) {
      for (let j = 1;j < word.length; j++) {
        const flag = word[j];
        if (!(VALUE_FLAGS_SHORT[program] ?? VALUE_FLAGS_SHORT.grep).has(flag))
          continue;
        const value = word.slice(j + 1) || words[++i] || "";
        if (flag === "f")
          patternFile = true;
        if (flag === "e") {
          patterns.push(value);
          regexpFlag = true;
        }
        if (flag === "g")
          globs.push(value);
        if (flag === "t")
          types.push(value);
        break;
      }
      continue;
    }
    positionals.push(word);
  }
  if (!regexpFlag && patterns.length === 0 && positionals.length > 0)
    patterns.push(positionals.shift());
  return { patterns, paths: positionals, globs, types, patternFile };
}
function judgeSearch(search, isFile) {
  const { patterns, paths, globs, types, patternFile } = search;
  if (patternFile)
    return { skip: "pattern-file" };
  if (patterns.length !== 1)
    return { skip: "multiple-patterns" };
  const [pattern] = patterns;
  if (/\s/.test(pattern))
    return { skip: "text-pattern" };
  if (!IDENTIFIER.test(pattern))
    return { skip: "regex-pattern" };
  if (!WORD_SHAPE.test(pattern))
    return { skip: "plain-word" };
  if (STOPWORDS.has(pattern.toLowerCase()))
    return { skip: "stopword" };
  if (pattern.length < 4)
    return { skip: "too-short" };
  if (paths.some((path) => VENDORED.test(path)))
    return { skip: "vendored" };
  if (types.length > 0 && types.every((type) => NON_CODE_TYPES.has(type)))
    return { skip: "non-code" };
  const scoped = [...paths, ...globs.filter((glob) => !glob.startsWith("!"))];
  if (scoped.length > 0 && scoped.every((path) => NON_CODE_EXTENSIONS.has(extension(path)))) {
    return { skip: "non-code" };
  }
  if (paths.length > 0 && paths.every(isFile))
    return { skip: "explicit-files" };
  return { term: pattern };
}
/** Terms an Orbit command in this Bash call looked up (`glab orbit grep|context|query ...` or `orbit ...`). */
export function orbitTerms(command) {
  const terms = [];
  for (const stages of pipelines(command)) {
    for (const stage of stages) {
      const words = shellWords(stage.trim()) ?? [];
      const direct = words[0]?.split("/").pop() === "orbit";
      const at = direct ? 0 : words.indexOf("orbit");
      if (!direct && (at < 1 || words[at - 1].split("/").pop() !== "glab"))
        continue;
      if (!["grep", "context", "query"].includes(words[at + 1] ?? ""))
        continue;
      for (const word of words.slice(at + 2).filter((word) => !word.startsWith("-"))) {
        for (const token of word.split(/[^\w$:.#]+/))
          if (IDENTIFIER.test(token))
            terms.push(token);
      }
    }
  }
  return terms;
}
/** `{ term }` for a single-identifier code search, else `{ skip: reason }`. `isFile` reports whether a path is an existing file. */
export function analyzeSearch(rawToolName, input, isFile) {
  const toolName = rawToolName.toLowerCase();
  if (toolName === "grep") {
    if (typeof input.pattern !== "string")
      return { skip: "not-search" };
    return judgeSearch({
      patterns: [input.pattern],
      paths: typeof input.path === "string" && input.path ? [input.path] : [],
      globs: typeof input.glob === "string" && input.glob ? [input.glob] : [],
      types: typeof input.type === "string" && input.type ? [input.type] : [],
      patternFile: false
    }, isFile);
  }
  if (toolName !== "bash" || typeof input.command !== "string")
    return { skip: "not-search" };
  const command = input.command;
  if (command.includes("<<"))
    return { skip: "heredoc" };
  let reason = "not-search";
  for (const stages of pipelines(command)) {
    for (let index = 0;index < stages.length; index++) {
      const words = shellWords(stages[index].trim());
      if (!words || words.length === 0)
        continue;
      const program = words[0].split("/").pop() ?? "";
      if (!SEARCH_PROGRAMS.has(program))
        continue;
      if (index > 0) {
        reason = "pipe-filter";
        continue;
      }
      const verdict = judgeSearch(parseSearchArguments(program, words.slice(1)), isFile);
      if ("term" in verdict)
        return verdict;
      reason = verdict.skip;
    }
  }
  return { skip: reason };
}
