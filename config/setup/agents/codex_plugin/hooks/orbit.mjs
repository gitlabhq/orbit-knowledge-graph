/*! es-toolkit, MIT License, https://github.com/toss/es-toolkit/blob/main/LICENSE */
import { readFileSync as readFileSync2 } from "node:fs";

import { spawnSync } from "node:child_process";
import { appendFileSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

function grepPatternOption(arg) {
  if (arg === "--regexp")
    return "separate";
  if (arg.startsWith("--regexp="))
    return "attached";
  const letters = arg.startsWith("--") ? null : arg.match(/^-[A-Za-z]*?e(.*)$/);
  if (!letters)
    return null;
  return letters[1] ? "attached" : "separate";
}
var LINES_OR_BYTES = /^-[A-Za-z]*[nc]$|^--(?:lines|bytes)$/;
var FILTERS = {
  head: { maxOperands: 0, numberOption: LINES_OR_BYTES },
  tail: { maxOperands: 0, numberOption: LINES_OR_BYTES },
  wc: { maxOperands: 0, fileOptions: /^--files0-from/ },
  cut: { maxOperands: 0, numberOption: /^-[A-Za-z]*[bcf]$|^--(?:bytes|characters|fields)$/ },
  grep: {
    maxOperands: 1,
    numberOption: /^-[A-Za-z]*[mABC]$|^--(?:max-count|after-context|before-context|context)$/,
    fileOptions: /^-[A-Za-z]*[rRdf]|^--(?:recursive|dereference-recursive|directories|file)/,
    patternOption: grepPatternOption
  },
  jq: {
    maxOperands: 1,
    fileOptions: /^-[A-Za-z]*[fL]|^--(?:from-file|slurpfile|rawfile|argfile)/,
    notQuiet: /\b(?:import|include|env)\b|\$ENV|^-[A-Za-z]*n|^--null-input$/
  }
};
function operandCount(spec, args, patternOptions) {
  let operands = 0;
  let optionsEnded = false;
  for (let i = 0;i < args.length; i++) {
    const arg = args[i] ?? "";
    if (optionsEnded || !arg.startsWith("-")) {
      const isNumberValue = !optionsEnded && /^\d+$/.test(arg) && spec.numberOption?.test(args[i - 1] ?? "");
      if (!isNumberValue)
        operands++;
    } else if (arg === "--")
      optionsEnded = true;
    else if (spec.fileOptions?.test(arg))
      return Infinity;
    else if (patternOptions[i] === "separate")
      i++;
  }
  return operands;
}
function isQuietFilter(program, args, readsInput) {
  const spec = Object.hasOwn(FILTERS, program) ? FILTERS[program] : undefined;
  if (!spec || readsInput || args.some((arg) => spec.notQuiet?.test(arg)))
    return false;
  const optionsEnd = args.includes("--") ? args.indexOf("--") : args.length;
  const patternOptions = args.map((arg, i) => (i < optionsEnd ? spec.patternOption?.(arg) : null) ?? null);
  const maxOperands = patternOptions.some(Boolean) ? 0 : spec.maxOperands;
  return operandCount(spec, args, patternOptions) <= maxOperands;
}

var QUOTE_FORMS = {
  single: { close: "'", escapes: false, substitutions: false, lineContinuation: false },
  ansi: { close: "'", escapes: true, substitutions: false, lineContinuation: false },
  double: { close: '"', escapes: true, substitutions: true, lineContinuation: true }
};
var HARMLESS_REDIRECT = /^(?:&>>?|>>?)(?:&\d+|[ \t]*\/dev\/null)(?=[\s;&|)]|$)/;
var SUBSTITUTION = /\$\(|`/;
var PREFIX_WORDS = new Set(["do", "then", "else", "elif", "if", "while", "until", "!", "time", "{"]);
var COMMENT_PRECEDERS = new Set([" ", "\t", `
`, ";", "&", "|", "("]);
function braceEnd(src, open) {
  let depth = 0;
  for (let i = open;i < src.length; i++) {
    const c = src[i];
    if (c === "\\")
      i++;
    else if (c === "'" || c === '"') {
      const { end, closed } = scanQuoted(src, i + 1, c === "'" ? QUOTE_FORMS.single : QUOTE_FORMS.double);
      if (!closed)
        return -1;
      i = end - 1;
    } else if (c === "{" && src[i - 1] === "$")
      depth++;
    else if (c === "}" && --depth === 0)
      return i + 1;
  }
  return -1;
}
function arithmeticEnd(src, open) {
  let depth = 2;
  for (let i = open + 2;i < src.length; i++) {
    if (src[i] === "(")
      depth++;
    else if (src[i] === ")") {
      if (depth === 2 && src[i + 1] === ")")
        return i + 2;
      if (--depth < 2)
        return -1;
    }
  }
  return -1;
}
function parenEnd(src, open) {
  let depth = 0;
  for (let i = open;i < src.length; i++) {
    const c = src[i];
    if (c === "\\")
      i++;
    else if (c === "#" && COMMENT_PRECEDERS.has(src[i - 1] ?? "")) {
      const newline = src.indexOf(`
`, i);
      if (newline < 0)
        return -1;
      i = newline;
    } else if (c === "'" || c === '"') {
      const { end, closed } = scanQuoted(src, i + 1, c === "'" ? QUOTE_FORMS.single : QUOTE_FORMS.double);
      if (!closed)
        return -1;
      i = end - 1;
    } else if (c === "(")
      depth++;
    else if (c === ")" && --depth === 0)
      return i + 1;
  }
  return -1;
}
function startsArithmetic(src, open) {
  return src[open] === "(" && src[open + 1] === "(" && arithmeticEnd(src, open) >= 0;
}
function startsSubstitution(src, at) {
  if (src[at] === "`")
    return true;
  return src[at] === "$" && src[at + 1] === "(" && !startsArithmetic(src, at + 1);
}
function nestedEnd(src, at) {
  if (src[at + 1] === "{")
    return braceEnd(src, at + 1);
  if (src[at + 1] === "(" && !startsArithmetic(src, at + 1))
    return parenEnd(src, at + 1);
  return at;
}
function scanQuoted(src, start, form) {
  let text = "";
  let substitutes = false;
  for (let i = start;i < src.length; i++) {
    const c = src[i] ?? "";
    if (c === form.close)
      return { text, end: i + 1, closed: true, substitutes };
    const nested = form.substitutions && c === "$" ? nestedEnd(src, i) : i;
    if (nested < 0)
      return { text, end: src.length, closed: false, substitutes };
    if (nested > i) {
      text += src.slice(i, nested);
      substitutes ||= SUBSTITUTION.test(src.slice(i, nested));
      i = nested - 1;
    } else if (form.lineContinuation && c === "\\" && src[i + 1] === `
`)
      i++;
    else if (form.escapes && c === "\\" && i + 1 < src.length)
      text += src[++i];
    else {
      if (form.substitutions && startsSubstitution(src, i))
        substitutes = true;
      text += c;
    }
  }
  return { text, end: src.length, closed: false, substitutes };
}
function scanHeredocOperand(src, start) {
  let i = start;
  let delimiter = "";
  let quoted = false;
  while (i < src.length && !/[\s;&|<>()]/.test(src[i] ?? "")) {
    const c = src[i] ?? "";
    if (c === "'" || c === '"') {
      const { text, end } = scanQuoted(src, i + 1, c === "'" ? QUOTE_FORMS.single : QUOTE_FORMS.double);
      quoted = true;
      delimiter += text;
      i = end;
    } else if (c === "\\") {
      quoted = true;
      delimiter += src[i + 1] ?? "";
      i += 2;
    } else {
      delimiter += c;
      i++;
    }
  }
  return { delimiter, quoted, end: i };
}
function scanHeredocBody(src, start, heredoc) {
  let at = start;
  let substitutes = false;
  while (at < src.length) {
    const newline = src.indexOf(`
`, at);
    const line = src.slice(at, newline < 0 ? src.length : newline);
    at = newline < 0 ? src.length : newline + 1;
    if ((heredoc.stripTabs ? line.replace(/^\t+/, "") : line) === heredoc.delimiter)
      return { end: at, closed: true, substitutes };
    if (!heredoc.quoted && SUBSTITUTION.test(line))
      substitutes = true;
  }
  return { end: at, closed: false, substitutes };
}

class Lexer {
  src;
  pos = 0;
  word = null;
  unparsed = false;
  tokens = [];
  heredocs = [];
  constructor(src) {
    this.src = src;
  }
  run() {
    while (this.pos < this.src.length)
      this.step(this.src[this.pos] ?? "");
    this.endWord();
    return { tokens: this.tokens, unparsed: this.unparsed };
  }
  step(c) {
    if (c === "'")
      this.quoted("single", 1);
    else if (c === '"')
      this.quoted("double", 1);
    else if (c === "\\")
      this.escape();
    else if (c === "$")
      this.dollar();
    else if (c === "`")
      this.substitution();
    else if (c === "(" && this.startsArithmeticCommand())
      this.arithmeticCommand();
    else if (c === "<" || c === ">" || c === "&" && this.src[this.pos + 1] === ">")
      this.redirect(c);
    else if (c === "|" && this.src[this.pos + 1] !== "|")
      this.pipe();
    else if (c === `
` || ";&|()".includes(c))
      this.separator(c);
    else if (c === "#" && this.startsComment())
      this.comment();
    else if (c === " " || c === "\t")
      this.whitespace();
    else
      this.plain(c);
  }
  append(text) {
    this.word = (this.word ?? "") + text;
  }
  endWord() {
    if (this.word !== null)
      this.tokens.push({ kind: "word", text: this.word });
    this.word = null;
  }
  currentCommandWords() {
    const words = [];
    for (const token of this.tokens.toReversed()) {
      if (token.kind === "separator")
        break;
      if (token.kind === "word")
        words.unshift(token.text);
    }
    return words;
  }
  startsArithmeticCommand() {
    if (this.word !== null || !startsArithmetic(this.src, this.pos))
      return false;
    const words = this.currentCommandWords();
    return words.every((word) => PREFIX_WORDS.has(word)) || words[0] === "for";
  }
  quoted(form, openingLength) {
    const { text, end, closed, substitutes } = scanQuoted(this.src, this.pos + openingLength, QUOTE_FORMS[form]);
    this.append(text);
    if (substitutes)
      this.tokens.push({ kind: "substitution" });
    if (!closed)
      this.unparsed = true;
    this.pos = end;
  }
  escape() {
    const next = this.src[this.pos + 1];
    if (next === `
`)
      this.pos += 2;
    else if (next === undefined)
      this.plain("\\");
    else {
      this.append(next);
      this.pos += 2;
    }
  }
  dollar() {
    const next = this.src[this.pos + 1];
    if (next === "'")
      this.quoted("ansi", 2);
    else if (startsSubstitution(this.src, this.pos))
      this.substitution();
    else if (next === "(" && startsArithmetic(this.src, this.pos + 1))
      this.arithmeticExpansion();
    else if (next === "{" && braceEnd(this.src, this.pos + 1) >= 0)
      this.parameterExpansion();
    else
      this.plain("$");
  }
  substitution() {
    this.tokens.push({ kind: "substitution" });
    this.pos++;
  }
  skipArithmetic(open) {
    const end = arithmeticEnd(this.src, open);
    if (SUBSTITUTION.test(this.src.slice(open + 2, end - 2)))
      this.tokens.push({ kind: "substitution" });
    this.pos = end;
  }
  arithmeticCommand() {
    this.skipArithmetic(this.pos);
  }
  arithmeticExpansion() {
    this.append("");
    this.skipArithmetic(this.pos + 1);
  }
  parameterExpansion() {
    const end = braceEnd(this.src, this.pos + 1);
    const text = this.src.slice(this.pos, end);
    if (SUBSTITUTION.test(text))
      this.tokens.push({ kind: "substitution" });
    this.append(text);
    this.pos = end;
  }
  redirect(c) {
    const harmless = this.src.slice(this.pos).match(HARMLESS_REDIRECT);
    this.endWordOrDescriptor();
    if (harmless) {
      this.pos += harmless[0].length;
      return;
    }
    if (c === "&")
      this.pos++;
    if (this.src[this.pos] === "<" && this.src[this.pos + 1] === "<" && this.src[this.pos + 2] !== "<")
      this.heredocOperator();
    else
      this.redirectOperator();
  }
  endWordOrDescriptor() {
    const isDescriptor = this.word !== null && /^\d+$/.test(this.word) && /\d/.test(this.src[this.pos - 1] ?? "");
    if (isDescriptor)
      this.word = null;
    else
      this.endWord();
  }
  redirectOperator() {
    const operator = this.src[this.pos] === "<" ? "<" : ">";
    this.pos += operator === "<" && this.src.startsWith("<<<", this.pos) ? 3 : 1;
    if (operator === ">") {
      if (this.src[this.pos] === ">")
        this.pos++;
      if (this.src[this.pos] === "|" || this.src[this.pos] === "&")
        this.pos++;
    }
    this.tokens.push({ kind: "redirect", operator });
  }
  heredocOperator() {
    const stripTabs = this.src[this.pos + 2] === "-";
    const operandStart = this.pos + (stripTabs ? 3 : 2);
    const { delimiter, quoted, end } = scanHeredocOperand(this.src, operandStart + (this.src.slice(operandStart).match(/^[ \t]*/)?.[0].length ?? 0));
    if (delimiter)
      this.heredocs.push({ delimiter, stripTabs, quoted });
    else
      this.unparsed = true;
    this.pos = end;
  }
  pipe() {
    this.endWord();
    this.tokens.push({ kind: "separator", pipe: true });
    this.pos += this.src[this.pos + 1] === "&" ? 2 : 1;
  }
  separator(c) {
    if (c === `
`)
      this.skipHeredocBodies();
    this.endWord();
    this.tokens.push({ kind: "separator", pipe: false });
    this.pos += (c === "&" || c === "|") && this.src[this.pos + 1] === c ? 2 : 1;
  }
  skipHeredocBodies() {
    let at = this.pos + 1;
    for (const heredoc of this.heredocs.splice(0)) {
      const body = scanHeredocBody(this.src, at, heredoc);
      if (body.substitutes)
        this.tokens.push({ kind: "substitution" });
      if (!body.closed)
        this.unparsed = true;
      at = body.end;
    }
    this.pos = at - 1;
  }
  startsComment() {
    return this.word === null && (this.pos === 0 || COMMENT_PRECEDERS.has(this.src[this.pos - 1] ?? ""));
  }
  comment() {
    const newline = this.src.indexOf(`
`, this.pos);
    this.pos = newline < 0 ? this.src.length : newline;
  }
  whitespace() {
    this.endWord();
    this.pos++;
  }
  plain(c) {
    this.append(c);
    this.pos++;
  }
}
function lex(src) {
  return new Lexer(src).run();
}

var MAX_NAME_LENGTH = 40;
var INVISIBLE = /[\p{Cc}\p{Cf}\u{FE00}-\u{FE0F}\u{E0100}-\u{E01EF}]/gu;
function clip(text, max) {
  const flat = text.replace(INVISIBLE, "").replace(/\s+/g, " ").trim();
  return flat.length > max ? flat.slice(0, max - 1) + "…" : flat;
}
function displayName(name) {
  const text = clip(name, MAX_NAME_LENGTH);
  return /[,\s']/.test(text) ? `'${text.replaceAll("'", "\\'")}'` : text;
}
function emptyCommand(piped) {
  return { piped, words: [], readsInput: false, writes: [], substitutes: false };
}
function isEmpty(command) {
  return !command.words.length && !command.writes.length && !command.substitutes;
}
function redirectTarget(tokens, from) {
  let at = from;
  while (tokens[at]?.kind === "substitution")
    at++;
  const token = tokens[at];
  return token?.kind === "word" ? { target: token.text, end: at } : { end: from - 1 };
}
function groupCommands(tokens) {
  const commands = [];
  let current = emptyCommand(false);
  for (let i = 0;i < tokens.length; i++) {
    const token = tokens[i];
    if (token?.kind === "word")
      current.words.push(token.text);
    else if (token?.kind === "substitution")
      current.substitutes = true;
    else if (token?.kind === "redirect") {
      const { target, end } = redirectTarget(tokens, i + 1);
      if (token.operator === "<")
        current.readsInput = true;
      else if (target !== undefined)
        current.writes.push(target);
      current.substitutes ||= tokens.slice(i + 1, end + 1).some((skipped) => skipped.kind === "substitution");
      i = end;
    } else if (token?.kind === "separator") {
      if (!isEmpty(current))
        commands.push(current);
      current = emptyCommand(token.pipe);
    }
  }
  if (!isEmpty(current))
    commands.push(current);
  return commands;
}
function orbitWords(words) {
  let i = 0;
  while (/^(?:ORBIT|GLAB)_\w*=/.test(words[i] ?? ""))
    i++;
  if (words[i] === "glab" && words[i + 1] === "orbit")
    i += 2;
  else if (words[i]?.split("/").pop() === "orbit")
    i += 1;
  else
    return null;
  while (words[i]?.startsWith("-"))
    i++;
  return words.slice(i);
}
var ASSIGNMENT = /^[A-Za-z_]\w*(?:\[[^\]]*\])?\+?=/;
var LOOP_HEADERS = new Set(["for", "select"]);
function withoutKeywords(words) {
  let i = 0;
  for (;i < words.length; i++) {
    const word = words[i] ?? "";
    if (word === "case")
      return [];
    if (LOOP_HEADERS.has(word)) {
      const body = words.findIndex((candidate, at) => at > i && (candidate === "do" || candidate === "{"));
      return body < 0 ? [] : withoutKeywords(words.slice(body + 1));
    }
    if (word === "function")
      i++;
    else if (PREFIX_WORDS.has(word)) {
      if (word === "time" && words[i + 1] === "-p")
        i++;
    } else
      break;
  }
  return words.slice(i);
}
function runnable(command) {
  const { tokens, unparsed } = lex(command);
  const runs = groupCommands(tokens).map((simple) => ({ ...simple, words: withoutKeywords(simple.words) })).filter((run) => !isEmpty(run));
  return { runs, unparsed };
}
function orbitInvocations(command) {
  return runnable(command).runs.map(({ words }) => orbitWords(words)).filter((words) => words !== null);
}
var SHELL_WORDS = new Set(["cd", "pushd", "popd", "true", ":", "fi", "done", "esac", "}"]);
function isQuiet(program, args, run) {
  if (program.includes("/"))
    return false;
  return SHELL_WORDS.has(program) || run.piped && isQuietFilter(program, args, run.readsInput);
}
function otherCommands(command, orbitExtras = []) {
  const { runs, unparsed } = runnable(command);
  const listed = new Map;
  const writes = new Map;
  if (unparsed)
    listed.set("unparsed", "unparsed");
  for (const run of runs) {
    if (run.substitutes)
      listed.set("$(...)", "$(...)");
    for (const target of run.writes)
      writes.set(target, "> " + displayName(target));
    if (orbitWords(run.words))
      continue;
    const at = run.words.findIndex((word) => !ASSIGNMENT.test(word));
    const program = run.words[at];
    if (program === undefined || SUBSTITUTION.test(program))
      continue;
    if (!isQuiet(program, run.words.slice(at + 1), run))
      listed.set(program, displayName(program));
  }
  return [...listed.values(), ...orbitExtras, ...writes.values()];
}
function subcommandOf(words, known) {
  const [sub, ...rest] = words;
  if (sub === undefined)
    return null;
  const nested = rest.findIndex((word) => known(sub + " " + word));
  if (nested >= 0)
    return { key: sub + " " + rest[nested], args: rest.slice(nested + 1) };
  return known(sub) ? { key: sub, args: rest } : null;
}

/*! es-toolkit, MIT License, https://github.com/toss/es-toolkit/blob/main/LICENSE */
function f(o, t) {
  let m = {};
  for (let r = 0;r < o.length; r++) {
    let p = o[r], i = t(p, r, o);
    m[i] = (m[i] ?? 0) + 1;
  }
  return m;
}
function e(o, t) {
  let m = new Map;
  for (let r = 0;r < o.length; r++) {
    let p = o[r], i = t(p, r, o);
    if (!m.has(i))
      m.set(i, p);
  }
  return Array.from(m.values());
}
function s(o, t) {
  let m = 0;
  for (let r = 0;r < o.length; r++)
    m += t(o[r], r);
  return m;
}

function queryOf(command) {
  for (const words of orbitInvocations(command)) {
    if (words[0] !== "query" || words.includes("--file"))
      continue;
    for (let i = 1;i < words.length; i++) {
      const word = words[i] ?? "";
      if (word === "--response-format")
        i++;
      else if (!word.startsWith("-"))
        return word;
    }
  }
  return "";
}
function hopsOf(query) {
  const patterns = query.split(/\bMATCH\b/i).slice(1).map((clause) => clause.split(/\b(?:WHERE|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT|CALL)\b/i)[0] ?? "");
  const hops = patterns.flatMap((pattern) => [...pattern.matchAll(/<?-(?:\[([^\]]*)\])?->?/g)]);
  return s(hops, (hop) => {
    const range = (hop[1] ?? "").match(/\*\s*(\d+)?/);
    return range ? Number(range[1] ?? 1) : 1;
  });
}
var NO_ENTITIES = new Set;
var UNIQUE_KEYS = new Set(["id", "full_path", "username"]);
function entityOfKey(key, known) {
  const m = key.match(/^(\w+)_id$/);
  if (!m?.[1])
    return;
  const name = m[1].split("_").map((w) => (w[0]?.toUpperCase() ?? "") + w.slice(1)).join("");
  return known.has(name) ? name : undefined;
}
function pinOf(keys, equals, label, known) {
  const one = equals.some((key) => UNIQUE_KEYS.has(key));
  if (keys.length === 0)
    return { score: 0, one };
  const implied = keys.map((key) => entityOfKey(key, known)).find((name) => name && name !== label);
  return { score: keys.includes("id") ? 2 : 1, one, ...implied ? { implied } : {} };
}
function pluralize(word) {
  if (/[^aeiou]y$/i.test(word))
    return word.slice(0, -1) + "ies";
  if (/(s|x|z|ch|sh)$/i.test(word))
    return word + "es";
  return word + "s";
}
function displayOf({ type, one }) {
  if (type === "any")
    return "Anything";
  const words = type.split(/(?<=[a-z0-9])(?=[A-Z])/).map((w, i) => i === 0 ? w : w.toLowerCase());
  if (!one)
    words[words.length - 1] = pluralize(words[words.length - 1] ?? "");
  return words.join(" ");
}
function pathOf(run, separator = " → ") {
  const nodes = [];
  for (const node of run) {
    const last = nodes[nodes.length - 1];
    if (last?.type === node.type)
      last.one &&= node.one;
    else
      nodes.push({ ...node });
  }
  return { nodes, text: nodes.map(displayOf).join(separator) };
}
function entitiesOf(text, known = NO_ENTITIES) {
  const labels = {};
  for (const m of text.matchAll(/\(\s*(\w+)\s*:\s*(\w+)/g)) {
    if (m[1] && m[2])
      labels[m[1]] ??= m[2];
  }
  const whereKeys = {};
  const whereEquals = {};
  for (const clause of text.split(/\bWHERE\b/i).slice(1)) {
    const where = clause.split(/\b(?:MATCH|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT)\b/i)[0] ?? "";
    for (const m of where.matchAll(/\b(\w+)\.(\w+)\s*(=|<>|!=|<=|>=|<|>|\bIN\b|\bSTARTS\b|\bENDS\b|\bCONTAINS\b)/gi)) {
      if (!m[1] || !m[2])
        continue;
      (whereKeys[m[1]] ??= []).push(m[2]);
      if (m[3] === "=")
        (whereEquals[m[1]] ??= []).push(m[2]);
    }
  }
  const paths = [];
  for (const segment of text.split(/\bMATCH\b/i).slice(1)) {
    const pattern = segment.split(/\b(?:WHERE|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT|CALL)\b/i)[0] ?? "";
    for (const part of splitTopLevel(pattern)) {
      const nodes = [];
      const pins = [];
      const hops = [];
      for (const m of part.matchAll(/\(([^)]*)\)|(<)?-(?:\[[^\]]*\])?-(>)?/g)) {
        if (m[1] !== undefined) {
          const [, name = "", label] = m[1].match(/^\s*(\w*)\s*(?::\s*(\w+))?/) ?? [];
          const node = label || labels[name] || "any";
          const inline = [...(m[1].match(/\{([^}]*)\}/)?.[1] ?? "").matchAll(/(\w+)\s*:/g)].map((k) => k[1] ?? "");
          const where = name ? whereKeys[name] ?? [] : [];
          const equals = name ? whereEquals[name] ?? [] : [];
          nodes.push(node);
          pins.push(pinOf([...inline, ...where], [...inline, ...equals], node, known));
        } else
          hops.push(m[2] ? "left" : m[3] ? "right" : "none");
      }
      const typed = nodes.map((type, i) => ({ type, one: pins[i]?.one ?? false }));
      paths.push(...readFromPin(typed, pins) ?? readForward(typed, hops));
    }
  }
  const unique = e(paths, (p) => p.text);
  const walks = unique.filter((p) => p.nodes.length > 1);
  const kept = unique.filter((p) => p.nodes.length > 1 || !walks.some((w) => w.nodes.some((n) => n.type === p.nodes[0]?.type)));
  if (kept.length)
    return kept.map((p) => p.text).join(", ");
  const names = e([...text.matchAll(/\\?"entity\\?"\s*:\s*\\?"(\w+)/g)].map((m) => m[1] ?? ""), (name) => name).filter(Boolean);
  return names.map((type) => displayOf({ type, one: false })).join(" → ");
}
function readFromPin(nodes, pins) {
  const best = Math.max(0, ...pins.map((p) => p.score));
  const at = pins.findIndex((p) => p.score === best);
  if (best === 0 || pins.filter((p) => p.score === best).length > 1)
    return null;
  const implied = pins[at]?.implied;
  const sides = [];
  if (at < nodes.length - 1)
    sides.push(nodes.slice(at));
  if (at > 0)
    sides.push(nodes.slice(0, at + 1).reverse());
  if (sides.length === 0)
    sides.push(nodes.slice(at, at + 1));
  return sides.map((side) => pathOf(implied ? [{ type: implied, one: true }, ...side] : side));
}
function readForward(nodes, hops) {
  if (hops.length === 0)
    return nodes[0] ? [pathOf([nodes[0]])] : [];
  const paths = [];
  let start = 0;
  for (let i = 1;i <= hops.length; i++) {
    if (i < hops.length && hops[i] === hops[start])
      continue;
    const run = nodes.slice(start, i + 1);
    if (hops[start] === "left")
      run.reverse();
    paths.push(pathOf(run, hops[start] === "none" ? " — " : " → "));
    start = i;
  }
  return paths;
}
function splitTopLevel(text) {
  const parts = [""];
  let depth = 0;
  for (const ch of text) {
    if ("([{".includes(ch))
      depth++;
    else if (")]}".includes(ch))
      depth--;
    if (ch === "," && depth === 0)
      parts.push("");
    else
      parts[parts.length - 1] += ch;
  }
  return parts.map((p) => p.trim()).filter(Boolean);
}
function listed(text) {
  return [...text.matchAll(/'([^']*)'|"([^"]*)"|(\d+)/g)].map((m) => m[1] ?? m[2] ?? m[3] ?? "");
}
function scopesOf(query) {
  const ids = [];
  const groupIds = [];
  const paths = [];
  for (const m of query.matchAll(/\bproject_id\s*(?::|=)\s*(\d+)/g))
    ids.push(m[1] ?? "");
  for (const m of query.matchAll(/\bproject_id\s+IN\s*\[([^\]]*)\]/gi))
    ids.push(...listed(m[1] ?? ""));
  for (const m of query.matchAll(/\(\s*(\w*)\s*:\s*(Project|Group)\b\s*(\{[^}]*\})?/g)) {
    const idsOf = m[2] === "Group" ? groupIds : ids;
    const map = m[3] ?? "";
    idsOf.push(...[...map.matchAll(/\bid\s*:\s*(\d+)/g)].map((x) => x[1] ?? ""));
    paths.push(...[...map.matchAll(/\bfull_path\s*:\s*['"]([^'"]+)['"]/g)].map((x) => x[1] ?? ""));
    const name = m[1];
    if (!name)
      continue;
    for (const w of query.matchAll(new RegExp(`\\b${name}\\.(id|full_path)\\s*(=|IN)\\s*(\\[[^\\]]*\\]|'[^']*'|"[^"]*"|\\d+)`, "gi"))) {
      (w[1]?.toLowerCase() === "id" ? idsOf : paths).push(...listed(w[3] ?? ""));
    }
  }
  const numeric = (id) => /^\d+$/.test(id);
  const scopes = [
    ...ids.filter(numeric).map((id) => ({ key: "project-id:" + id, flags: ["--project-id", id] })),
    ...groupIds.filter(numeric).map((id) => ({ key: "namespace-id:" + id, flags: ["--namespace-id", id] })),
    ...paths.filter(Boolean).map((path) => ({ key: "full-path:" + path, flags: ["--full-path", path] }))
  ];
  return e(scopes, (scope) => scope.key);
}

var labels_default = {
  commands: {
    query: { done: "Explored Orbit", running: "Exploring Orbit" },
    schema: { done: "Read Orbit schema", running: "Reading Orbit schema" },
    ontology: { done: "Read Orbit schema", running: "Reading Orbit schema" },
    status: { done: "Checked Orbit", running: "Checking Orbit" },
    "graph-status": { done: "Checked indexing", running: "Checking indexing" },
    grep: { done: "Searched code", running: "Searching code" },
    context: { done: "Read code", running: "Reading code" },
    "workspace index": { done: "Indexed repo", running: "Indexing repo" },
    "workspace sql": { done: "Queried local graph", running: "Querying local graph" },
    "workspace schema": { done: "Read local schema", running: "Reading local schema" },
    "workspace list": { done: "Listed repos", running: "Listing repos" },
    "workspace reset": { done: "Reset local graph", running: "Resetting local graph" },
    index: { done: "Indexed repo", running: "Indexing repo" },
    sql: { done: "Queried local graph", running: "Querying local graph" },
    list: { done: "Listed repos", running: "Listing repos" },
    skills: { done: "Read Orbit guide", running: "Reading Orbit guide" },
    skill: { done: "Read Orbit guide", running: "Reading Orbit guide" },
    setup: { done: "Set up Orbit", running: "Setting up Orbit" },
    uninstall: { done: "Removed Orbit setup", running: "Removing Orbit setup" },
    "config get": { done: "Read Orbit settings", running: "Reading Orbit settings" },
    "config list": { done: "Read Orbit settings", running: "Reading Orbit settings" },
    "config set": { done: "Changed Orbit settings", running: "Changing Orbit settings" },
    version: { done: "Checked version", running: "Checking version" }
  },
  tools: {
    query_graph: { done: "Explored Orbit", running: "Exploring Orbit" },
    get_graph_schema: { done: "Read Orbit schema", running: "Reading Orbit schema" },
    get_query_dsl: { done: "Read query syntax", running: "Reading query syntax" },
    get_response_format: { done: "Read response format", running: "Reading response format" },
    list_commands: { done: "Listed Orbit tools", running: "Listing Orbit tools" }
  }
};

var MARK = "◎";
var COMMAND_LABELS = labels_default.commands;
var TOOL_LABELS = labels_default.tools;
var QUERY = labels_default.commands.query;
var SCHEMA = labels_default.commands.schema;
var QUERY_LABEL = QUERY.done;
var VALUE_FLAGS = new Set([
  "--db",
  "--file",
  "--response-format",
  "--full-path",
  "--namespace-id",
  "--project-id",
  "--repo",
  "--budget",
  "--timeout"
]);
var VALUE_FLAGS_BY_KEY = {
  grep: new Set(["-A", "-B", "-C", "-m", "-g", "--glob", "--include", "--exclude", "--exclude-dir", "-t", "--type", "-T", "--type-not", "-M", "--max-columns"]),
  "workspace sql": new Set(["-f", "-F", "--format"])
};
var SHOWS_ARGUMENT = new Set(["grep", "context", "workspace sql", "sql", "schema", "ontology", "graph-status", "skills", "skill"]);
function labelFor(table, key) {
  return Object.hasOwn(table, key) ? table[key] : undefined;
}
function callOf(label, detail) {
  return { label: label.done, active: label.running, detail };
}
function firstArg(args, valueFlags) {
  for (let i = 0;i < args.length; i++) {
    const arg = args[i] ?? "";
    if (arg === "-e" || arg === "--regexp")
      return args[i + 1] ?? "";
    if (VALUE_FLAGS.has(arg) || valueFlags.has(arg))
      i++;
    else if (!arg.startsWith("-"))
      return arg;
  }
  return "";
}
function detailFor(key, args, known) {
  if (key === "query") {
    const text = args.join(" ");
    const schema = text.match(/CALL\s+db\.schema\(\s*'?(\w*)/i);
    if (schema)
      return schema[1] ?? "";
    const file = args.indexOf("--file");
    return entitiesOf(text, known) || (file >= 0 ? args[file + 1] ?? "" : "");
  }
  if (!SHOWS_ARGUMENT.has(key))
    return "";
  const valueFlags = VALUE_FLAGS_BY_KEY[key] ?? new Set;
  return clip(firstArg(key.startsWith("skill") && args[0] === "get" ? args.slice(1) : args, valueFlags), 48);
}
function stringField(input, key) {
  const value = typeof input === "object" && input !== null ? input[key] : undefined;
  return typeof value === "string" ? value : undefined;
}
var MAX_DETAIL = 96;
var MAX_OTHERS_SHOWN = 6;
var CHANGES_STATE = new Set([
  "orbit setup",
  "orbit uninstall",
  "orbit index",
  "orbit workspace index",
  "orbit workspace reset",
  "orbit config set"
]);
function invocationOf(words, known) {
  if (words.length === 0 || words.some((arg) => arg === "--help" || arg === "-h"))
    return null;
  const sub = subcommandOf(words, (key) => labelFor(COMMAND_LABELS, key) !== undefined);
  if (!sub)
    return { call: null, name: "orbit " + displayName(words[0] ?? "") };
  const isSchemaQuery = sub.key === "query" && sub.args.some((arg) => /^\s*CALL\s+db\.schema\(/i.test(arg));
  const label = isSchemaQuery ? SCHEMA : labelFor(COMMAND_LABELS, sub.key) ?? QUERY;
  return { call: callOf(label, detailFor(sub.key, sub.args, known)), name: "orbit " + sub.key };
}
function describeCommand(command, known = NO_ENTITIES) {
  const invocations = orbitInvocations(command).flatMap((words) => invocationOf(words, known) ?? []);
  const calls = invocations.flatMap(({ call }) => call ?? []);
  const [first] = calls;
  if (!first)
    return null;
  const detail = [...new Set(calls.filter((call) => call.label === first.label).map((call) => call.detail).filter(Boolean))].join(", ");
  const otherOrbit = invocations.filter(({ call, name }) => call === null || call.label !== first.label && CHANGES_STATE.has(name)).map(({ name }) => name);
  const others = otherCommands(command, [...new Set(otherOrbit)]);
  return { ...first, detail: clip(detail, MAX_DETAIL), ...others.length ? { others } : {} };
}
function othersText(call) {
  if (!call.others?.length)
    return "";
  const shown = call.others.slice(0, MAX_OTHERS_SHOWN);
  const hidden = call.others.length - shown.length;
  return "  + " + shown.join(", ") + (hidden > 0 ? `, … +${hidden} more` : "");
}
var SHELL_TOOLS = new Set(["bash", "shell"]);
function describeOrbitCall(tool, input, known = NO_ENTITIES) {
  const command = stringField(input, "command");
  if (SHELL_TOOLS.has(tool.toLowerCase()))
    return command === undefined ? null : describeCommand(command, known);
  const [, server = "", name = tool] = tool.match(/^mcp__(.+?)__(.+)$/) ?? [];
  if (!/orbit|knowledge[-_]?graph|gkg/i.test(server) && !/orbit|query_graph|get_graph_schema/i.test(name))
    return null;
  const invoked = stringField(input, "command_name") ?? command ?? name;
  const key = Object.keys(TOOL_LABELS).find((k) => invoked.endsWith(k));
  return callOf(key && labelFor(TOOL_LABELS, key) || QUERY, input === undefined ? "" : entitiesOf(JSON.stringify(input), known));
}

function parseResponse(text) {
  const out = { nodes: 0, edges: 0, byEntity: {}, aggregated: 0, kind: "other", grouped: false };
  const raw = text.trim().startsWith("{") ? tryJson(text) : null;
  if (raw) {
    const r = raw["result"] ?? raw;
    const nodes = Array.isArray(r.nodes) ? r.nodes : [];
    out.byEntity = f(nodes, (node) => String(node.type));
    out.nodes = nodes.length;
    out.edges = Array.isArray(r.edges) ? r.edges.length : 0;
    if (Array.isArray(r.nodes))
      out.kind = "traversal";
    return out;
  }
  if (/^query_type:\s*aggregation/m.test(text)) {
    out.kind = "aggregation";
    out.grouped = /^group_columns\[[1-9]\d*\]/m.test(text);
    const counts = plainCountNames(text);
    const header = text.match(/^rows\[\d+\]\{([^}]*)\}:\s*$/m);
    if (!header || header.index === undefined || counts.length === 0)
      return out;
    const names = (header[1] ?? "").split(",");
    const rows = text.slice(header.index + header[0].length).split(/^\S/m)[0] ?? "";
    const lines = rows.split(`
`).map((line) => line.trim().split(","));
    const cellValue = (cells, i) => /^\d+$/.test(cells[i] ?? "") ? Number(cells[i]) : 0;
    const totals = names.flatMap((name, i) => counts.includes(name) ? [s(lines, (cells) => cellValue(cells, i))] : []);
    out.aggregated = Math.max(0, ...totals);
    return out;
  }
  if (/^query_type:\s*traversal/m.test(text))
    out.kind = "traversal";
  const nodeBlock = text.split(/^nodes:\s*$/m)[1]?.split(/^\S/m)[0] ?? "";
  const sets = [...nodeBlock.matchAll(/^\s+(\w+)\[(\d+)\]\{/gm)].map(([, type = "", count = "0"]) => ({ type, count: Number(count) }));
  for (const { type, count } of sets)
    out.byEntity[type] = (out.byEntity[type] ?? 0) + count;
  out.nodes = s(sets, (set) => set.count);
  if (sets.length)
    out.kind = "traversal";
  out.edges = Number(text.match(/^edges\[(\d+)\]/m)?.[1] ?? 0);
  return out;
}
function columnFields(text) {
  const section = text.match(/^columns\[\d+\](?:\{([^}]*)\})?:\s*$/m);
  if (!section || section.index === undefined)
    return [];
  const body = text.slice(section.index + section[0].length).split(/^\S/m)[0] ?? "";
  if (section[1] !== undefined) {
    const fieldNames = section[1].split(",");
    return body.split(`
`).filter((line) => line.trim()).map((line) => Object.fromEntries(line.trim().split(",").map((cell, i) => [fieldNames[i] ?? "", cell.trim()])));
  }
  return body.split(/^\s+- /m).slice(1).map((item) => Object.fromEntries([...item.matchAll(/^\s*(?:- )?(\w+):\s*(.*)$/gm)].map((m) => [m[1] ?? "", m[2]?.trim()])));
}
function plainCountNames(text) {
  return columnFields(text).filter(({ function: fn, name, target = "", property }) => fn === "count" && name && (!property || name === `count_${target}_${property}`)).map((column) => column["name"] ?? "");
}
function apiCallsFor(counts) {
  const pages = (records) => Math.max(1, Math.ceil(records / 100));
  if (counts.kind === "traversal")
    return pages(Math.max(0, ...Object.values(counts.byEntity)));
  if (counts.kind === "aggregation")
    return counts.grouped ? pages(counts.aggregated) : 1;
  return 0;
}
function tryJson(text) {
  try {
    const value = JSON.parse(text);
    return typeof value === "object" && value !== null ? value : null;
  } catch {
    return null;
  }
}
function graphStatus(json) {
  try {
    const domains = JSON.parse(json).itemCounts?.domains;
    if (!Array.isArray(domains))
      return null;
    const entities = domains.flatMap((domain) => domain.entities ?? []);
    return {
      total: s(entities, (entity) => Number(entity.count) || 0),
      entities: entities.map((entity) => String(entity.name ?? "")).filter(Boolean)
    };
  } catch {
    return null;
  }
}
function scopeIdentity(json, kind) {
  try {
    const body = JSON.parse(json);
    const path = kind === "group" ? body.full_path : body.path_with_namespace;
    if (typeof path !== "string" || !path || typeof body.id !== "number" && typeof body.id !== "string")
      return null;
    return { kind, id: String(body.id), path };
  } catch {
    return null;
  }
}
function identityLookups(flags) {
  const [flag, value] = flags;
  if (!value)
    return [];
  const lookup = (kind, collection) => ({ kind, argv: ["glab", "api", collection + "/" + encodeURIComponent(value)] });
  if (flag === "--namespace-id")
    return [lookup("group", "groups")];
  if (flag === "--project-id")
    return [lookup("project", "projects")];
  return [lookup("group", "groups"), lookup("project", "projects")];
}
function countedTotal(scopes) {
  const distinct = new Map;
  for (const scope of scopes)
    distinct.set(scope.identity ? scope.identity.kind + ":" + scope.identity.id : scope.key, scope);
  const groups = [...distinct.values()].flatMap((scope) => scope.identity?.kind === "group" ? [scope.identity] : []);
  const isInsideCountedGroup = ({ identity }) => identity !== undefined && groups.some((group) => identity.path.startsWith(group.path + "/"));
  return s([...distinct.values()].filter((scope) => !isInsideCountedGroup(scope)), (scope) => scope.total);
}
function grouped(n) {
  return String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}
function compact(n) {
  if (n < 1000)
    return String(n);
  if (n < 1e4)
    return (n / 1000).toFixed(1).replace(/\.0$/, "") + "k";
  if (n < 1e6)
    return Math.round(n / 1000) + "k";
  return (n / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
}

var BLUE = "#949dcb";
var FADE_STOPS = ["#b5dedc", BLUE, "#6851ac"];
var TOKEN_STYLE = {
  keyword: { color: BLUE, bold: true },
  label: { color: FADE_STOPS[0] },
  relationship: { color: FADE_STOPS[0], italic: true },
  string: { color: "success" },
  number: { color: "warning" },
  function: { bold: true },
  property: {},
  variable: {},
  punctuation: { dimColor: true },
  comment: { dimColor: true, italic: true },
  space: {}
};

function rgb(hex) {
  const n = parseInt(hex.slice(1), 16);
  return [n >> 16 & 255, n >> 8 & 255, n & 255].join(";");
}
var COLOR = { blue: "38;2;" + rgb(BLUE), warning: "33", error: "31" };
function colorsOn(env = process.env) {
  return !env["NO_COLOR"] && Boolean(env["TERM"]) && env["TERM"] !== "dumb";
}
function paint(text, style, colors) {
  const codes = [style.bold ? "1" : "", style.dim ? "2" : "", style.color ? COLOR[style.color] : ""].filter(Boolean);
  return colors && text && codes.length ? `\x1B[${codes.join(";")}m${text}\x1B[0m` : text;
}

var STALE_MS = 7 * 24 * 60 * 60 * 1000;
var MAX_PROJECT_TOTALS = 8;
var SCHEMA_LABEL = COMMAND_LABELS["schema"]?.done;
var CUT_HEADER = /^(?:Warning: truncated output \(.*\)\n)?(?:Total output lines: \d+\n)?(?:\.\.\. \d+ bytes omitted \.\.\.\n)?\n?/;
var ORBIT_ERROR = /^\s*(?:Orbit |Error: |error: |not authenticated|rate limited|\w+ not found\b)/;
function text(value) {
  return typeof value === "string" ? value : "";
}
function isTally(line) {
  return typeof line.calls === "number";
}
function responseText(response) {
  if (typeof response === "string")
    return response;
  if (typeof response !== "object" || response === null)
    return "";
  const { content, structuredContent, isError } = response;
  if (isError === true)
    return null;
  const blocks = Array.isArray(content) ? content : [];
  const joined = blocks.flatMap((block) => {
    const blockText = block?.text;
    return typeof blockText === "string" ? [blockText] : [];
  }).join(`
`);
  return joined || (structuredContent === undefined || structuredContent === null ? "" : JSON.stringify(structuredContent));
}
function tallyOf(input) {
  const call = describeOrbitCall(text(input.tool_name), input.tool_input);
  if (!call)
    return null;
  const response = responseText(input.tool_response);
  if (response === null)
    return null;
  const isGraphRead = call.label === QUERY_LABEL || call.label === SCHEMA_LABEL;
  if (isGraphRead && ORBIT_ERROR.test(response.replace(CUT_HEADER, "")))
    return null;
  const tally = { calls: 1, nodes: 0, edges: 0, apiCalls: 0 };
  if (call.label !== QUERY_LABEL)
    return tally;
  const command = text(input.tool_input?.command);
  const parsed = parseResponse(response);
  tally.nodes = parsed.nodes + parsed.aggregated;
  tally.edges = parsed.edges + parsed.aggregated * hopsOf(queryOf(command));
  tally.apiCalls = apiCallsFor(parsed);
  return tally;
}
function rowOf(call, failed, colors) {
  return [
    paint(MARK, { color: failed ? "error" : "blue", bold: true }, colors),
    " ",
    paint(call.label, { color: "blue", bold: true }, colors),
    call.detail ? paint("  " + call.detail, { dim: true }, colors) : "",
    paint(othersText(call), { color: "warning", dim: true }, colors)
  ].join("");
}
function summaryOf({ calls, nodes, edges, apiCalls }, colors, graphTotal = 0) {
  const words = [
    [compact(calls), calls === 1 ? " query" : " queries"],
    ["~" + compact(apiCalls), apiCalls === 1 ? " API call replaced" : " API calls replaced"],
    graphTotal ? [compact(nodes) + " / " + grouped(graphTotal), " nodes"] : [compact(nodes), nodes === 1 ? " node" : " nodes"],
    [compact(edges), edges === 1 ? " edge" : " edges"]
  ];
  const parts = words.flatMap(([value, unit], i) => [
    ...i > 0 ? [paint("  ·  ", { dim: true }, colors)] : [],
    paint(value, { color: "blue" }, colors),
    paint(unit, { dim: true }, colors)
  ]);
  return parts.join("") + paint("  " + MARK + " GitLab Orbit", { color: "blue" }, colors);
}
function stateDir(env = process.env) {
  const data = env["PLUGIN_DATA"];
  return data ? join(data, "sessions") : join(tmpdir(), "gitlab-orbit-codex");
}
function sessionFile(dir, session) {
  return /^[\w-]{1,128}$/.test(session) ? join(dir, session + ".jsonl") : null;
}
function readLines(file) {
  let raw = "";
  try {
    raw = readFileSync(file, "utf8");
  } catch {
    return [];
  }
  return raw.split(`
`).flatMap((line) => {
    try {
      return line ? [JSON.parse(line)] : [];
    } catch {
      return [];
    }
  });
}
function append(dir, file, lines) {
  mkdirSync(dir, { recursive: true });
  appendFileSync(file, lines.map((line) => JSON.stringify(line) + `
`).join(""));
}
function summary(dir, file, colors) {
  const lines = readLines(file);
  if (!lines.slice(lines.findLastIndex((line) => line.shown) + 1).some(isTally))
    return null;
  const tallies = lines.filter(isTally);
  const sum = (key) => s(tallies, (tally) => Number(tally[key]) || 0);
  const scopes = lines.flatMap((line) => line.scope && typeof line.total === "number" ? [{ key: line.scope, total: line.total, ...line.identity ? { identity: line.identity } : {} }] : []);
  append(dir, file, [{ shown: true }]);
  return summaryOf({ calls: sum("calls"), nodes: sum("nodes"), edges: sum("edges"), apiCalls: sum("apiCalls") }, colors, countedTotal(scopes));
}
function removeSession(dir, file) {
  rmSync(file, { force: true });
  let names = [];
  try {
    names = readdirSync(dir);
  } catch {}
  for (const name of names) {
    const path = join(dir, name);
    try {
      if (name.endsWith(".jsonl") && Date.now() - statSync(path).mtimeMs > STALE_MS)
        rmSync(path, { force: true });
    } catch {}
  }
}
function handle(input, dir, colors = colorsOn()) {
  const file = sessionFile(dir, text(input.session_id));
  if (!file)
    return null;
  const event = text(input.hook_event_name);
  if (event === "PostToolUse") {
    const known = new Set(readLines(file).flatMap((line) => line.entities ?? []));
    const call = describeOrbitCall(text(input.tool_name), input.tool_input, known);
    if (!call)
      return null;
    const tally = tallyOf(input);
    if (tally)
      append(dir, file, [tally]);
    return { systemMessage: rowOf(call, tally === null, colors) };
  }
  if (event === "Stop") {
    const message = summary(dir, file, colors);
    return message ? { systemMessage: message } : null;
  }
  if (event === "SessionEnd")
    removeSession(dir, file);
  return null;
}
function lookUp(input, dir, run) {
  const file = sessionFile(dir, text(input.session_id));
  if (!file || text(input.hook_event_name) !== "PostToolUse")
    return;
  if (describeOrbitCall(text(input.tool_name), input.tool_input)?.label !== QUERY_LABEL || !tallyOf(input))
    return;
  const asked = new Set(readLines(file).flatMap((line) => typeof line.asked === "string" ? [line.asked] : []));
  const command = text(input.tool_input?.command);
  const lookUps = scopesOf(queryOf(command)).filter((scope) => !asked.has(scope.key)).slice(0, MAX_PROJECT_TOTALS);
  const entitiesAsked = asked.has("");
  if (!entitiesAsked && lookUps.length === 0)
    lookUps.push({ key: "", flags: [] });
  if (lookUps.length === 0)
    return;
  append(dir, file, [...lookUps.map(({ key }) => ({ asked: key })), ...entitiesAsked || lookUps.some(({ key }) => !key) ? [] : [{ asked: "" }]]);
  for (const { key, flags } of lookUps) {
    let identity = null;
    for (const { kind, argv } of key ? identityLookups(flags) : []) {
      const result = run(argv);
      identity = result.exitCode === 0 ? scopeIdentity(result.stdout, kind) : null;
      if (identity)
        break;
    }
    for (const launcher of [["glab", "orbit"], ["orbit"]]) {
      const result = run([...launcher, "graph-status", ...flags, "--response-format", "raw"]);
      const graph = result.exitCode === 0 ? graphStatus(result.stdout) : null;
      if (!graph)
        continue;
      append(dir, file, [{ scope: key, ...key ? { total: graph.total } : {}, ...identity ? { identity } : {}, entities: graph.entities }]);
      break;
    }
  }
}
var runCommand = (argv) => {
  const result = spawnSync(argv[0] ?? "", argv.slice(1), { encoding: "utf8", timeout: 30000 });
  return { exitCode: result.status ?? 1, stdout: result.stdout ?? "" };
};
function main(stdin, dir = stateDir(), colors = colorsOn()) {
  try {
    const output = handle(JSON.parse(stdin), dir, colors);
    return output ? JSON.stringify(output) : "";
  } catch {
    return "";
  }
}
function lookUpMain(stdin, dir = stateDir(), run = runCommand) {
  try {
    lookUp(JSON.parse(stdin), dir, run);
  } catch {}
}

if (Number(process.versions.node.split(".")[0]) >= 20) {
  let stdin = "";
  try {
    stdin = readFileSync2(0, "utf8");
  } catch {}
  if (process.argv[2] === "lookups")
    lookUpMain(stdin);
  else
    process.stdout.write(main(stdin));
}
