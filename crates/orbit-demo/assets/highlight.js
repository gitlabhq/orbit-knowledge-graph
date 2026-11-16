"use strict";

(function () {
  const GQL_KEYWORDS = new Set(("MATCH OPTIONAL WHERE RETURN WITH ORDER BY ASC ASCENDING DESC DESCENDING LIMIT SKIP OFFSET " +
    "AND OR XOR NOT IN IS NULL TRUE FALSE AS DISTINCT CONTAINS STARTS ENDS MATCHES UNION ALL ANY CASE WHEN " +
    "THEN ELSE END EXISTS SHORTEST UNWIND CALL DEBUG").split(" "));
  const SQL_KEYWORDS = new Set(("SELECT FROM WHERE AND OR NOT IN AS ON JOIN INNER LEFT RIGHT FULL OUTER SEMI ANTI ANY ALL " +
    "ARRAY GROUP BY ORDER ASC DESC LIMIT OFFSET SETTINGS WITH UNION DISTINCT CASE WHEN THEN ELSE END IS NULL " +
    "FINAL PREWHERE HAVING USING MATERIALIZED TRUE FALSE INTERVAL").split(" "));
  const SQL_BREAKS = new Set(["FROM", "WHERE", "PREWHERE", "JOIN", "INNER", "LEFT", "GROUP", "ORDER", "LIMIT", "SETTINGS", "UNION", "HAVING"]);

  function scan(text, rules, classify) {
    const tokens = [];
    let at = 0;
    while (at < text.length) {
      let matched = null;
      for (const [cls, re] of rules) {
        re.lastIndex = at;
        const m = re.exec(text);
        if (m && m[0].length) { matched = [cls, m[0]]; break; }
      }
      if (!matched) matched = ["plain", text[at]];
      tokens.push(classify(matched, tokens, text, at));
      at += matched[1].length;
    }
    return tokens;
  }

  const common = [
    ["comment", /\/\*[\s\S]*?(\*\/|$)|\/\/[^\n]*/y],
    ["string", /'(?:[^'\\]|\\.)*'?|"(?:[^"\\]|\\.)*"?/y],
    ["number", /\d+(?:\.\d+)?/y],
    ["space", /\s+/y],
    ["word", /[A-Za-z_][A-Za-z0-9_]*/y],
    ["op", /<-|->|<>|<=|>=|=~|[=<>+*\/%-]/y],
    ["punct", /[()[\]{},.:;|$]/y],
  ];

  function gql(text) {
    const brackets = [];
    return scan(text, common, ([cls, value], tokens, all, at) => {
      if (cls === "punct" && "([{".includes(value)) brackets.push(value);
      if (cls === "punct" && ")]}".includes(value)) brackets.pop();
      if (cls !== "word") return [cls, value];
      const prev = significant(tokens);
      const next = all.slice(at + value.length).match(/^\s*(.)/)?.[1];
      if (GQL_KEYWORDS.has(value.toUpperCase())) return ["keyword", value];
      if (prev && prev[1] === ":" && brackets[brackets.length - 1] === "[") return ["reltype", value];
      if (prev && prev[1] === ":" && brackets[brackets.length - 1] === "(") return ["label", value];
      if (prev && prev[1] === ".") return ["property", value];
      if (next === ":" && brackets[brackets.length - 1] === "{") return ["property", value];
      if (next === "(") return ["function", value];
      return ["variable", value];
    });
  }

  function sql(text) {
    let depth = 0;
    const tokens = [];
    for (const token of scan(text, [["param", /\{[A-Za-z_][A-Za-z0-9_]*:[^}]*\}/y], ...common], ([cls, value], seen, all, at) => {
      if (cls === "punct" && value === "(") depth++;
      if (cls === "punct" && value === ")") depth--;
      if (cls !== "word") return [cls, value];
      const next = all.slice(at + value.length).match(/^\s*(.)/)?.[1];
      if (SQL_KEYWORDS.has(value.toUpperCase())) return ["keyword", value, depth];
      if (significant(seen)?.[1] === ".") return ["property", value];
      if (next === "(") return ["function", value];
      return ["variable", value];
    })) {
      const breaks = token[0] === "keyword" && SQL_BREAKS.has(token[1].toUpperCase()) && !joinsPrevious(tokens);
      if (breaks && tokens.length) {
        while (tokens.length && tokens[tokens.length - 1][0] === "space") tokens.pop();
        tokens.push(["space", "\n" + "  ".repeat(Math.max(0, token[2]))]);
      }
      tokens.push([token[0], token[1]]);
    }
    return tokens;
  }

  const PLAN_WORDS = new Set("asc desc matches and or not in is null true false".split(" "));
  const planRules = [
    ["reltype", /<?-\[[A-Za-z_]*\]->?/y],
    ["string", /'(?:[^'\\]|\\.)*'?|"(?:[^"\\]|\\.)*"?/y],
    ["number", /\d+(?:\.\d+)?/y],
    ["space", /\s+/y],
    ["word", /[A-Za-z_][A-Za-z0-9_]*/y],
    ["op", /<-|->/y],
    ["compare", /<>|!=|<=|>=|[=<>]/y],
    ["punct", /[()[\]{},.:;|$!*+\/%-]/y],
  ];

  function plan(text) {
    let depth = 0;
    return scan(text, planRules, ([cls, value], tokens, all, at) => {
      if (cls === "punct" && value === "{") depth++;
      if (cls === "punct" && value === "}") depth--;
      if (cls !== "word") return [cls, value];
      const prev = significant(tokens);
      const next = all.slice(at + value.length).match(/^\s*(.)/)?.[1];
      if (!prev) return ["keyword", value];
      if (PLAN_WORDS.has(value)) return ["keyword", value];
      if (prev[1] === ".") return ["property", value];
      if (/^[A-Z]/.test(value)) return ["label", value];
      if (depth > 0) return ["property", value];
      if (next === "(") return ["function", value];
      return ["variable", value];
    });
  }

  function scalar(value, key, names) {
    if (names.entities.has(value)) return "label";
    if (names.relationships.has(value)) return "reltype";
    if (/^-?\d+(?:\.\d+)?$/.test(value)) return "number";
    if (key) return "property";
    return /^(?:true|false|null|~)$/.test(value) ? "keyword" : "string";
  }

  // Flow collections and `|` or `>` block scalars span lines, so their state carries over.
  function yaml(text, names) {
    let depth = 0, block = -1;
    return text.split("\n").map((line) => {
      const indent = line.search(/\S|$/);
      if (block >= 0 && (indent > block || indent === line.length)) return [["string", line]];
      block = -1;
      const tokens = [];
      let at = 0;
      while (at < line.length) {
        const rest = line.slice(at);
        const keyed = (end) => /^\s*:(\s|$)/.test(line.slice(end));
        let m;
        if ((m = /^\s+/.exec(rest))) tokens.push(["space", m[0]]);
        else if (rest[0] === "#" && (at === 0 || /\s/.test(line[at - 1]))) tokens.push(["comment", rest]);
        else if ((m = /^'(?:[^']|'')*'?|^"(?:[^"\\]|\\.)*"?/.exec(rest))) tokens.push([keyed(at + m[0].length) ? "property" : "string", m[0]]);
        else if ((m = /^[{[]/.exec(rest))) { depth++; tokens.push(["punct", m[0]]); }
        else if ((m = /^[}\]]/.exec(rest))) { depth = Math.max(0, depth - 1); tokens.push(["punct", m[0]]); }
        else if ((m = /^(?:->|<-)/.exec(rest))) tokens.push(["op", m[0]]);
        else if ((m = /^(?::(?=\s|$)|-(?=\s|$)|,)/.exec(rest))) tokens.push(["punct", m[0]]);
        else if ((m = /^[|>][-+]?\s*$/.exec(rest))) { block = indent; tokens.push(["punct", m[0]]); }
        else {
          let end = at;
          while (end < line.length && !/^(?::(\s|$)|\s+(#|->|<-))/.test(line.slice(end)) && !(depth && /[,{}[\]]/.test(line[end]))) end++;
          const value = line.slice(at, end).trimEnd() || line[at];
          m = [value];
          tokens.push([scalar(value, keyed(at + value.length), names), value]);
        }
        at += m[0].length;
      }
      return tokens;
    });
  }

  function significant(tokens) {
    for (let i = tokens.length - 1; i >= 0; i--) if (tokens[i][0] !== "space") return tokens[i];
    return null;
  }

  function joinsPrevious(tokens) {
    const prev = significant(tokens);
    return prev && prev[0] === "keyword" && ["INNER", "LEFT", "SEMI", "ANTI", "ANY", "ALL", "ARRAY", "FULL", "OUTER"].includes(prev[1].toUpperCase());
  }

  function render(target, tokens) {
    const nodes = tokens.map(([cls, value]) => {
      if (cls === "space" || cls === "plain") return document.createTextNode(value);
      const span = document.createElement("span");
      span.className = `tok-${cls}`;
      span.textContent = value;
      return span;
    });
    target.replaceChildren(...nodes);
  }

  window.Highlight = { gql, sql, plan, yaml, render };
})();
