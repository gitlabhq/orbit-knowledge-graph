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

  window.Highlight = { gql, sql, render };
})();
