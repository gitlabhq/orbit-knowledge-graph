// Keep this file free of backticks and dollar-paren substitution: the
// reminder is prepended as an echo inside double quotes, where bash would
// substitute them, corrupting output and executing the command we only
// suggest. A test asserts this over the whole file.
export const OrbitPlugin = async () => {
  let reminded = false;
  return {
    "tool.execute.before": async (input, output) => {
      if (reminded) return;
      if (input.tool === "bash") {
        output.args.command =
          'echo "[orbit] remote Orbit graph available: for code-structure and GitLab-entity questions run glab orbit remote query (scoped results, cheaper than grep); glab orbit remote schema lists entities." ; ' +
          output.args.command;
        reminded = true;
      }
    },
  };
};
