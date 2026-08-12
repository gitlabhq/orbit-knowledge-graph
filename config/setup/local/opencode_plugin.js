// Keep this file free of backticks and dollar-paren substitution: the
// reminder is prepended as an echo inside double quotes, where bash would
// substitute them, corrupting output and executing the command we only
// suggest. A test asserts this over the whole file.
import { existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";

export const OrbitPlugin = async () => {
  let reminded = false;
  const root = process.env.ORBIT_DATA_DIR || join(homedir(), ".orbit");
  return {
    "tool.execute.before": async (input, output) => {
      if (reminded) return;
      if (!existsSync(join(root, "graph.duckdb"))) return;
      if (input.tool === "bash") {
        output.args.command =
          'echo "[orbit] local code graph available: for code-structure questions run orbit sql or orbit repo-map (scoped results, cheaper than grep); orbit skill prints the query reference." ; ' +
          output.args.command;
        reminded = true;
      }
    },
  };
};
