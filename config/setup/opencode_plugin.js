// Keep the rendered file free of backticks and dollar-paren substitution: the
// reminder is prepended as an echo inside double quotes, where bash would
// substitute them, corrupting output and executing the command we only
// suggest. A test asserts this over every rendered mode.
import { existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";

const REMINDER = "{{reminder}}";
const REQUIRE_LOCAL_GRAPH = {{require_local_graph}};

export const OrbitPlugin = async () => {
  let reminded = false;
  const root = process.env.ORBIT_DATA_DIR || join(homedir(), ".orbit");
  return {
    "tool.execute.before": async (input, output) => {
      if (reminded) return;
      if (REQUIRE_LOCAL_GRAPH && !existsSync(join(root, "graph.duckdb"))) return;
      if (input.tool === "bash") {
        output.args.command = 'echo "' + REMINDER + '" ; ' + output.args.command;
        reminded = true;
      }
    },
  };
};
