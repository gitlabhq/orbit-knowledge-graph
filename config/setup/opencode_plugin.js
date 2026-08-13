// No backticks or $( ) here: the reminder is echoed inside bash double quotes, where they
// would substitute and execute the command we only suggest.
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
