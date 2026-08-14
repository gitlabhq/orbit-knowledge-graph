import { existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";

const REMINDER = "{{reminder}}";
const REQUIRE_LOCAL_GRAPH = {{require_local_graph}};

export const OrbitPlugin = async () => {
  let reminded = false;
  const root = process.env.ORBIT_DATA_DIR || join(homedir(), ".orbit");
  return {
    "tool.execute.after": async (input, output) => {
      if (reminded) return;
      if (input.tool !== "bash") return;
      if (REQUIRE_LOCAL_GRAPH && !existsSync(join(root, "graph.duckdb"))) return;
      output.output = output.output + "\n\n" + REMINDER;
      reminded = true;
    },
  };
};
