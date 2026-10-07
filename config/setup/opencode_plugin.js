{{hook_client}}
import { resolve } from "node:path";

export const OrbitPlugin = async ({ directory }) => {
  const names = { bash: "Bash", shell: "Bash", grep: "Grep" };
  const notes = new Map();
  return {
    "tool.execute.before": async (input, output) => {
      const args = output.args ?? {};
      const cwd = args.workdir ? resolve(directory, args.workdir) : directory;
      const note = await nudge(names[input.tool], args, cwd);
      if (note) notes.set(input.callID, note);
    },
    "tool.execute.after": async (input, output) => {
      const note = notes.get(input.callID);
      if (!note) return;
      notes.delete(input.callID);
      output.output = (output.output ?? "") + "\n\n" + note;
    },
  };
};
