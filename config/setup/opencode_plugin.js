{{hook_client}}

export const OrbitPlugin = async () => {
  const names = { bash: "Bash", shell: "Bash", read: "Read", grep: "Grep", glob: "Glob" };
  const notes = new Map();
  return {
    "tool.execute.before": async (input, output) => {
      const args = output.args ?? {};
      const note = await nudge(names[input.tool], { ...args, file_path: args.filePath });
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
