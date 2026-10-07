{{hook_client}}

export default function (pi: any) {
  const names = { bash: "Bash", read: "Read", grep: "Grep", find: "Glob" };
  const notes = new Map<string, string>();
  pi.on("tool_call", async (event: any) => {
    const input = event.input ?? {};
    const note = await nudge(names[event.toolName], { ...input, file_path: input.path });
    if (note) notes.set(event.toolCallId, note);
  });
  pi.on("tool_result", async (event: any) => {
    const note = notes.get(event.toolCallId);
    if (!note) return;
    notes.delete(event.toolCallId);
    return { content: [...event.content, { type: "text", text: note }] };
  });
}
