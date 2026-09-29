{{hook_client}}

export default function (pi: any) {
  const names = { bash: "Bash", read: "Read", grep: "Grep", find: "Glob" };
  const notes = new Map<string, string>();
  pi.on("tool_call", async (event: any, ctx: any) => {
    try {
      const input = event.input ?? {};
      const decision = await guard(names[event.toolName], { ...input, file_path: input.path },
        ctx.sessionManager.getSessionId(), ctx.cwd);
      if (decision?.permissionDecision === "deny") {
        return { block: true, reason: decision.permissionDecisionReason };
      }
      if (decision?.additionalContext) notes.set(event.toolCallId, decision.additionalContext);
    } catch {
      return;
    }
  });
  pi.on("tool_result", async (event: any) => {
    const note = notes.get(event.toolCallId);
    if (!note) return;
    notes.delete(event.toolCallId);
    return { content: [...event.content, { type: "text", text: note }] };
  });
}
