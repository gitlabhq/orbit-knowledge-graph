const LAUNCHERS: [string, string[]][] = [["orbit", []], ["glab", ["orbit"]]];

export default function (pi: any) {
  pi.on("before_agent_start", async (event: any) => {
    for (const [command, prefix] of LAUNCHERS) {
      const run = await pi.exec(command, [...prefix, "hook-guard", "instructions"], { timeout: 9000 }).catch(() => null);
      if (!run) continue;
      const text = run.code === 0 ? String(run.stdout).trim() : "";
      if (!text || String(event.systemPrompt).includes(text)) return;
      return { systemPrompt: event.systemPrompt + "\n\n" + text };
    }
  });
}
