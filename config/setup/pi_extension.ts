const LAUNCHERS: [string, string[]][] = [["glab", ["orbit"]], ["orbit", []]];

async function loadInstructions(pi: any): Promise<string> {
  for (const [command, prefix] of LAUNCHERS) {
    const run = await pi.exec(command, [...prefix, "hook-guard", "instructions"], { timeout: 9000 }).catch(() => null);
    const text = run && run.code === 0 ? String(run.stdout).trim() : "";
    if (text) return text;
  }
  return "";
}

export default function (pi: any) {
  let instructions: Promise<string> | undefined;
  pi.on("before_agent_start", async (event: any) => {
    instructions ??= loadInstructions(pi);
    const text = await instructions;
    if (!text || String(event.systemPrompt).includes(text)) return;
    return { systemPrompt: event.systemPrompt + "\n\n" + text };
  });
}
