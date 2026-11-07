const LAUNCHERS: [string, string[]][] = [["glab", ["orbit"]], ["orbit", []]];

async function loadInstructions(pi: any): Promise<string> {
  const texts = await Promise.all(
    LAUNCHERS.map(async ([command, prefix]) => {
      const run = await pi.exec(command, [...prefix, "hook-guard", "instructions"], { timeout: 9000 }).catch(() => null);
      return run && run.code === 0 ? String(run.stdout).trim() : "";
    }),
  );
  return texts.find(Boolean) ?? "";
}

export default function (pi: any) {
  let instructions: Promise<string> | undefined;
  pi.on("before_agent_start", async (event: any) => {
    instructions ??= loadInstructions(pi);
    const text = await instructions;
    if (!text) {
      instructions = undefined;
      return;
    }
    if (String(event.systemPrompt).includes(text)) return;
    return { systemPrompt: event.systemPrompt + "\n\n" + text };
  });
}
