import { existsSync } from "fs";
import { delimiter, join } from "path";

const LAUNCHERS: [string, string[]][] = [["orbit", []], ["glab", ["orbit"]]];

function onPath(command: string): boolean {
  const extensions = process.platform === "win32" ? (process.env.PATHEXT ?? ".EXE").split(";") : [""];
  return (process.env.PATH ?? "")
    .split(delimiter)
    .some((dir) => dir && extensions.some((extension) => existsSync(join(dir, command + extension))));
}

async function loadInstructions(pi: any): Promise<string> {
  const launcher = LAUNCHERS.find(([command]) => onPath(command));
  if (!launcher) return "";
  const [command, prefix] = launcher;
  const run = await pi.exec(command, [...prefix, "hook-guard", "instructions"], { timeout: 9000 }).catch(() => null);
  return run && run.code === 0 ? String(run.stdout).trim() : "";
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
