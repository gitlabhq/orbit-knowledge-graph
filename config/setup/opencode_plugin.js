import { execFile } from "child_process";
import { existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";

// One default export carries both plugin APIs and imports nothing from OpenCode, so the same file
// loads in both: OpenCode 2 calls setup(), and OpenCode 1 (1.3.4 and later) calls server().

const REMINDER = "{{reminder}}";
const SHELL_TOOLS = ["bash", "shell"];

function hasGraph() {
  const root = process.env.ORBIT_DATA_DIR || join(homedir(), ".gitlab", "orbit");
  return existsSync(join(root, "local", "manifest.duckdb"));
}

const LAUNCHERS = [["glab", ["orbit"]], ["orbit", []]];
const RETRY_AFTER_MS = 60000;

function run(command, args) {
  return new Promise((resolve) => {
    execFile(command, args, { timeout: 9000 }, (error, stdout) => resolve(error ? "" : String(stdout).trim()));
  });
}

async function loadInstructions() {
  const texts = await Promise.all(LAUNCHERS.map(([command, prefix]) => run(command, [...prefix, "hook-guard", "instructions"])));
  return texts.find(Boolean) ?? "";
}

function withReminder(result) {
  const suffix = "\n\n" + REMINDER;
  if (Array.isArray(result.content)) return { ...result, content: [...result.content, { type: "text", text: REMINDER }] };
  if (typeof result.content === "string") return { ...result, content: result.content + suffix };
  if (typeof result.output === "string") return { ...result, output: result.output + suffix };
  const kept = result.output === undefined ? [] : [{ type: "text", text: JSON.stringify(result.output) }];
  return { ...result, content: [...kept, { type: "text", text: REMINDER }] };
}

export default {
  id: "orbit",
  async setup(ctx) {
    let reminded = false;
    await ctx.tool.hook("execute.after", (event) => {
      if (reminded || event.status !== "completed") return;
      if (!SHELL_TOOLS.includes(event.tool) || !hasGraph()) return;
      event.result = withReminder(event.result ?? {});
      reminded = true;
    });
  },
  async server() {
    let reminded = false;
    let instructions = loadInstructions();
    let loadedAt = Date.now();
    return {
      "experimental.chat.system.transform": async (_input, output) => {
        const text = await instructions;
        if (!text && Date.now() - loadedAt > RETRY_AFTER_MS) {
          instructions = loadInstructions();
          loadedAt = Date.now();
        }
        if (text && !output.system.includes(text)) output.system.push(text);
      },
      "tool.execute.after": async (input, output) => {
        if (reminded || !SHELL_TOOLS.includes(input.tool) || !hasGraph()) return;
        output.output = output.output + "\n\n" + REMINDER;
        reminded = true;
      },
    };
  },
};
