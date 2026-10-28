# @orbit/opencode-plugin

The OpenCode plugin that `orbit setup` installs. Before each Orbit call it shows a toast, such as `◎ Searching code  login`, with the labels from [`@orbit/agent-plugin-core`](../agent-plugin-core). After the first shell call in a session with an indexed graph, it adds the reminder from `config/setup/setup.yaml` to the output.

`bun run build` bundles the plugin with the core and the labels into `config/setup/opencode_plugin.js`, which setup writes to `.opencode/plugins/orbit.js` with the reminder filled in. CI fails when that file differs from a fresh build.

```shell
bun test
bun run typecheck
bun run build
```
