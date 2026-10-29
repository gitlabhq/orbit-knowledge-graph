// The parts of the OpenCode 2 plugin API the OpenCode plugin uses. OpenCode resolves these
// modules at runtime, so they are not dependencies of this package.
// See https://opencode.ai/v2/docs/build/plugins.

declare module '@opencode/plugin/rpc' {
  export type RpcContract<Events> = { readonly id: string; readonly __events?: Events }
  export const Rpc: {
    define<Events>(definition: {
      id: string
      methods: Record<string, never>
      events: { [Name in keyof Events]: { schema: Record<string, unknown> } }
    }): RpcContract<Events>
  }
}

declare module '@opencode/plugin' {
  import type { RpcContract } from '@opencode/plugin/rpc'

  /** The `execute.before` event, as of OpenCode 2.2.1. */
  export type ToolExecuteBefore = {
    tool: string
    id: string
    sessionID: string
    messageID: string
    agent: string
    input: unknown
  }

  export type ToolResult = {
    output?: string
    content?: readonly { type: string; text?: string }[]
    metadata?: Record<string, unknown>
  }

  /** The `execute.after` event, as of OpenCode 2.2.1. */
  export type ToolExecuteAfter = ToolExecuteBefore &
    ({ status: 'completed'; result: ToolResult } | { status: 'error'; error: unknown })

  export type ToolHooks = { 'execute.before': ToolExecuteBefore; 'execute.after': ToolExecuteAfter }

  export type PluginContext = {
    tool: {
      hook<Name extends keyof ToolHooks>(
        name: Name,
        callback: (event: ToolHooks[Name]) => Promise<void> | void,
      ): Promise<unknown>
    }
    rpc: {
      register<Events>(
        contract: RpcContract<Events>,
        methods: Record<string, never>,
      ): Promise<{ events: { emit<Name extends keyof Events>(name: Name, data: Events[Name]): Promise<void> } }>
    }
  }

  export type PluginDefinition = { id: string; setup(ctx: PluginContext): Promise<void> | void }

  export const Plugin: { define<Definition extends PluginDefinition>(definition: Definition): Definition }
}

declare module '@opencode/plugin/tui' {
  import type { RpcContract } from '@opencode/plugin/rpc'

  type Color = string | object

  export type TuiContext = {
    theme: { text: { base: Color; muted: Color; feedback: { warning: { base: Color }; error?: { base: Color } } } }
    storage: {
      memory<State extends object>(
        key: string,
        options: { initial: State },
      ): [State, (update: (draft: State) => void) => void]
    }
    data: { on(type: string, callback: (event: unknown) => void): () => void }
    client: {
      rpc<Events>(contract: RpcContract<Events>): {
        events: { on<Name extends keyof Events>(name: Name, callback: (event: { data: Events[Name] }) => void): () => void }
      }
    }
    ui: {
      slot(options: { append: string; render: (props: { sessionID?: string }) => unknown }): (() => void) | void
    }
  }

  export const Plugin: {
    define(definition: { id: string; setup(context: TuiContext): (() => void) | void }): unknown
  }
}

declare module '@opentui/solid/jsx-runtime' {
  export namespace JSX {
    type Element = unknown
    interface ElementChildrenAttribute {
      children: unknown
    }
    interface IntrinsicElements {
      box: { flexDirection?: 'row' | 'column'; paddingLeft?: number; marginBottom?: number; children?: unknown }
      text: { fg?: string | object; children?: unknown }
    }
  }
}
