declare module '@earendil-works/pi-tui' {
  export interface Component {
    render(width: number): string[]
    invalidate(): void
  }
  export class Container implements Component {
    render(width: number): string[]
    invalidate(): void
  }
  export class Text implements Component {
    constructor(text?: string, paddingX?: number, paddingY?: number)
    setText(text: string): void
    render(width: number): string[]
    invalidate(): void
  }
}

declare module 'pi' {
  import type { Component } from '@earendil-works/pi-tui'

  export type Color = 'accent' | 'muted' | 'dim' | 'success' | 'warning' | 'error' | 'toolTitle'
  export interface Theme {
    fg(color: Color, text: string): string
    bold(text: string): string
  }
  export interface RenderContext {
    args: unknown
    expanded: boolean
    isError: boolean
    isPartial: boolean
    durationMs?: number
    lastComponent?: Component
  }
  export interface ToolRenderers {
    renderCall?(args: unknown, theme: Theme, context: RenderContext): Component
    renderResult?(result: unknown, options: { expanded: boolean; isPartial: boolean }, theme: Theme, context: RenderContext): Component
  }
  export interface ToolResultEvent {
    toolName: string
    input: unknown
    content?: readonly { type: string; text?: string }[]
    isError?: boolean
  }
  export interface ExtensionContext {
    hasUI: boolean
    ui: { theme: Theme; setStatus(key: string, text: string | undefined): void }
  }
  export interface ExtensionApi {
    on(event: 'session_start', handler: (event: unknown, ctx: ExtensionContext) => void): void
    on(event: 'tool_result', handler: (event: ToolResultEvent, ctx: ExtensionContext) => void): void
    registerToolRenderer(resolver: (toolName: string, next: () => ToolRenderers | undefined) => ToolRenderers | undefined): void
  }
}
