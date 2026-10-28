import { createHighlighterCoreSync } from 'shiki/core'
import { createJavaScriptRegexEngine } from 'shiki/engine/javascript'
import cypher from 'shiki/langs/cypher.mjs'

const highlighter = createHighlighterCoreSync({
  themes: [{ name: 'orbit', type: 'dark', settings: [] }],
  langs: [cypher],
  engine: createJavaScriptRegexEngine(),
})

export function cypherPieces(code: string): { text: string; scope: string }[][] {
  return highlighter
    .codeToTokensBase(code, { lang: 'cypher', theme: 'orbit', includeExplanation: true })
    .map((line) =>
      line.flatMap((token) =>
        (token.explanation ?? []).map((piece) => ({ text: piece.content, scope: piece.scopes.at(-1)?.scopeName ?? '' })),
      ),
    )
}
