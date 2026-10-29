import { expect, test } from 'claude-code/testing'

import { fade } from '../hooks/theme.ts'

test('the fade runs from the light stop to the dark stop', () => {
  expect(fade(0)).toBe('#b5dedc')
  expect(fade(0.5)).toBe('#949dcb')
  expect(fade(1)).toBe('#6851ac')
})
