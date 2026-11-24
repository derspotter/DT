import { normalizeOpenAlexId, normalizeDoi } from '../src/identifiers.js'

test.each([
  ['W12', 'W12'],
  ['w12', 'W12'],
  ['https://openalex.org/W12', 'W12'],
  ['https://api.openalex.org/works/W12', 'W12'],
  ['A123', null],
  ['', null],
  [null, null],
])('normalizeOpenAlexId(%p) is %p', (input, expected) => expect(normalizeOpenAlexId(input)).toBe(expected))

test.each([
  ['https://doi.org/10.1000/abc', '10.1000/ABC'],
  ['  10.1000/x ', '10.1000/X'],
  ['abc', 'ABC'],
  ['   ', null],
  [0, null],
])('normalizeDoi(%p) is %p', (input, expected) => expect(normalizeDoi(input)).toBe(expected))
