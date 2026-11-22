// D10/D11 (BE-10, FE-59): the log tail reads backwards in chunks, stops at its
// window, is pinned against rotation, and trusts a corpus tag only in a
// recognized writer's fixed position. Exports are reached through the module
// namespace, so a missing export fails only the tests that use it.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import request from 'supertest'
import { jest } from '@jest/globals'
import * as appModule from '../src/app.js'

const BASE_NAME = 'backend-pipeline-v2.log'
const CHUNK = 64 * 1024

// Today's semantics, written out: every file oldest first, split, trimmed,
// blanks dropped, filtered, then the window counted from the newest line.
function reference(files, { limit, cursor, accept = () => true }) {
  const all = files
    .flatMap((text) => text.split(/\r?\n/).map((l) => l.trimEnd()).filter(Boolean))
    .filter(accept)
  const safe = Math.min(cursor, all.length)
  const end = all.length - safe
  const start = Math.max(0, end - limit)
  return { entries: all.slice(start, end), hasMore: start > 0, nextCursor: safe + (end - start) }
}

// Rotates as index.js does: .4 -> .5 … .1 -> .2, base -> .1, then a new base.
function rotate(base, newLines, maxFiles = 5) {
  for (let i = maxFiles - 1; i >= 1; i -= 1) {
    if (fs.existsSync(`${base}.${i}`)) fs.renameSync(`${base}.${i}`, `${base}.${i + 1}`)
  }
  fs.renameSync(base, `${base}.1`)
  fs.writeFileSync(base, newLines.join('\n') + '\n')
}

// The files of a window, oldest first, as text ('' for a missing file).
function filesOldestFirst(base, maxFiles = 5) {
  const out = []
  for (let i = maxFiles; i >= 1; i -= 1) {
    out.push(fs.existsSync(`${base}.${i}`) ? fs.readFileSync(`${base}.${i}`, 'utf8') : '')
  }
  out.push(fs.existsSync(base) ? fs.readFileSync(base, 'utf8') : '')
  return out
}

const realOpen = fs.promises.open.bind(fs.promises)

// Spies on fs.promises.open; every returned handle's read() adds its bytesRead
// to `bytes.total`. `onOpen(filePath, callIndex)` runs right after the real open.
function countReads(onOpen = () => {}) {
  const bytes = { total: 0 }
  let calls = 0
  const open = jest.spyOn(fs.promises, 'open').mockImplementation(async (...args) => {
    const handle = await realOpen(...args)
    const index = calls
    calls += 1
    const realRead = handle.read.bind(handle)
    handle.read = async (...readArgs) => {
      const result = await realRead(...readArgs)
      bytes.total += result.bytesRead
      return result
    }
    await onOpen(String(args[0]), index)
    return handle
  })
  return { open, bytes }
}

let tmpRoot = ''
beforeAll(() => {
  tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-logs-tail-reader-'))
})
afterAll(() => {
  fs.rmSync(tmpRoot, { recursive: true, force: true })
})
afterEach(() => {
  jest.restoreAllMocks()
  delete process.env.RAG_FEEDER_STUB
  delete process.env.RAG_FEEDER_LOG_DIR
})

const freshDir = () => fs.mkdtempSync(path.join(tmpRoot, 'case-'))

// ~150 KiB of numbered lines with multibyte text of varying length, CRLF
// endings in some files, blank and whitespace-only lines.
function makeFileText(tag, { crlf = false, trailingNewline = true, targetBytes = 150 * 1024 } = {}) {
  const eol = crlf ? '\r\n' : '\n'
  const parts = []
  let bytes = 0
  for (let i = 0; bytes < targetBytes; i += 1) {
    let line
    if (i % 37 === 5) line = ''
    else if (i % 41 === 7) line = '   \t '
    else line = `[2026-01-01T00:00:00.000Z] [${tag}] n=${i} ${i % 2 === 0 ? 'corpus=12' : 'corpus=7'} ${'ü€😀'.repeat(i % 9)}${'x'.repeat(i % 23)}`
    parts.push(line)
    bytes += Buffer.byteLength(line + eol)
  }
  return parts.join(eol) + (trailingNewline ? eol : '')
}

describe('readLogWindow', () => {
  test('matches the full-read reference across files, chunk boundaries, CRLF and multibyte text', async () => {
    const dir = freshDir()
    const base = path.join(dir, BASE_NAME)
    // Oldest first: .5, .4, (.3 missing), .2, .1, base.
    fs.writeFileSync(`${base}.5`, makeFileText('f5'))
    fs.writeFileSync(`${base}.4`, makeFileText('f4', { crlf: true }))
    fs.writeFileSync(`${base}.2`, makeFileText('f2'))
    fs.writeFileSync(`${base}.1`, makeFileText('f1', { crlf: true }))
    fs.writeFileSync(base, makeFileText('f0', { trailingNewline: false }))
    // Some multibyte character straddles a backward chunk boundary.
    const straddles = [base, `${base}.1`, `${base}.2`, `${base}.4`, `${base}.5`].some((file) => {
      const buf = fs.readFileSync(file)
      for (let pos = buf.length - CHUNK; pos > 0; pos -= CHUNK) {
        if ((buf[pos] & 0xc0) === 0x80) return true
      }
      return false
    })
    expect(straddles).toBe(true)
    const files = filesOldestFirst(base)
    for (const accept of [() => true, (l) => /\bcorpus=12\b/.test(l)]) {
      for (const cursor of [0, 1, 199, 1000]) {
        for (const limit of [1, 200, 2000]) {
          const got = await appModule.readLogWindow(base, { limit, cursor, maxFiles: 5, accept })
          expect(got).toMatchObject(reference(files, { limit, cursor, accept }))
          expect(got.cursor).toBe(cursor)
        }
      }
    }
  })

  test('a cursor past the end answers nothing and echoes the cursor', async () => {
    const dir = freshDir()
    const base = path.join(dir, BASE_NAME)
    fs.writeFileSync(base, 'a\nb\nc\n')
    fs.writeFileSync(`${base}.1`, 'old\n')
    const got = await appModule.readLogWindow(base, { limit: 200, cursor: 1e6 })
    expect(got).toEqual({ entries: [], hasMore: false, cursor: 1e6, nextCursor: 1e6 })
  })

  test('a default tail reads one chunk of the newest file when it holds enough lines', async () => {
    const dir = freshDir()
    const base = path.join(dir, BASE_NAME)
    fs.writeFileSync(base, makeFileText('big', { targetBytes: 1024 * 1024 }))
    fs.writeFileSync(`${base}.1`, makeFileText('older'))
    const { open, bytes } = countReads()
    const got = await appModule.readLogWindow(base, { limit: 200, cursor: 0 })
    expect(got.entries).toHaveLength(200)
    expect(got.hasMore).toBe(true)
    expect(open.mock.calls.map(([p]) => path.basename(String(p)))).toEqual([BASE_NAME])
    expect(bytes.total).toBeLessThanOrEqual(64 * 1024)
  })

  test('a rotation between two opens is read once, without duplicates or a skipped cursor', async () => {
    const dir = freshDir()
    const base = path.join(dir, BASE_NAME)
    const numbered = (from, n) => Array.from({ length: n }, (_, i) => `line ${from + i}`)
    fs.writeFileSync(`${base}.2`, numbered(0, 200).join('\n') + '\n')
    fs.writeFileSync(`${base}.1`, numbered(200, 200).join('\n') + '\n')
    fs.writeFileSync(base, numbered(400, 100).join('\n') + '\n')
    const { open } = countReads((p, index) => {
      if (index === 0) rotate(base, numbered(500, 2))
    })
    const got = await appModule.readLogWindow(base, { limit: 300, cursor: 0 })
    const after = filesOldestFirst(base)
    expect(got).toMatchObject(reference(after, { limit: 300, cursor: 0 }))
    expect(new Set(got.entries).size).toBe(got.entries.length)
    expect(got.entries.at(-1)).toBe('line 501')
    const baseOpens = open.mock.calls.filter(([p]) => String(p) === base)
    expect(baseOpens.length).toBeGreaterThanOrEqual(2)
  })

  test('a second rotation during the retry throws LOG_ROTATED, and the route answers 503', async () => {
    const dir = freshDir()
    const base = path.join(dir, BASE_NAME)
    let counter = 0
    const twoNew = () => [`new ${counter++}`, `new ${counter++}`]
    fs.writeFileSync(`${base}.1`, 'old a\nold b\n')
    fs.writeFileSync(base, 'cur a\ncur b\n')
    countReads((p) => {
      if (p === base) rotate(base, twoNew())
    })
    await expect(appModule.readLogWindow(base, { limit: 300, cursor: 0 })).rejects.toMatchObject({ code: 'LOG_ROTATED' })

    process.env.RAG_FEEDER_STUB = '1'
    process.env.RAG_FEEDER_LOG_DIR = dir
    const app = appModule.createApp({ broadcast: () => {} })
    const consoleError = jest.spyOn(console, 'error').mockImplementation(() => {})
    const res = await request(app).get('/api/logs/tail?type=pipeline')
    expect(res.status).toBe(503)
    expect(res.body).toEqual({ error: 'Log rotated during the read; retry' })
    consoleError.mockRestore()
  })

  test('a rotated file deleted mid-read restarts once and then counts as empty', async () => {
    const dir = freshDir()
    const base = path.join(dir, BASE_NAME)
    const seed = () => {
      fs.writeFileSync(`${base}.3`, 'three a\nthree b\n')
      fs.writeFileSync(`${base}.2`, 'two a\ntwo b\n')
      fs.writeFileSync(`${base}.1`, 'one a\none b\n')
      fs.writeFileSync(base, 'zero a\nzero b\n')
    }
    seed()
    let deleted = false
    const onOpen = () => {
      if (!deleted) {
        deleted = true
        fs.rmSync(`${base}.2`)
      }
    }
    const first = countReads(onOpen)
    const expected = ['three a', 'three b', 'one a', 'one b', 'zero a', 'zero b']
    const got = await appModule.readLogWindow(base, { limit: 2000, cursor: 0 })
    expect(got).toEqual({ entries: expected, hasMore: false, cursor: 0, nextCursor: 6 })
    // The retry ran: the newest file was opened again after the restart.
    expect(first.open.mock.calls.filter(([p]) => String(p) === base)).toHaveLength(2)

    jest.restoreAllMocks()
    seed()
    deleted = false
    const second = countReads(onOpen)
    process.env.RAG_FEEDER_STUB = '1'
    process.env.RAG_FEEDER_LOG_DIR = dir
    const app = appModule.createApp({ broadcast: () => {} })
    const res = await request(app).get('/api/logs/tail?type=pipeline&lines=2000')
    expect(res.status).toBe(200)
    expect(res.body.entries).toEqual(expected)
    expect(res.body.has_more).toBe(false)
    expect(second.open.mock.calls.filter(([p]) => String(p) === base)).toHaveLength(2)
  })

  test('short reads keep records separate', async () => {
    // Every read returns at most `cap` bytes, so record boundaries fall inside
    // and between short reads (and, for the big files, across 64 KiB chunks).
    let cap = 0
    jest.spyOn(fs.promises, 'open').mockImplementation(async (...args) => {
      const handle = await realOpen(...args)
      const realRead = handle.read.bind(handle)
      handle.read = (buffer, offset, length, position) => realRead(buffer, offset, Math.min(cap, length), position)
      return handle
    })
    for (const [size, readCap] of [[150 * 1024, 4093], [2 * 1024, 3]]) {
      cap = readCap
      const dir = freshDir()
      const base = path.join(dir, BASE_NAME)
      fs.writeFileSync(`${base}.1`, makeFileText('s1', { targetBytes: size }))
      fs.writeFileSync(base, makeFileText('s0', { crlf: true, targetBytes: size }))
      const files = filesOldestFirst(base)
      for (const [limit, cursor] of [[200, 0], [2000, 15], [5000, 0]]) {
        const got = await appModule.readLogWindow(base, { limit, cursor })
        expect(got).toMatchObject(reference(files, { limit, cursor }))
      }
    }
  })

  test('a failing read fails the request instead of answering a partial page', async () => {
    const dir = freshDir()
    const base = path.join(dir, BASE_NAME)
    fs.writeFileSync(`${base}.1`, makeFileText('e1'))
    fs.writeFileSync(base, 'newest a\nnewest b\n')
    jest.spyOn(fs.promises, 'open').mockImplementation(async (...args) => {
      const handle = await realOpen(...args)
      if (String(args[0]) === `${base}.1`) {
        let reads = 0
        const realRead = handle.read.bind(handle)
        handle.read = async (...readArgs) => {
          reads += 1
          if (reads === 2) throw Object.assign(new Error('EIO: i/o error, read'), { code: 'EIO' })
          return realRead(...readArgs)
        }
      }
      return handle
    })
    await expect(appModule.readLogWindow(base, { limit: 5000, cursor: 0 })).rejects.toMatchObject({ code: 'EIO' })

    process.env.RAG_FEEDER_STUB = '1'
    process.env.RAG_FEEDER_LOG_DIR = dir
    const app = appModule.createApp({ broadcast: () => {} })
    const consoleError = jest.spyOn(console, 'error').mockImplementation(() => {})
    const res = await request(app).get('/api/logs/tail?type=pipeline&lines=2000')
    expect(res.status).toBe(500)
    expect(res.body).toEqual({ error: 'EIO: i/o error, read' })
    expect(res.body).not.toHaveProperty('entries')
    expect(res.body).not.toHaveProperty('next_cursor')
    consoleError.mockRestore()
  })

  test('the tail route reads no file synchronously', async () => {
    const dir = freshDir()
    fs.writeFileSync(path.join(dir, BASE_NAME), 'a\nb\nc\nd\ne\n')
    fs.writeFileSync(path.join(dir, `${BASE_NAME}.1`), 'old\n')
    process.env.RAG_FEEDER_STUB = '1'
    process.env.RAG_FEEDER_LOG_DIR = dir
    const app = appModule.createApp({ broadcast: () => {} })
    const sync = jest.spyOn(fs, 'readFileSync')
    const res = await request(app).get('/api/logs/tail?type=pipeline&lines=4')
    expect(res.status).toBe(200)
    expect(res.body.entries).toEqual(['b', 'c', 'd', 'e'])
    expect(sync.mock.calls.filter(([p]) => String(p).includes('backend-pipeline'))).toEqual([])
  })

  test('readLogWindow continues into olderBase only when asked', async () => {
    const dir = freshDir()
    const base = path.join(dir, BASE_NAME)
    const legacy = path.join(dir, 'backend-pipeline.log')
    fs.writeFileSync(`${legacy}.1`, 'l1\nl2\n')
    fs.writeFileSync(legacy, 'l3\nl4\n')
    fs.writeFileSync(`${base}.1`, 'v1\nv2\n')
    fs.writeFileSync(base, 'v3\nv4\n')

    const { open } = countReads()
    const only = await appModule.readLogWindow(base, { limit: 100, cursor: 0 })
    expect(only).toEqual({ entries: ['v1', 'v2', 'v3', 'v4'], hasMore: false, cursor: 0, nextCursor: 4 })
    expect(open.mock.calls.filter(([p]) => path.basename(String(p)).startsWith('backend-pipeline.log'))).toEqual([])

    const merged = await appModule.readLogWindow(base, { limit: 100, cursor: 0, olderBase: legacy })
    expect(merged.entries).toEqual(['l1', 'l2', 'l3', 'l4', 'v1', 'v2', 'v3', 'v4'])
    expect(merged.hasMore).toBe(false)

    const page = await appModule.readLogWindow(base, { limit: 3, cursor: 3, olderBase: legacy })
    expect(page).toEqual({ entries: ['l3', 'l4', 'v1'], hasMore: true, cursor: 3, nextCursor: 6 })
  })
})

describe('logLineCorpusId', () => {
  test('logLineCorpusId trusts only a recognized writer in fixed position', () => {
    const ts = '[2026-01-01T00:00:00.000Z]'
    const id = appModule.logLineCorpusId
    expect(id(`${ts} [get_bib_pages][corpus=12] a`)).toBe(12)
    expect(id(`${ts} [extract-status] [corpus=12] [base=b] done`)).toBe(12)
    expect(id(`${ts} [APIscraper ERROR] [corpus=12] x`)).toBe(12)
    expect(id(`${ts} [extract-status][corpus=12][file=[corpus=9].pdf] a`)).toBe(12)
    expect(id(`${ts} [APIscraper inline] [corpus=12] [model=m] x`)).toBe(12)
    for (const line of [
      `${ts} [x][corpus=12] a`,
      `${ts} [get_bib_pages] failed: corpus=12`,
      `${ts} [DAEMON] [corpus=12] x`,
      '[get_bib_pages][corpus=12] a',
      `${ts} [get_bib_pages][base=b][corpus=12] a`,
      `${ts} [get_bib_pages][corpus=none] c`,
      `${ts} [APIscraper inline (m)] [corpus=12] x`,
      '',
    ]) {
      expect([line, id(line)]).toEqual([line, null])
    }
  })

  test('SCOPED_LOG_LABELS holds each fixed label with and without ERROR', () => {
    const labels = [
      'extract-status',
      '/api/extract-bibliography',
      'get_bib_pages',
      'APIscraper',
      'APIscraper inline fallback',
      'APIscraper (reused pages)',
      'APIscraper (page mode)',
      'APIscraper inline',
    ]
    expect([...appModule.SCOPED_LOG_LABELS].sort()).toEqual(
      [...labels, ...labels.map((l) => `${l} ERROR`)].sort()
    )
  })

  test('escapeLogLineBreaks turns every line break into the two characters \\n', () => {
    expect(appModule.escapeLogLineBreaks('a\r\nb\rc\nd')).toBe('a\\nb\\nc\\nd')
    expect(appModule.escapeLogLineBreaks('plain')).toBe('plain')
  })
})
