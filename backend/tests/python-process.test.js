import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { createPythonRunner, parseJsonFromOutput } from '../src/pythonProcess.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const VENV_PYTHON = '/home/jay/DT/venv/bin/python'
const pythonExec = fs.existsSync(VENV_PYTHON) ? VENV_PYTHON : 'python3'

let tmpDir
beforeAll(() => {
  // A space in the path checks that script paths are passed as one argv entry.
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'python process '))
})
afterAll(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true })
})

function writeScript(name, body) {
  const file = path.join(tmpDir, name)
  fs.writeFileSync(file, body)
  return file
}

function makeRunner(appSettingsEnv = () => ({})) {
  return createPythonRunner({
    pythonExec,
    dlLitProjectDir: '/opt/dl_lit_project',
    repoRoot: '/opt',
    dbPath: path.join(tmpDir, 'default.db'),
    appSettingsEnv,
  })
}

test('pythonEnv merges appSettingsEnv under extra and sets PYTHONPATH/db vars', () => {
  const runner = makeRunner(() => ({ RAG_FEEDER_GEMINI_MODEL: 'from-admin', X: '1' }))
  const env = runner.pythonEnv({ RAG_FEEDER_GEMINI_MODEL: 'override' })
  expect(env.RAG_FEEDER_GEMINI_MODEL).toBe('override')
  expect(env.X).toBe('1')
  expect(env.PYTHONPATH.startsWith('/opt/dl_lit_project')).toBe(true)
  expect(env.PYTHONPATH.split(path.delimiter)).toContain('/opt')
  expect(env.RAG_FEEDER_DL_LIT_PROJECT_DIR).toBe('/opt/dl_lit_project')
  expect(env.RAG_FEEDER_DB_PATH).toBe(path.join(tmpDir, 'default.db'))
})

test('spawnPython forwards complete lines and flushes the partial tail on close', async () => {
  const script = writeScript('lines.py', [
    'import os, sys',
    'print("a")',
    'sys.stdout.flush()',
    'sys.stderr.write("e\\n")',
    'sys.stderr.flush()',
    'print(os.environ["RAG_FEEDER_DB_PATH"] + "|" + "|".join(sys.argv[1:]))',
    'print("b", end="")',
  ].join('\n'))
  const out = []
  const err = []
  const { child, done } = makeRunner().spawnPython(script, ['x y'], {
    dbPath: '/tmp/other.db',
    corpusId: 0,
    onStdoutLine: (line) => out.push(line),
    onStderrLine: (line) => err.push(line),
  })
  expect(typeof child.pid).toBe('number')
  const result = await done
  expect(out).toEqual(['a', '/tmp/other.db|x y|--corpus-id|0', 'b'])
  expect(err).toEqual(['e'])
  expect(result.code).toBe(0)
  expect(result.stdout).toContain('a\n')
  expect(result.stderr).toBe('e\n')
})

test('spawnPython rejects once on non-zero exit with the partial line still forwarded', async () => {
  const script = writeScript('fail.py', [
    'import sys',
    'print("done-ish", end="")',
    'sys.stdout.flush()',
    'sys.stderr.write("boom")',
    'sys.exit(3)',
  ].join('\n'))
  const out = []
  const err = []
  const { done } = makeRunner().spawnPython(script, [], {
    corpusId: null,
    label: 'failer',
    onStdoutLine: (line) => out.push(line),
    onStderrLine: (line) => err.push(line),
  })
  let caught = 0
  let error = null
  await done.catch((e) => { caught += 1; error = e })
  expect(caught).toBe(1)
  expect(error.message).toBe('Python script failed (3): boom')
  expect(error.label).toBe('failer')
  expect(error.exitCode).toBe(3)
  expect(out).toEqual(['done-ish'])
  expect(err).toEqual(['boom'])
})

test('spawnPython keeps a multi-byte character split across two chunks intact', async () => {
  const script = writeScript('split_utf8.py', [
    'import sys, time',
    'sys.stdout.buffer.write(b"\\xc3")',
    'sys.stdout.buffer.flush()',
    'time.sleep(0.2)',
    'sys.stdout.buffer.write(b"\\xa4\\n")',
    'sys.stdout.buffer.flush()',
  ].join('\n'))
  const out = []
  const { done } = makeRunner().spawnPython(script, [], { onStdoutLine: (line) => out.push(line) })
  const result = await done
  expect(out).toEqual(['ä'])
  expect(result.stdout).toBe('ä\n')
})

test('runPythonJson parses the last JSON line', async () => {
  const script = writeScript('emit_payload.py', [
    'import json, sys',
    'print("[DB Manager] noise")',
    'print(json.dumps({"first": True}))',
    'print(json.dumps({"args": sys.argv[1:]}))',
  ].join('\n'))
  const payload = await makeRunner().runPythonJson(script, ['q'], { corpusId: 7 })
  expect(payload).toEqual({ args: ['q', '--corpus-id', '7'] })
})

test('app.js no longer imports spawnSync and has no PYTHONPATH literal', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'src', 'app.js'), 'utf8')
  expect(src).not.toMatch(/\bspawnSync\b/)
  expect(src).not.toMatch(/PYTHONPATH:/)
})

test('spawnPython does not add a second --corpus-id', async () => {
  const script = writeScript('print_argv.py', [
    'import json, sys',
    'print(json.dumps(sys.argv[1:]))',
  ].join('\n'))
  const runner = makeRunner()
  const given = JSON.parse((await runner.spawnPython(script, ['--corpus-id', '5'], { corpusId: 5 }).done).stdout)
  expect(given.filter((a) => a === '--corpus-id')).toHaveLength(1)
  expect(given).toEqual(['--corpus-id', '5'])
  const appended = JSON.parse((await runner.spawnPython(script, ['x'], { corpusId: 5 }).done).stdout)
  expect(appended).toEqual(['x', '--corpus-id', '5'])
})

test('the import-seed route leaves --corpus-id to spawnPython', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'src', 'app.js'), 'utf8')
  const start = src.indexOf("app.post('/api/ingest/import-seed'")
  expect(start).toBeGreaterThan(0)
  const body = src.slice(start, src.indexOf('app.post(', start + 10))
  expect(body).not.toMatch(/--corpus-id/)
})

// D12 (BE-42): the fallback is the original multiline search, capped by a
// cumulative work budget (controller ruling): within the budget every result
// equals 95d861e's, a pathological input throws instead of pinning a core.
const NO_JSON = 'No valid JSON output from python.'

// Runs the parser while measuring the work actually done: every string built by
// Array#join and every character handed to JSON.parse, independent of the
// parser's own counters.
function measured(output) {
  const stats = { visited: 0, parses: 0, parsedChars: 0 }
  const realJoin = Array.prototype.join
  const realParse = JSON.parse
  const seen = { joined: 0, parsed: 0 }
  Array.prototype.join = function join(...args) {
    const out = realJoin.apply(this, args)
    seen.joined += out.length
    return out
  }
  JSON.parse = function parse(text, ...rest) {
    seen.parsed += String(text).length
    return realParse(text, ...rest)
  }
  let result
  let error = null
  try {
    result = parseJsonFromOutput(output, { stats })
  } catch (e) {
    error = e
  } finally {
    Array.prototype.join = realJoin
    JSON.parse = realParse
  }
  return { stats, seen, result, error }
}

const pathological = {
  'tagged log lines with brackets': Array.from({ length: 300 }, (_, i) => `[DB Manager] step ${i} [${i}/300]`).join('\n'),
  'repeated unclosed openers': Array.from({ length: 2000 }, (_, i) => `[warn ${i} {unclosed`).join('\n'),
  'overlapping malformed multiline blocks': [
    ...Array.from({ length: 1000 }, () => '{"a":'),
    'oops',
    ...Array.from({ length: 1000 }, () => '}'),
  ].join('\n'),
  'nested blocks closing on one long line': [...Array.from({ length: 8000 }, () => '{"a":'), 'oops', '}'.repeat(8000)].join('\n'),
}

for (const [name, output] of Object.entries(pathological)) {
  test(`a pathological input throws within the work budget: ${name}`, () => {
    const { stats, seen, error } = measured(output)
    expect(error?.message).toBe(`No valid JSON output from python. Output was: ${output}`)
    const cap = 4 * output.length + 64 * 1024
    expect(stats.cap).toBe(cap)
    expect(stats.parses).toBeGreaterThan(0)
    expect(stats.visited + stats.parsedChars).toBeLessThanOrEqual(cap)
    // The real work, measured outside the parser: the fast path reads each line
    // once; everything else is the fallback, which must stay within the cap.
    expect(seen.joined + seen.parsed).toBeLessThanOrEqual(cap + 2 * output.length)
  })
}

test('the thrown error text is unchanged', () => {
  expect(() => parseJsonFromOutput('[log] nothing')).toThrow('No valid JSON output from python. Output was: [log] nothing')
})

// Strict parity with 95d861e (each expected value is what that revision returns).
const parity = [
  ['a later single-line log label does not displace an earlier JSON result', '{\n "ok": true\n}\n[3] done', { ok: true }],
  ['a pretty-printed object with a nested array returns the whole object', JSON.stringify({ items: [1], ok: true }, null, 2), { items: [1], ok: true }],
  ['diagnostic text with a quote before the first opener does not hide the block', 'warning " opening {\n  "ok": true\n}', { ok: true }],
  // Ruling: the unchanged fast path wins, so the brief's fixture with
  // '"n": [1, 2]' on its own line returns [1, 2]; the reordered one exercises the fallback.
  ['the fast path takes a nested array alone on its line', '[log] a\n{\n  "ok": true,\n  "n": [1, 2]\n}\n[log] done', [1, 2]],
  ['a pretty-printed block before trailing log lines', '[log] a\n{\n  "n": [1, 2],\n  "ok": true\n}\n[log] done', { ok: true, n: [1, 2] }],
  ['brackets inside strings', '{\n "s": "a } ] {"\n}', { s: 'a } ] {' }],
  ['escaped quotes and backslashes', '{\n "q": "say \\"hi\\" \\\\"\n}', { q: 'say "hi" \\' }],
  ['a quote outside a block', '[log] it said "unterminated\n{\n "a": 1\n}', { a: 1 }],
  ['a dangling [ before a quoted diagnostic', '[warn] items: [\nmsg: "oops {\n  "ok": true\n}', { ok: true }],
  ['a dangling [ before an unquoted diagnostic', '[warn] unmatched [ here\nnote "quoted" then {\n  "ok": true\n}', { ok: true }],
  ['a dangling [ before a line starting with a quote', '[warn] items: [\n"oops {\n  "ok": true\n}', { ok: true }],
  ['a dangling [ and a bracket inside a string', '[warn] items: [\nresult: {"text": "[",\n "ok": true\n}', { text: '[', ok: true }],
]
for (const [name, output, expected] of parity) {
  test(`parity: ${name}`, () => {
    expect(parseJsonFromOutput(output)).toEqual(expected)
  })
}

test('parity: an outer block with trailing text on its closing line stays unparsed', () => {
  // 95d861e throws here too; the budgeted fallback keeps its results.
  expect(() => parseJsonFromOutput('{\n "a": 1\n} done')).toThrow(NO_JSON)
})
