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

// D12 (BE-42): the JSON fallback follows brackets in one scan within a parse budget.
const NO_JSON = 'No valid JSON output from python.'

test('fallback parsing is one scan plus one parse per bracketed line', () => {
  const stats = { visited: 0, parses: 0, parsedChars: 0 }
  const output = Array.from({ length: 300 }, (_, i) => `[DB Manager] step ${i} [${i}/300]`).join('\n')
  expect(() => parseJsonFromOutput(output, { stats })).toThrow(NO_JSON)
  expect(stats.visited).toBeGreaterThan(0)
  expect(stats.visited).toBeLessThanOrEqual(output.length)
  expect(stats.parses).toBe(0)
})

test('repeated unclosed openers do not rescan the suffix', () => {
  const stats = { visited: 0, parses: 0, parsedChars: 0 }
  const output = Array.from({ length: 2000 }, (_, i) => `[warn ${i} {unclosed`).join('\n')
  expect(() => parseJsonFromOutput(output, { stats })).toThrow(NO_JSON)
  expect(stats.visited).toBeGreaterThan(0)
  expect(stats.visited).toBeLessThanOrEqual(output.length)
  expect(stats.parses).toBe(0)
})

test('overlapping malformed multiline blocks stay within the parse budget', () => {
  const stats = { visited: 0, parses: 0, parsedChars: 0 }
  const output = [...Array.from({ length: 1000 }, () => '{"a":'), 'oops', ...Array.from({ length: 1000 }, () => '}')].join('\n')
  expect(() => parseJsonFromOutput(output, { stats })).toThrow(NO_JSON)
  expect(stats.parses).toBeGreaterThan(0)
  expect(stats.parsedChars).toBeGreaterThan(0)
  expect(stats.parsedChars).toBeLessThanOrEqual(4 * output.length)
})

test('the thrown error text is unchanged', () => {
  expect(() => parseJsonFromOutput('[log] nothing')).toThrow('No valid JSON output from python. Output was: [log] nothing')
})

test('parity: a later single-line log label does not displace an earlier JSON result', () => {
  expect(parseJsonFromOutput('{\n "ok": true\n}\n[3] done')).toEqual({ ok: true })
})

test('parity: a pretty-printed object with a nested array returns the whole object', () => {
  expect(parseJsonFromOutput(JSON.stringify({ items: [1], ok: true }, null, 2))).toEqual({ items: [1], ok: true })
})

test('a pretty-printed block before trailing log lines is found', () => {
  // The nested array is not alone on its line: a line ending '[1, 2]' would be
  // taken by the unchanged fast path before the fallback runs.
  expect(parseJsonFromOutput('[log] a\n{\n  "n": [1, 2],\n  "ok": true\n}\n[log] done')).toEqual({ ok: true, n: [1, 2] })
  expect(parseJsonFromOutput('{\n "s": "a } ] {"\n}')).toEqual({ s: 'a } ] {' })
  expect(parseJsonFromOutput('{\n "q": "say \\"hi\\" \\\\"\n}')).toEqual({ q: 'say "hi" \\' })
  expect(parseJsonFromOutput('[log] it said "unterminated\n{\n "a": 1\n}')).toEqual({ a: 1 })
  expect(parseJsonFromOutput('{\n "a": 1\n} done')).toEqual({ a: 1 })
})

test('parity: diagnostic text with a quote before the first opener does not hide the block', () => {
  expect(parseJsonFromOutput('warning " opening {\n  "ok": true\n}')).toEqual({ ok: true })
})
