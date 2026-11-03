import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { createPythonRunner } from '../src/pythonProcess.js'

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
  expect(error.message).toMatch(/^Python script failed \(3\)/)
  expect(error.message).toContain('failer')
  expect(error.message).toContain('boom')
  expect(error.exitCode).toBe(3)
  expect(out).toEqual(['done-ish'])
  expect(err).toEqual(['boom'])
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
