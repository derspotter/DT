import { spawnSync } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const backendDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')

test("every suite's os.tmpdir() is exactly the run dir", () => {
  expect(process.env.DT_JEST_TMP_ROOT).toBeTruthy()
  expect(os.tmpdir()).toBe(process.env.DT_JEST_TMP_ROOT)
})

// Runs global-setup.js and global-teardown.js in a fresh node process and
// reports what TMPDIR and the run dir look like during and after.
const PROBE = `
import fs from 'node:fs'
import globalSetup from './tests/global-setup.js'
import globalTeardown from './tests/global-teardown.js'
await globalSetup()
const root = process.env.DT_JEST_TMP_ROOT
const during = process.env.TMPDIR
const rootExisted = fs.existsSync(root)
await globalTeardown()
const after = process.env.TMPDIR
console.log(JSON.stringify({ root, during, rootExisted, after, rootExistsAfter: fs.existsSync(root) }))
`

function runProbe(env) {
  const result = spawnSync(process.execPath, ['--input-type=module', '-e', PROBE], {
    cwd: backendDir,
    env,
    encoding: 'utf8',
  })
  expect(result.stderr).toBe('')
  expect(result.status).toBe(0)
  return JSON.parse(result.stdout.trim())
}

function cleanEnv() {
  const env = { ...process.env }
  delete env.DT_JEST_TMP_ROOT
  delete env.DT_JEST_PRIOR_TMPDIR
  return env
}

test('global setup points TMPDIR at its run dir and teardown restores it', () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-tmpdir-base-'))
  try {
    const out = runProbe({ ...cleanEnv(), TMPDIR: base })
    expect(path.dirname(out.root)).toBe(base)
    expect(path.basename(out.root)).toMatch(/^dt-jest-run-/)
    expect(out.during).toBe(out.root)
    expect(out.rootExisted).toBe(true)
    expect(out.after).toBe(base)
    expect(out.rootExistsAfter).toBe(false)
    expect(fs.existsSync(base)).toBe(true)
  } finally {
    fs.rmSync(base, { recursive: true, force: true })
  }

  const env = cleanEnv()
  delete env.TMPDIR
  const unset = runProbe(env)
  expect(unset.during).toBe(unset.root)
  expect(unset.after).toBeUndefined()
  expect(unset.rootExistsAfter).toBe(false)
})

test('setup-env refuses to run without the run dir', () => {
  const result = spawnSync(process.execPath, ['tests/setup-env.js'], {
    cwd: backendDir,
    env: cleanEnv(),
    encoding: 'utf8',
  })
  expect(result.status).not.toBe(0)
  expect(result.stderr).toContain(
    "DT_JEST_TMP_ROOT is unset: run the backend tests through jest's globalSetup (npm test)."
  )
})
