import { spawnSync } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const backendDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')

// An exported RAG_FEEDER_DB_PATH (e.g. the real literature.db) must not reach
// the suites: setup-env.js overrides it with a temp path.
test('an inherited RAG_FEEDER_DB_PATH is replaced by a temp DB', () => {
  const env = { ...process.env, RAG_FEEDER_DB_PATH: '/nonexistent/forbidden.db' }
  delete env.DT_JEST_TMP_ROOT
  const result = spawnSync(
    process.execPath,
    ['--experimental-vm-modules', 'node_modules/jest/bin/jest.js', '--runTestsByPath', 'tests/db-path-probe.test.js'],
    { cwd: backendDir, env, encoding: 'utf8', timeout: 120000 }
  )
  expect(`${result.stdout}\n${result.stderr}`).toContain('1 passed')
  expect(result.status).toBe(0)
}, 130000)
