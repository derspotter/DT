import os from 'node:os'
import path from 'node:path'

// setup-env.js gives every test file a temp RAG_FEEDER_DB_PATH, even when the
// shell exports one. setup-env-isolation.test.js runs this file with a forbidden
// inherited path; in the normal run it is a plain isolation check.
test('RAG_FEEDER_DB_PATH is a per-file temp DB', () => {
  const dbPath = path.resolve(process.env.RAG_FEEDER_DB_PATH || '')
  expect(dbPath).not.toBe(path.resolve('/nonexistent/forbidden.db'))
  expect(dbPath.startsWith(path.resolve(os.tmpdir()) + path.sep)).toBe(true)
})
