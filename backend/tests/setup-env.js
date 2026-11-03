// Jest setupFiles: runs in every test file before its modules load. Suites that
// import app.js statically would otherwise open dl_lit_project/data/literature.db
// (app.js falls back to it when RAG_FEEDER_DB_PATH is unset). Give each test file
// its own temp DB under the run's dir (global-setup.js, removed by
// global-teardown.js); suites that set their own path before importing still win.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

if (!process.env.RAG_FEEDER_DB_PATH) {
  const root = process.env.DT_JEST_TMP_ROOT || os.tmpdir()
  const tmpDir = fs.mkdtempSync(path.join(root, 'dt-jest-'))
  process.env.RAG_FEEDER_DB_PATH = path.join(tmpDir, 'literature.db')
}
