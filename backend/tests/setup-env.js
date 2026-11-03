// Jest setupFiles: runs in every test file before its modules load. Suites that
// import app.js statically would otherwise open dl_lit_project/data/literature.db
// (app.js falls back to it when RAG_FEEDER_DB_PATH is unset). Give each test file
// its own temp DB under the run's dir (global-setup.js, removed by
// global-teardown.js). The temp path is assigned unconditionally, so a shell that
// exports RAG_FEEDER_DB_PATH (e.g. the real DB) never reaches a suite; suites that
// set their own path in beforeAll before a dynamic import still win.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

const root = process.env.DT_JEST_TMP_ROOT || os.tmpdir()
const tmpDir = fs.mkdtempSync(path.join(root, 'dt-jest-'))
process.env.RAG_FEEDER_DB_PATH = path.join(tmpDir, 'literature.db')
