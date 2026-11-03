// One parent dir per Jest run for the per-file temp DBs made by setup-env.js;
// workers inherit the env var, global-teardown.js removes the dir.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

export default function globalSetup() {
  process.env.DT_JEST_TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-jest-run-'))
}
