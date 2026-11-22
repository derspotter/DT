// One parent dir per Jest run for every temp file the suites make. TMPDIR points
// at it before Jest starts any worker, so os.tmpdir() in every suite (and in its
// Python children) is the run dir; setup-env.js puts the per-file temp DBs there
// too. global-teardown.js restores TMPDIR and removes the dir.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

export default function globalSetup() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-jest-run-'))
  process.env.DT_JEST_TMP_ROOT = root
  process.env.DT_JEST_PRIOR_TMPDIR = process.env.TMPDIR ?? ''
  process.env.TMPDIR = root
}
