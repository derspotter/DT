// One parent dir per Jest run for every temp file the suites make. TMPDIR points
// at it before Jest starts any worker, so os.tmpdir() in every suite (and in its
// Python children) is the run dir; setup-env.js puts the per-file temp DBs there
// too. global-teardown.js restores TMPDIR and removes the dir. The dir is also
// recorded on globalThis (Jest shares globals between globalSetup and
// globalTeardown), and teardown deletes only that record, never an inherited
// DT_JEST_TMP_ROOT.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

const RUN_RECORD = Symbol.for('dt.jest.runDir')

export default function globalSetup() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-jest-run-'))
  const priorTmpdir = process.env.TMPDIR ?? ''
  globalThis[RUN_RECORD] = { root, priorTmpdir }
  process.env.DT_JEST_TMP_ROOT = root
  process.env.DT_JEST_PRIOR_TMPDIR = priorTmpdir
  process.env.TMPDIR = root
}
