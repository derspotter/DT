// Undoes exactly what this process's global-setup.js did: restores the TMPDIR it
// replaced and removes the run dir it made. Without that record (a teardown with
// no matching setup, e.g. in a child that inherited the parent's env) it changes
// and deletes nothing.
import fs from 'node:fs'

const RUN_RECORD = Symbol.for('dt.jest.runDir')

export default function globalTeardown() {
  const record = globalThis[RUN_RECORD]
  if (!record) return
  delete globalThis[RUN_RECORD]
  if (record.priorTmpdir === '') delete process.env.TMPDIR
  else process.env.TMPDIR = record.priorTmpdir
  delete process.env.DT_JEST_PRIOR_TMPDIR
  fs.rmSync(record.root, { recursive: true, force: true })
}
