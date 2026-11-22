// Restores the TMPDIR global-setup.js replaced, then removes this run's dir and
// nothing else.
import fs from 'node:fs'

export default function globalTeardown() {
  const prior = process.env.DT_JEST_PRIOR_TMPDIR
  if (prior !== undefined) {
    if (prior === '') delete process.env.TMPDIR
    else process.env.TMPDIR = prior
    delete process.env.DT_JEST_PRIOR_TMPDIR
  }
  const root = process.env.DT_JEST_TMP_ROOT
  if (root) fs.rmSync(root, { recursive: true, force: true })
}
