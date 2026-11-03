import fs from 'node:fs'

export default function globalTeardown() {
  const root = process.env.DT_JEST_TMP_ROOT
  if (root) fs.rmSync(root, { recursive: true, force: true })
}
