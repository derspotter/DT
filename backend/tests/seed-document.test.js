import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { resolveSeedDocumentPath } from '../src/seed.js'

describe('resolveSeedDocumentPath', () => {
  let uploadsDir

  beforeEach(() => {
    uploadsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seed-doc-'))
  })

  afterEach(() => {
    fs.rmSync(uploadsDir, { recursive: true, force: true })
  })

  test('serves the stored upload path when it exists inside the uploads dir', () => {
    const stored = path.join(uploadsDir, '123-paper.pdf')
    fs.writeFileSync(stored, 'pdf')
    expect(resolveSeedDocumentPath({ sourcePdf: stored, sourceKey: '123-paper', uploadsDir })).toBe(stored)
  })

  test('falls back to the upload named after the seed when the stored path is an artifact', () => {
    const upload = path.join(uploadsDir, '123-paper.pdf')
    fs.writeFileSync(upload, 'pdf')
    const artifact = path.join(os.tmpdir(), 'artifacts', '123-paper', '123-paper_refs_physical_p36.pdf')
    expect(resolveSeedDocumentPath({ sourcePdf: artifact, sourceKey: '123-paper', uploadsDir })).toBe(upload)
  })

  test('returns null when neither file exists', () => {
    expect(resolveSeedDocumentPath({ sourcePdf: path.join(uploadsDir, 'gone.pdf'), sourceKey: 'gone', uploadsDir })).toBeNull()
  })

  test('refuses paths that escape the uploads dir even when they exist', () => {
    const outside = path.join(os.tmpdir(), `outside-${process.pid}.pdf`)
    fs.writeFileSync(outside, 'pdf')
    try {
      expect(resolveSeedDocumentPath({ sourcePdf: outside, sourceKey: '../outside', uploadsDir })).toBeNull()
    } finally {
      fs.rmSync(outside, { force: true })
    }
  })
})
