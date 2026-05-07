import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import request from 'supertest'

describe('scraper metadata draft export', () => {
  let app
  let token = ''
  let tmpDir = ''
  let dbPath = ''
  let targetDir = ''
  let sourceFile = ''

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-scraper-draft-'))
    dbPath = path.join(tmpDir, 'draft.db')
    targetDir = path.join(tmpDir, 'upstream-target')
    const artifactDir = path.join(tmpDir, 'artifacts')
    fs.mkdirSync(targetDir, { recursive: true })
    fs.mkdirSync(artifactDir, { recursive: true })
    sourceFile = path.join(artifactDir, 'source.pdf')
    fs.writeFileSync(sourceFile, 'PDF bytes')
    fs.writeFileSync(
      path.join(targetDir, 'metadata.bib'),
      '@book{existing,\n  title = {Existing Entry}\n}\n',
      'utf8'
    )

    process.env.RAG_FEEDER_STUB = '1'
    process.env.RAG_FEEDER_DB_PATH = dbPath
    process.env.RAG_FEEDER_ARTIFACTS_DIR = artifactDir
    process.env.RAG_FEEDER_KANTROPOS_CORPORA = JSON.stringify([
      { id: 'target-a', name: 'Target A', path: targetDir },
    ])

    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast: () => {} })
    const login = await request(app)
      .post('/api/auth/login')
      .send({ username: 'stub-admin', password: 'stub-password' })
    token = login.body?.token || ''
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
    delete process.env.RAG_FEEDER_DB_PATH
    delete process.env.RAG_FEEDER_ARTIFACTS_DIR
    delete process.env.RAG_FEEDER_KANTROPOS_CORPORA
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  const seedArtifact = () => {
    const db = new Database(dbPath)
    try {
      const runId = Number(
        db
          .prepare("INSERT INTO scraper_runs (source_type, query_json, status) VALUES ('eurlex', '{}', 'completed')")
          .run()
          .lastInsertRowid
      )
      return Number(
        db
          .prepare(
            `INSERT INTO scraper_artifacts (
               run_id, source_type, source_id, title, year, source_url, local_artifact_path,
               file_name, raw_metadata_json, bibtex_key, status
             ) VALUES (?, 'eurlex', '32024R1689', 'AI Act', 2024, 'https://example.test/ai-act', ?, 'ai-act.pdf', '{}', 'eurlex_32024R1689', 'downloaded')`
          )
          .run(runId, sourceFile)
          .lastInsertRowid
      )
    } finally {
      db.close()
    }
  }

  test('creates a downloadable metadata.bib draft without changing the upstream metadata.bib', async () => {
    const originalBib = fs.readFileSync(path.join(targetDir, 'metadata.bib'), 'utf8')
    const artifactId = seedArtifact()

    const applyRes = await request(app)
      .post('/api/scraper/apply')
      .set('Authorization', `Bearer ${token}`)
      .send({ targetId: 'target-a', artifactIds: [artifactId] })

    expect(applyRes.status).toBe(200)
    expect(applyRes.body.mode).toBe('draft')
    expect(applyRes.body.applied_count).toBe(1)
    expect(applyRes.body.generated_bib_url).toContain('/metadata-bib')
    expect(fs.readFileSync(path.join(targetDir, 'metadata.bib'), 'utf8')).toBe(originalBib)
    expect(fs.existsSync(path.join(targetDir, 'scraper', 'eurlex', 'ai-act.pdf'))).toBe(false)

    const downloadRes = await request(app)
      .get(applyRes.body.generated_bib_url)
      .set('Authorization', `Bearer ${token}`)

    expect(downloadRes.status).toBe(200)
    expect(downloadRes.text).toContain('@book{existing')
    expect(downloadRes.text).toContain('@misc{eurlex_32024R1689')
    expect(downloadRes.headers['content-disposition']).toContain('metadata.bib')
  })

  test('rejects empty downloaded artifacts when creating a metadata draft', async () => {
    const emptyFile = path.join(tmpDir, 'artifacts', 'empty.pdf')
    fs.writeFileSync(emptyFile, '')
    const db = new Database(dbPath)
    let artifactId = 0
    try {
      const runId = Number(
        db
          .prepare("INSERT INTO scraper_runs (source_type, query_json, status) VALUES ('eurlex', '{}', 'completed')")
          .run()
          .lastInsertRowid
      )
      artifactId = Number(
        db
          .prepare(
            `INSERT INTO scraper_artifacts (
               run_id, source_type, source_id, title, year, source_url, local_artifact_path,
               file_name, raw_metadata_json, bibtex_key, status
             ) VALUES (?, 'eurlex', 'EMPTY', 'Empty artifact', 2024, 'https://example.test/empty', ?, 'empty.pdf', '{}', 'eurlex_empty', 'downloaded')`
          )
          .run(runId, emptyFile)
          .lastInsertRowid
      )
    } finally {
      db.close()
    }

    const applyRes = await request(app)
      .post('/api/scraper/apply')
      .set('Authorization', `Bearer ${token}`)
      .send({ targetId: 'target-a', artifactIds: [artifactId] })

    expect(applyRes.status).toBe(200)
    expect(applyRes.body.applied_count).toBe(0)
    expect(applyRes.body.error_count).toBe(1)
    expect(applyRes.body.generated_bib_url).toBe('')
    expect(applyRes.body.details).toEqual([
      expect.objectContaining({ artifact_id: artifactId, status: 'error', reason: 'source_file_empty' }),
    ])
  })

  test('creates a download ticket for an imported upstream work file', async () => {
    const upstreamPdf = path.join(targetDir, 'downloaded.pdf')
    fs.writeFileSync(upstreamPdf, 'upstream PDF bytes')
    const db = new Database(dbPath)
    let workId = 0
    try {
      workId = Number(
        db
          .prepare(
            `INSERT INTO works (
               title, normalized_title, authors, year, metadata_status, download_status,
               file_path, origin_type, origin_key
             ) VALUES (?, ?, ?, ?, 'matched', 'downloaded', ?, 'bibtex_import', ?)`
          )
          .run(
            'Downloaded upstream item',
            'downloaded upstream item',
            'Upstream Author',
            2026,
            '/downloaded.pdf',
            path.join(targetDir, 'metadata.bib')
          )
          .lastInsertRowid
      )
    } finally {
      db.close()
    }

    const ticketRes = await request(app)
      .post(`/api/kantropos/corpora/target-a/works/${workId}/download-ticket`)
      .set('Authorization', `Bearer ${token}`)

    expect(ticketRes.status).toBe(200)
    expect(ticketRes.body.url).toContain('/file?ticket=')

    const fileRes = await request(app).get(ticketRes.body.url)

    expect(fileRes.status).toBe(200)
    expect(fileRes.body.toString()).toBe('upstream PDF bytes')
    expect(fileRes.headers['content-disposition']).toContain('downloaded.pdf')
  })
})
