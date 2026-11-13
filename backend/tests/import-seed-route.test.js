// Route-level contract of POST /api/ingest/import-seed against the real
// ingest_import_seed.py (JSON only: CI's backend Python has no bibtexparser).
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import request from 'supertest'

const VENV_PYTHON = '/home/jay/DT/venv/bin/python'

describe('POST /api/ingest/import-seed', () => {
  let app
  let tmpDir = ''
  let uploadsDir = ''
  let token = ''
  let corpusId = null
  const saved = {}
  const ENV_KEYS = [
    'RAG_FEEDER_UPLOADS_DIR',
    'RAG_FEEDER_PYTHON',
    'RAG_FEEDER_ARTIFACTS_DIR',
    'RAG_FEEDER_JWT_SECRET',
    'RAG_ADMIN_USER',
    'RAG_ADMIN_PASSWORD',
  ]

  beforeAll(async () => {
    for (const key of ENV_KEYS) saved[key] = process.env[key]
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-import-seed-'))
    uploadsDir = path.join(tmpDir, 'uploads')
    fs.mkdirSync(uploadsDir)
    process.env.RAG_FEEDER_UPLOADS_DIR = uploadsDir
    process.env.RAG_FEEDER_ARTIFACTS_DIR = path.join(tmpDir, 'artifacts')
    process.env.RAG_FEEDER_PYTHON = fs.existsSync(VENV_PYTHON) ? VENV_PYTHON : 'python3'
    process.env.RAG_FEEDER_JWT_SECRET = 'import-seed-secret'
    process.env.RAG_ADMIN_USER = 'import-seed-admin'
    process.env.RAG_ADMIN_PASSWORD = 'import-seed-password'
    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast: () => {} })
    const login = await request(app).post('/api/auth/login').send({
      username: 'import-seed-admin',
      password: 'import-seed-password',
    })
    expect(login.status).toBe(200)
    token = login.body.token
    corpusId = login.body.user?.last_corpus_id
    expect(corpusId).toBeTruthy()
  })

  afterAll(() => {
    for (const key of ENV_KEYS) {
      if (saved[key] === undefined) delete process.env[key]
      else process.env[key] = saved[key]
    }
    fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  const importSeed = (body) =>
    request(app).post('/api/ingest/import-seed').set('Authorization', `Bearer ${token}`).send(body)

  test('a malformed seed file answers the stopped summary, not a 500', async () => {
    fs.writeFileSync(path.join(uploadsDir, 'bad.json'), '{not json')
    const res = await importSeed({ filename: 'bad.json', kind: 'json' })
    expect(res.status).toBe(200)
    expect(res.body.stopped).toBe(true)
    expect(res.body.processed).toBe(0)
    expect(res.body.added).toBe(0)
    expect(res.body.error.startsWith('JSONDecodeError: Failed to parse json:')).toBe(true)
  }, 30_000)

  test('a good json seed answers stopped false with processed === total', async () => {
    fs.writeFileSync(
      path.join(uploadsDir, 'good.json'),
      JSON.stringify([{ title: 'Seed Route One' }, { title: 'Seed Route Two', year: 2020 }])
    )
    const res = await importSeed({ filename: 'good.json', kind: 'json' })
    expect(res.status).toBe(200)
    expect(res.body.stopped).toBe(false)
    expect(res.body.error).toBeNull()
    expect(res.body.total).toBe(2)
    expect(res.body.processed).toBe(res.body.total)
    expect(res.body.added).toBe(2)
    expect(res.body.corpus_linked).toBe(2)
    const db = new Database(process.env.RAG_FEEDER_DB_PATH, { readonly: true })
    try {
      const linked = db
        .prepare(
          `SELECT COUNT(*) AS c FROM corpus_works cw JOIN works w ON w.id = cw.work_id
           WHERE cw.corpus_id = ? AND w.title LIKE 'Seed Route %'`
        )
        .get(corpusId)
      expect(linked.c).toBe(2)
    } finally {
      db.close()
    }
  }, 30_000)
})
