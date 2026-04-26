import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'

describe('corpus sharing and multi-user isolation', () => {
  let app
  let dbPath = ''
  let tmpDir = ''
  const adminUsername = 'sharing-admin'
  const adminPassword = 'sharing-admin-password'

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-corpus-sharing-'))
    dbPath = path.join(tmpDir, 'sharing.db')
    process.env.RAG_FEEDER_DB_PATH = dbPath
    process.env.RAG_FEEDER_JWT_SECRET = 'sharing-secret'
    process.env.RAG_ADMIN_USER = adminUsername
    process.env.RAG_ADMIN_PASSWORD = adminPassword
    process.env.RAG_FEEDER_KANTROPOS_CORPORA = JSON.stringify([
      { id: 'target-a', name: 'Target A', path: path.join(tmpDir, 'target-a') },
    ])

    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast: () => {} })
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_DB_PATH
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
    delete process.env.RAG_FEEDER_KANTROPOS_CORPORA
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  const withDb = (fn) => {
    const db = new Database(dbPath)
    try {
      return fn(db)
    } finally {
      db.close()
    }
  }

  const login = async (username, password) => {
    const res = await request(app).post('/api/auth/login').send({ username, password })
    expect(res.status).toBe(200)
    return res.body
  }

  const createActiveUser = (username, password) =>
    withDb((db) => {
      const passwordHash = bcrypt.hashSync(password, 10)
      const user = db
        .prepare(
          `INSERT INTO users (username, password_hash, email, account_status, password_set_at)
           VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP)`
        )
        .run(username, passwordHash, `${username}@example.test`)
      const userId = Number(user.lastInsertRowid)
      const corpus = db
        .prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)')
        .run(`${username}'s private corpus`, userId)
      const corpusId = Number(corpus.lastInsertRowid)
      db
        .prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')")
        .run(userId, corpusId)
      db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpusId, userId)
      return { userId, corpusId }
    })

  const seedCorpusWork = (corpusId, title) =>
    withDb((db) => {
      const work = db
        .prepare(
          `INSERT INTO works (title, normalized_title, authors, year, metadata_status, download_status)
           VALUES (?, ?, ?, ?, 'matched', 'not_requested')`
        )
        .run(title, title.toLowerCase(), 'Shared Author', 2026)
      const workId = Number(work.lastInsertRowid)
      db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)').run(corpusId, workId)
      return workId
    })

  const seedDownloadedWork = (corpusId, { title, filePath }) =>
    withDb((db) => {
      const work = db
        .prepare(
          `INSERT INTO works (title, normalized_title, authors, year, metadata_status, download_status, file_path)
           VALUES (?, ?, ?, ?, 'matched', 'downloaded', ?)`
        )
        .run(title, title.toLowerCase(), 'Shared Author', 2026, filePath)
      const workId = Number(work.lastInsertRowid)
      db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)').run(corpusId, workId)
      return workId
    })

  test('shared corpus is visible to another user without leaking selected corpus state', async () => {
    const admin = await login(adminUsername, adminPassword)
    const sharedCorpusId = Number(admin.user.last_corpus_id)
    seedCorpusWork(sharedCorpusId, 'Shared corpus sentinel')

    const privateRes = await request(app)
      .post('/api/corpora')
      .set('Authorization', `Bearer ${admin.token}`)
      .send({ name: 'Admin private corpus' })
    expect(privateRes.status).toBe(201)
    const adminPrivateCorpusId = Number(privateRes.body.id)

    const username = 'sharing-reader'
    const password = 'sharing-reader-password'
    const reader = createActiveUser(username, password)
    const readerLogin = await login(username, password)

    const forbiddenSelect = await request(app)
      .post(`/api/corpora/${adminPrivateCorpusId}/select`)
      .set('Authorization', `Bearer ${readerLogin.token}`)
    expect(forbiddenSelect.status).toBe(403)

    const shareRes = await request(app)
      .post(`/api/corpora/${sharedCorpusId}/share`)
      .set('Authorization', `Bearer ${admin.token}`)
      .send({ username, role: 'viewer' })
    expect(shareRes.status).toBe(200)

    const readerMe = await request(app)
      .get('/api/auth/me')
      .set('Authorization', `Bearer ${readerLogin.token}`)
    expect(readerMe.status).toBe(200)
    expect(readerMe.body.corpora).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: sharedCorpusId, role: 'viewer' }),
        expect.objectContaining({ id: reader.corpusId, role: 'owner' }),
      ])
    )

    await Promise.all([
      request(app).post(`/api/corpora/${sharedCorpusId}/select`).set('Authorization', `Bearer ${admin.token}`),
      request(app).post(`/api/corpora/${sharedCorpusId}/select`).set('Authorization', `Bearer ${readerLogin.token}`),
    ])

    const [adminCorpus, readerCorpus] = await Promise.all([
      request(app).get('/api/corpus').set('Authorization', `Bearer ${admin.token}`),
      request(app).get('/api/corpus').set('Authorization', `Bearer ${readerLogin.token}`),
    ])
    expect(adminCorpus.status).toBe(200)
    expect(readerCorpus.status).toBe(200)
    expect(adminCorpus.body.items).toEqual(
      expect.arrayContaining([expect.objectContaining({ title: 'Shared corpus sentinel' })])
    )
    expect(readerCorpus.body.items).toEqual(
      expect.arrayContaining([expect.objectContaining({ title: 'Shared corpus sentinel' })])
    )

    await Promise.all([
      request(app).post(`/api/corpora/${adminPrivateCorpusId}/select`).set('Authorization', `Bearer ${admin.token}`),
      request(app).post(`/api/corpora/${sharedCorpusId}/select`).set('Authorization', `Bearer ${readerLogin.token}`),
    ])

    const [adminMe, readerMeAfter] = await Promise.all([
      request(app).get('/api/auth/me').set('Authorization', `Bearer ${admin.token}`),
      request(app).get('/api/auth/me').set('Authorization', `Bearer ${readerLogin.token}`),
    ])
    expect(adminMe.body.user.last_corpus_id).toBe(adminPrivateCorpusId)
    expect(readerMeAfter.body.user.last_corpus_id).toBe(sharedCorpusId)
  })

  test('viewer shares are read-only and editor shares can write corpus configuration', async () => {
    const admin = await login(adminUsername, adminPassword)
    const corpusId = Number(admin.user.last_corpus_id)
    const username = 'sharing-editor'
    const password = 'sharing-editor-password'
    createActiveUser(username, password)
    const userLogin = await login(username, password)

    const invalidRole = await request(app)
      .post(`/api/corpora/${corpusId}/share`)
      .set('Authorization', `Bearer ${admin.token}`)
      .send({ username, role: 'owner' })
    expect(invalidRole.status).toBe(400)

    const viewerShare = await request(app)
      .post(`/api/corpora/${corpusId}/share`)
      .set('Authorization', `Bearer ${admin.token}`)
      .send({ username, role: 'viewer' })
    expect(viewerShare.status).toBe(200)

    const selectShared = await request(app)
      .post(`/api/corpora/${corpusId}/select`)
      .set('Authorization', `Bearer ${userLogin.token}`)
    expect(selectShared.status).toBe(200)

    const viewerWrite = await request(app)
      .put(`/api/corpora/${corpusId}/kantropos-assignment`)
      .set('Authorization', `Bearer ${userLogin.token}`)
      .send({ targetId: 'target-a' })
    expect(viewerWrite.status).toBe(403)

    const viewerCurrentCorpusWrite = await request(app)
      .post('/api/seed/candidates/dismiss')
      .set('Authorization', `Bearer ${userLogin.token}`)
      .send({ sourceType: 'search', sourceKey: 'run-1', candidateKeys: ['candidate-1'] })
    expect(viewerCurrentCorpusWrite.status).toBe(403)

    const editorShare = await request(app)
      .post(`/api/corpora/${corpusId}/share`)
      .set('Authorization', `Bearer ${admin.token}`)
      .send({ username, role: 'editor' })
    expect(editorShare.status).toBe(200)

    const editorWrite = await request(app)
      .put(`/api/corpora/${corpusId}/kantropos-assignment`)
      .set('Authorization', `Bearer ${userLogin.token}`)
      .send({ targetId: 'target-a' })
    expect(editorWrite.status).toBe(200)
    expect(editorWrite.body.assignment).toEqual(expect.objectContaining({ target_id: 'target-a' }))
  })

  test('downloaded corpus files can be served from relative pdf_library paths', async () => {
    const admin = await login(adminUsername, adminPassword)
    const corpusId = Number(admin.user.last_corpus_id)
    const pdfLibraryDir = path.join(path.dirname(dbPath), 'pdf_library')
    fs.mkdirSync(pdfLibraryDir, { recursive: true })
    fs.writeFileSync(path.join(pdfLibraryDir, 'relative-download.pdf'), '%PDF-1.4\n% relative test\n', 'utf8')
    const workId = seedDownloadedWork(corpusId, {
      title: 'Relative download path sentinel',
      filePath: 'relative-download.pdf',
    })

    const res = await request(app)
      .get(`/api/corpus/downloaded/${workId}/file`)
      .set('Authorization', `Bearer ${admin.token}`)
    expect(res.status).toBe(200)
    expect(res.headers['content-disposition']).toContain('relative-download.pdf')
  })
})
