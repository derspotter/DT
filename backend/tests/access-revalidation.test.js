import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'

describe('requireAuth re-validates corpus membership', () => {
  let app
  let dbPath = ''
  let tmpDir = ''
  const adminUsername = 'revalidation-admin'
  const adminPassword = 'revalidation-admin-password'

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-access-revalidation-'))
    dbPath = path.join(tmpDir, 'revalidation.db')
    process.env.RAG_FEEDER_DB_PATH = dbPath
    process.env.RAG_FEEDER_JWT_SECRET = 'revalidation-secret'
    process.env.RAG_ADMIN_USER = adminUsername
    process.env.RAG_ADMIN_PASSWORD = adminPassword

    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast: () => {} })
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_DB_PATH
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
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

  const insertUser = (db, username, password) =>
    Number(
      db
        .prepare(
          `INSERT INTO users (username, password_hash, email, account_status, password_set_at)
           VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP)`
        )
        .run(username, bcrypt.hashSync(password, 10), `${username}@example.test`).lastInsertRowid
    )

  const createActiveUser = (username, password) =>
    withDb((db) => {
      const userId = insertUser(db, username, password)
      const corpusId = Number(
        db.prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)').run(`${username}'s corpus`, userId)
          .lastInsertRowid
      )
      db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')").run(userId, corpusId)
      db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpusId, userId)
      return { userId, corpusId }
    })

  const seedWork = (corpusId, title, { downloadStatus = 'not_requested', filePath = null } = {}) =>
    withDb((db) => {
      const workId = Number(
        db
          .prepare(
            `INSERT INTO works (title, normalized_title, authors, year, metadata_status, download_status, file_path)
             VALUES (?, ?, 'Revalidation Author', 2026, 'matched', ?, ?)`
          )
          .run(title, title.toLowerCase(), downloadStatus, filePath).lastInsertRowid
      )
      db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)').run(corpusId, workId)
      return workId
    })

  const corpusReadRoutes = ['/api/corpus', '/api/downloads', '/api/ingest/runs', '/api/ingest/stats']

  const revoke = (userId, corpusId) =>
    withDb((db) => db.prepare('DELETE FROM user_corpora WHERE user_id = ? AND corpus_id = ?').run(userId, corpusId))

  // owner1 owns corpus A; member2 owns corpus B and is a viewer on A.
  const setupSharedCorpus = async (suffix) => {
    const owner = createActiveUser(`owner1-${suffix}`, 'owner1-password')
    const member = createActiveUser(`member2-${suffix}`, 'member2-password')
    withDb((db) =>
      db
        .prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'viewer')")
        .run(member.userId, owner.corpusId)
    )
    const memberLogin = await login(`member2-${suffix}`, 'member2-password')
    const select = await request(app)
      .post(`/api/corpora/${owner.corpusId}/select`)
      .set('Authorization', `Bearer ${memberLogin.token}`)
    expect(select.status).toBe(200)
    return { corpusA: owner.corpusId, corpusB: member.corpusId, memberId: member.userId, memberToken: memberLogin.token }
  }

  test('a revoked member no longer reads the corpus through last_corpus_id', async () => {
    const { corpusA, corpusB, memberId, memberToken } = await setupSharedCorpus('revoked')
    const workA = seedWork(corpusA, 'Revoked corpus A sentinel')
    const workB = seedWork(corpusB, 'Own corpus B sentinel')

    revoke(memberId, corpusA)

    const corpusRes = await request(app).get('/api/corpus').set('Authorization', `Bearer ${memberToken}`)
    expect(corpusRes.status).toBe(200)
    const ids = corpusRes.body.items.map((item) => Number(item.id))
    expect(ids).toContain(workB)
    expect(ids).not.toContain(workA)

    const me = await request(app).get('/api/auth/me').set('Authorization', `Bearer ${memberToken}`)
    expect(me.status).toBe(200)
    expect(Number(me.body.user.last_corpus_id)).toBe(corpusB)
    const stored = withDb((db) => db.prepare('SELECT last_corpus_id FROM users WHERE id = ?').get(memberId))
    expect(Number(stored.last_corpus_id)).toBe(corpusB)
  })

  test('with no memberships left, corpusId is null and reads do not 500', async () => {
    const { corpusA, corpusB, memberId, memberToken } = await setupSharedCorpus('empty')
    seedWork(corpusA, 'Empty-case corpus A sentinel')
    seedWork(corpusB, 'Empty-case corpus B sentinel')

    revoke(memberId, corpusA)
    revoke(memberId, corpusB)

    // A null corpusId must not fall through to the unscoped all-corpora readers.
    for (const route of corpusReadRoutes) {
      const res = await request(app).get(route).set('Authorization', `Bearer ${memberToken}`)
      expect([route, res.status, res.body]).toEqual([route, 403, { error: 'Select a corpus first' }])
    }

    const stored = withDb((db) => db.prepare('SELECT last_corpus_id FROM users WHERE id = ?').get(memberId))
    expect(stored.last_corpus_id).toBeNull()
  })

  test('a member keeps reading their own corpus on every guarded route', async () => {
    const { corpusB, memberId, memberToken } = await setupSharedCorpus('own')
    withDb((db) => db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpusB, memberId))

    for (const route of corpusReadRoutes) {
      const res = await request(app).get(route).set('Authorization', `Bearer ${memberToken}`)
      expect([route, res.status]).toEqual([route, 200])
    }
  })

  test('admin with last_corpus_id NULL still reads /api/corpus', async () => {
    const admin = await login(adminUsername, adminPassword)
    withDb((db) => db.prepare('UPDATE users SET last_corpus_id = NULL WHERE id = ?').run(admin.user.id))

    const res = await request(app).get('/api/corpus').set('Authorization', `Bearer ${admin.token}`)
    expect(res.status).toBe(200)
  })

  test('admin keeps a last_corpus_id they are not a member of', async () => {
    const { corpusB } = await setupSharedCorpus('admin')
    const admin = await login(adminUsername, adminPassword)
    withDb((db) => db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpusB, admin.user.id))

    const me = await request(app).get('/api/auth/me').set('Authorization', `Bearer ${admin.token}`)
    expect(me.status).toBe(200)
    expect(Number(me.body.user.last_corpus_id)).toBe(corpusB)
  })

  test('login no longer hands a membership-less user the default corpus', async () => {
    withDb((db) => insertUser(db, 'no-corpus-user', 'no-corpus-password'))

    const res = await login('no-corpus-user', 'no-corpus-password')
    expect(res.user.last_corpus_id).toBeNull()
  })

  test('a download ticket minted for A fails after revocation', async () => {
    const { corpusA, memberId, memberToken } = await setupSharedCorpus('ticket')
    const filePath = path.join(tmpDir, 'ticket-download.pdf')
    fs.writeFileSync(filePath, '%PDF-1.4\n% ticket test\n', 'utf8')
    const workId = seedWork(corpusA, 'Ticket corpus A sentinel', { downloadStatus: 'downloaded', filePath })

    const ticketRes = await request(app)
      .post(`/api/corpus/downloaded/${workId}/download-ticket`)
      .set('Authorization', `Bearer ${memberToken}`)
    expect(ticketRes.status).toBe(200)

    revoke(memberId, corpusA)

    const fileRes = await request(app).get(ticketRes.body.url)
    expect(fileRes.status).toBe(403)
    expect(fileRes.body).toEqual({ error: 'No access to this corpus' })
  })
})
