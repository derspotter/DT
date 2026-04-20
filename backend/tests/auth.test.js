import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import request from 'supertest'

describe('auth hardening', () => {
  let createApp
  let tmpDir = ''

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-auth-test-'))
    process.env.RAG_FEEDER_DB_PATH = path.join(tmpDir, 'auth.db')
    ;({ createApp } = await import('../src/app.js'))
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_DB_PATH
    delete process.env.RAG_FEEDER_STUB
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  afterEach(() => {
    delete process.env.RAG_FEEDER_STUB
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
    delete process.env.RAG_FEEDER_SMTP_HOST
    delete process.env.RAG_FEEDER_SMTP_PORT
    delete process.env.RAG_FEEDER_SMTP_FROM
  })

  test('fails fast when required auth env vars are missing', () => {
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
    expect(() => createApp({ broadcast: () => {} })).toThrow(/Missing required environment variables/)
  })

  test('allows login with configured bootstrap credentials', async () => {
    process.env.RAG_FEEDER_JWT_SECRET = 'auth-test-secret'
    process.env.RAG_ADMIN_USER = 'auth-test-admin'
    process.env.RAG_ADMIN_PASSWORD = 'auth-test-password'

    const app = createApp({ broadcast: () => {} })
    const res = await request(app).post('/api/auth/login').send({
      username: process.env.RAG_ADMIN_USER,
      password: process.env.RAG_ADMIN_PASSWORD,
    })

    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('token')
    expect(res.body.user?.username).toBe(process.env.RAG_ADMIN_USER)
  })

  test('disables public registration outside stub mode', async () => {
    process.env.RAG_FEEDER_JWT_SECRET = 'auth-test-secret'
    process.env.RAG_ADMIN_USER = 'auth-test-admin'
    process.env.RAG_ADMIN_PASSWORD = 'auth-test-password'

    const app = createApp({ broadcast: () => {} })
    const res = await request(app).post('/api/auth/register').send({
      username: 'new-user',
      password: 'new-password',
    })

    expect(res.status).toBe(404)
  })

  test('cleans up invited user if invitation delivery fails', async () => {
    process.env.RAG_FEEDER_JWT_SECRET = 'auth-test-secret'
    process.env.RAG_ADMIN_USER = 'auth-test-admin'
    process.env.RAG_ADMIN_PASSWORD = 'auth-test-password'
    process.env.RAG_FEEDER_SMTP_HOST = '127.0.0.1'
    process.env.RAG_FEEDER_SMTP_PORT = '1'
    process.env.RAG_FEEDER_SMTP_FROM = 'noreply@example.com'

    const app = createApp({ broadcast: () => {} })
    const login = await request(app).post('/api/auth/login').send({
      username: process.env.RAG_ADMIN_USER,
      password: process.env.RAG_ADMIN_PASSWORD,
    })
    const token = login.body?.token

    const inviteRes = await request(app)
      .post('/api/auth/invitations')
      .set('Authorization', `Bearer ${token}`)
      .send({ username: 'invite-fail-user', email: 'invite-fail@example.com' })

    expect(inviteRes.status).toBe(500)

    const db = new Database(process.env.RAG_FEEDER_DB_PATH)
    try {
      const user = db.prepare('SELECT id FROM users WHERE username = ?').get('invite-fail-user')
      const emailUser = db.prepare('SELECT id FROM users WHERE email = ?').get('invite-fail@example.com')
      expect(user).toBeUndefined()
      expect(emailUser).toBeUndefined()
    } finally {
      db.close()
    }
  })

  test('fails fast when legacy library tables contain rows but works is empty', () => {
    process.env.RAG_FEEDER_JWT_SECRET = 'auth-test-secret'
    process.env.RAG_ADMIN_USER = 'auth-test-admin'
    process.env.RAG_ADMIN_PASSWORD = 'auth-test-password'

    const db = new Database(process.env.RAG_FEEDER_DB_PATH)
    try {
      db.exec(`
        DROP TABLE IF EXISTS works;
        CREATE TABLE IF NOT EXISTS with_metadata (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          title TEXT
        );
      `)
      db.prepare('INSERT INTO with_metadata (title) VALUES (?)').run('Legacy row')
    } finally {
      db.close()
    }

    expect(() => createApp({ broadcast: () => {} })).toThrow(/Legacy library data detected/)
  })
})
