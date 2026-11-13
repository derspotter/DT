import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { jest } from '@jest/globals'
import bcrypt from 'bcryptjs'
import Database from 'better-sqlite3'
import request from 'supertest'

// The routes await getPasswordHasher(). This stand-in hashes with bcryptjs's
// async API (which yields between rounds, so two requests really interleave at
// the await) and can be switched to answer HASHER_BUSY.
const hasherState = { busy: false }
jest.unstable_mockModule('../src/passwordHasher.js', () => {
  const busy = () => Object.assign(new Error('Password hasher queue is full'), { code: 'HASHER_BUSY' })
  const hasher = {
    async hash(password, rounds = 10) {
      if (hasherState.busy) throw busy()
      return bcrypt.hash(password, rounds)
    },
    async compare(password, hash) {
      if (hasherState.busy) throw busy()
      return bcrypt.compare(password, hash)
    },
    async close() {},
  }
  return { createPasswordHasher: () => hasher, getPasswordHasher: () => hasher }
})

// app.js reads RAG_FEEDER_DB_PATH once, at import, so both describes share one
// DB. The route tests run first: the legacy-data test at the end poisons it.
let sharedTmpDir = ''
let sharedDbPath = ''
let createApp
beforeAll(async () => {
  sharedTmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-auth-test-'))
  sharedDbPath = path.join(sharedTmpDir, 'auth.db')
  process.env.RAG_FEEDER_DB_PATH = sharedDbPath
  ;({ createApp } = await import('../src/app.js'))
})
afterAll(() => {
  delete process.env.RAG_FEEDER_DB_PATH
  if (sharedTmpDir && fs.existsSync(sharedTmpDir)) {
    fs.rmSync(sharedTmpDir, { recursive: true, force: true })
  }
})

describe('auth routes: hashing, one-time tokens, mails and transactions', () => {
  let app
  let sent = []
  const mailTransport = {
    async sendMail(options) {
      sent.push(options)
      return { messageId: `test-${sent.length}` }
    },
  }
  const ADMIN = 'routes-admin'
  const ADMIN_PASSWORD = 'routes-admin-password'

  const setEnv = (password = ADMIN_PASSWORD) => {
    process.env.RAG_FEEDER_JWT_SECRET = 'auth-routes-secret'
    process.env.RAG_ADMIN_USER = ADMIN
    process.env.RAG_ADMIN_PASSWORD = password
  }
  const withDb = (fn) => {
    const db = new Database(sharedDbPath)
    try {
      return fn(db)
    } finally {
      db.close()
    }
  }
  const login = (username, password) => request(app).post('/api/auth/login').send({ username, password })
  const adminToken = async () => {
    const res = await login(ADMIN, ADMIN_PASSWORD)
    expect(res.status).toBe(200)
    return res.body.token
  }
  const tokenFromMail = (mail) => {
    const match = String(mail.html).match(/token=([^"&]+)/)
    expect(match).not.toBeNull()
    return decodeURIComponent(match[1])
  }
  const invite = async (username, email) => {
    const token = await adminToken()
    sent = []
    const res = await request(app)
      .post('/api/auth/invitations')
      .set('Authorization', `Bearer ${token}`)
      .send({ username, email })
    return res
  }

  beforeEach(() => {
    setEnv()
    sent = []
    hasherState.busy = false
    app = createApp({ broadcast() {}, mailTransport })
  })

  afterEach(() => {
    hasherState.busy = false
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
  })

  test('login and invite answer 503 when the hasher queue is full', async () => {
    const token = await adminToken()
    hasherState.busy = true
    const res = await login(ADMIN, ADMIN_PASSWORD)
    expect(res.status).toBe(503)
    expect(res.headers['retry-after']).toBe('5')
    expect(res.body.error).toBe('The server is busy; try again in a moment.')

    const inv = await request(app)
      .post('/api/auth/invitations')
      .set('Authorization', `Bearer ${token}`)
      .send({ username: 'busy-user', email: 'busy-user@example.test' })
    expect(inv.status).toBe(503)
    expect(inv.headers['retry-after']).toBe('5')
    expect(inv.body.error).toBe('The server is busy; try again in a moment.')
    expect(withDb((db) => db.prepare('SELECT id FROM users WHERE username = ?').get('busy-user'))).toBeUndefined()
  })

  test('two concurrent accept-invite submissions: exactly one succeeds', async () => {
    const inv = await invite('race-invitee', 'race-invitee@example.test')
    expect(inv.status).toBe(201)
    expect(sent).toHaveLength(1)
    const token = tokenFromMail(sent[0])
    const passwords = ['first-pass', 'second-pass']
    const results = await Promise.all(
      passwords.map((password) => request(app).post('/api/auth/accept-invite').send({ token, password }))
    )
    expect(results.map((r) => r.status).sort()).toEqual([200, 400])
    const loser = results.find((r) => r.status === 400)
    expect(loser.body.error).toBe('Invite link is invalid or expired')
    const winnerPassword = passwords[results.findIndex((r) => r.status === 200)]
    const loserPassword = passwords.find((p) => p !== winnerPassword)
    expect((await login('race-invitee', winnerPassword)).status).toBe(200)
    expect((await login('race-invitee', loserPassword)).status).toBe(401)
  })

  test('two concurrent reset-password submissions: exactly one succeeds', async () => {
    const inv = await invite('race-reset', 'race-reset@example.test')
    expect(inv.status).toBe(201)
    const accepted = await request(app)
      .post('/api/auth/accept-invite')
      .send({ token: tokenFromMail(sent[0]), password: 'initial-pass' })
    expect(accepted.status).toBe(200)

    sent = []
    const forgot = await request(app).post('/api/auth/forgot-password').send({ email: 'race-reset@example.test' })
    expect(forgot.status).toBe(200)
    expect(sent).toHaveLength(1)
    const token = tokenFromMail(sent[0])
    const passwords = ['first-pass', 'second-pass']
    const results = await Promise.all(
      passwords.map((password) => request(app).post('/api/auth/reset-password').send({ token, password }))
    )
    expect(results.map((r) => r.status).sort()).toEqual([200, 400])
    expect(results.find((r) => r.status === 400).body.error).toBe('Reset link is invalid or expired')
    const winnerPassword = passwords[results.findIndex((r) => r.status === 200)]
    const loserPassword = passwords.find((p) => p !== winnerPassword)
    expect((await login('race-reset', winnerPassword)).status).toBe(200)
    expect((await login('race-reset', loserPassword)).status).toBe(401)
    expect((await login('race-reset', 'initial-pass')).status).toBe(401)
  })

  test('the invitation and forgot-password routes send escaped HTML', async () => {
    const inv = await invite('<b>x</b>', 'escaped-x@example.test')
    expect(inv.status).toBe(201)
    expect(sent).toHaveLength(1)
    expect(sent[0].html).toContain('&lt;b&gt;x&lt;/b&gt;')
    expect(sent[0].html).not.toContain('<b>x</b>')
    expect(sent[0].subject).toBe('Set your Korpus Builder password')

    withDb((db) => {
      db.prepare(
        `INSERT INTO users (username, password_hash, email, account_status)
         VALUES (?, ?, ?, 'active')`
      ).run('<i>y</i>', bcrypt.hashSync('y-password', 4), 'escaped-y@example.test')
    })
    sent = []
    const forgot = await request(app).post('/api/auth/forgot-password').send({ email: 'escaped-y@example.test' })
    expect(forgot.status).toBe(200)
    expect(sent).toHaveLength(1)
    expect(sent[0].subject).toBe('Reset your Korpus Builder password')
    expect(sent[0].html).toContain('&lt;i&gt;y&lt;/i&gt;')
    expect(sent[0].html).not.toContain('<i>y</i>')
  })

  test('boot does not rewrite a matching admin hash', async () => {
    const adminHash = () =>
      withDb((db) => db.prepare('SELECT password_hash FROM users WHERE username = ?').get(ADMIN).password_hash)
    const before = adminHash()
    createApp({ broadcast() {}, mailTransport })
    createApp({ broadcast() {}, mailTransport })
    expect(adminHash()).toBe(before)
    expect((await login(ADMIN, ADMIN_PASSWORD)).status).toBe(200)

    setEnv('changed-admin-password')
    app = createApp({ broadcast() {}, mailTransport })
    const after = adminHash()
    expect(after).not.toBe(before)
    expect((await login(ADMIN, 'changed-admin-password')).status).toBe(200)
    expect((await login(ADMIN, ADMIN_PASSWORD)).status).toBe(401)

    // Restore the documented password for the other tests.
    setEnv()
    app = createApp({ broadcast() {}, mailTransport })
    expect((await login(ADMIN, ADMIN_PASSWORD)).status).toBe(200)
  })

  test('a failing membership insert leaves no corpus and no user behind', async () => {
    const token = await adminToken()
    withDb((db) => db.exec("CREATE TRIGGER boom BEFORE INSERT ON user_corpora BEGIN SELECT RAISE(ABORT, 'boom'); END"))
    try {
      const corpusRes = await request(app)
        .post('/api/corpora')
        .set('Authorization', `Bearer ${token}`)
        .send({ name: 'X' })
      expect(corpusRes.status).toBe(500)
      expect(corpusRes.body.error).toMatch(/boom/)
      expect(withDb((db) => db.prepare("SELECT COUNT(*) AS c FROM corpora WHERE name = 'X'").get().c)).toBe(0)

      const inv = await request(app)
        .post('/api/auth/invitations')
        .set('Authorization', `Bearer ${token}`)
        .send({ username: 'u-x', email: 'u-x@example.test' })
      expect(inv.status).toBe(500)
      expect(inv.body.error).toMatch(/boom/)
      withDb((db) => {
        expect(db.prepare("SELECT id FROM users WHERE username = 'u-x'").get()).toBeUndefined()
        expect(
          db.prepare("SELECT COUNT(*) AS c FROM corpora WHERE name LIKE '%u-x%' OR owner_user_id NOT IN (SELECT id FROM users)").get().c
        ).toBe(0)
      })
      expect(sent).toHaveLength(0)
    } finally {
      withDb((db) => db.exec('DROP TRIGGER IF EXISTS boom'))
    }
  })
})

describe('auth hardening', () => {
  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
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
