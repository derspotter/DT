import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
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
  })

  test('fails fast when required auth env vars are missing', () => {
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
})
