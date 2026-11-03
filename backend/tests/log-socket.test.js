import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import jwt from 'jsonwebtoken'
import WebSocket from 'ws'

const authConfig = { jwtSecret: 's', adminUsername: 'admin' }
const sign = (userId, opts = {}) => jwt.sign({ sub: userId, username: 'x' }, authConfig.jwtSecret, opts)
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

describe('log WebSocket', () => {
  let tmpDir = ''
  let authDb
  let attachLogSocket
  const servers = []
  const sockets = []
  const ids = {}

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-log-socket-'))
    process.env.RAG_FEEDER_DB_PATH = path.join(tmpDir, 'app.db')
    const appModule = await import('../src/app.js')
    ;({ attachLogSocket } = await import('../src/logSocket.js'))
    authDb = new Database(path.join(tmpDir, 'auth.db'))
    appModule.ensureAuthSchema(authDb)
    const addUser = (username) =>
      Number(
        authDb
          .prepare("INSERT INTO users (username, password_hash) VALUES (?, 'x')")
          .run(username).lastInsertRowid
      )
    ids.admin = addUser('admin')
    ids.member = addUser('member')
    ids.revoked = addUser('revoked')
    ids.deleted = addUser('deleted')
    const grant = authDb.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'viewer')")
    grant.run(ids.member, 1)
    grant.run(ids.revoked, 1)
    grant.run(ids.revoked, 3)
    grant.run(ids.deleted, 1)
  })

  afterAll(async () => {
    sockets.forEach((ws) => ws.terminate())
    await Promise.all(
      servers.map(
        ({ server, logSocket }) =>
          new Promise((resolve) => {
            logSocket.close()
            server.close(() => resolve())
          })
      )
    )
    authDb?.close()
    delete process.env.RAG_FEEDER_DB_PATH
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  const start = async (opts = {}) => {
    const server = http.createServer()
    const appended = []
    const logSocket = attachLogSocket(server, {
      authDb,
      authConfig,
      isStub: () => false,
      appendLogLine: (line) => appended.push(line),
      ...opts,
    })
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
    const entry = { server, logSocket, appended, url: `ws://127.0.0.1:${server.address().port}/api/ws` }
    servers.push(entry)
    return entry
  }

  const connect = (url, protocols) => {
    const ws = protocols ? new WebSocket(url, protocols) : new WebSocket(url)
    sockets.push(ws)
    const messages = []
    ws.on('message', (data) => messages.push(String(data)))
    const closed = new Promise((resolve) => ws.on('close', (code) => resolve(code)))
    const opened = new Promise((resolve, reject) => {
      ws.on('open', resolve)
      ws.on('error', reject)
    })
    const waitFor = async (predicate) => {
      for (let i = 0; i < 200; i += 1) {
        if (messages.some(predicate)) return
        await delay(10)
      }
      throw new Error('timed out waiting for message')
    }
    return { ws, messages, closed, opened, waitFor }
  }

  const SENTINEL = (corpusId) => ({ type: 'sentinel', corpus_id: corpusId })
  const isSentinel = (m) => m.includes('"sentinel"')

  const emitMixed = (logSocket) => {
    logSocket.broadcast('[x][corpus=1] a')
    logSocket.broadcast('[x][corpus=2] b')
    logSocket.broadcast('[x][corpus=none] c')
    logSocket.broadcastEvent({ type: 't', corpus_id: 1 })
    logSocket.broadcastEvent({ type: 't', corpus_id: null })
  }

  test('no protocol/token -> close 4401 unauthorized', async () => {
    const { url } = await start()
    const client = connect(url)
    await client.opened
    expect(await client.closed).toBe(4401)
  })

  test('invalid or expired token -> close 4401', async () => {
    const { url } = await start()
    const bad = [
      jwt.sign({ sub: ids.member }, 'wrong-secret'),
      sign(ids.member, { expiresIn: -10 }),
      sign(999999),
      'not-a-jwt',
    ]
    for (const token of bad) {
      const client = connect(url, ['bearer', token])
      await client.opened
      expect(await client.closed).toBe(4401)
    }
  })

  test('member receives own-corpus lines and events only', async () => {
    const { url, logSocket, appended } = await start()
    const client = connect(url, ['bearer', sign(ids.member)])
    await client.opened
    emitMixed(logSocket)
    logSocket.broadcastEvent(SENTINEL(1))
    await client.waitFor(isSentinel)
    const received = client.messages.filter((m) => !isSentinel(m))
    expect(received).toHaveLength(2)
    expect(received[0]).toMatch(/^\[\d{4}-\d{2}-\d{2}T[^\]]+\] \[x\]\[corpus=1\] a$/)
    expect(JSON.parse(received[1])).toEqual({ type: 't', corpus_id: 1 })
    expect(appended).toHaveLength(3)
    expect(appended[2]).toMatch(/\] \[x\]\[corpus=none\] c$/)
  })

  test('admin receives everything', async () => {
    const { url, logSocket } = await start()
    const client = connect(url, ['bearer', sign(ids.admin)])
    await client.opened
    emitMixed(logSocket)
    logSocket.broadcastEvent(SENTINEL(null))
    await client.waitFor(isSentinel)
    const received = client.messages.filter((m) => !isSentinel(m))
    expect(received).toHaveLength(5)
    expect(received.slice(0, 3).map((m) => m.replace(/^\[[^\]]+\] /, ''))).toEqual([
      '[x][corpus=1] a',
      '[x][corpus=2] b',
      '[x][corpus=none] c',
    ])
  })

  test('revocation takes effect after the memo window', async () => {
    const { url, logSocket } = await start({ membershipTtlMs: 10 })
    const client = connect(url, ['bearer', sign(ids.revoked)])
    await client.opened
    logSocket.broadcast('[x][corpus=1] before')
    logSocket.broadcastEvent(SENTINEL(3))
    await client.waitFor(isSentinel)
    expect(client.messages.some((m) => m.endsWith('before'))).toBe(true)

    authDb.prepare('DELETE FROM user_corpora WHERE user_id = ? AND corpus_id = 1').run(ids.revoked)
    await delay(30)
    client.messages.length = 0
    logSocket.broadcast('[x][corpus=1] after')
    logSocket.broadcastEvent({ type: 't', corpus_id: 1 })
    logSocket.broadcastEvent(SENTINEL(3))
    await client.waitFor(isSentinel)
    expect(client.messages.filter((m) => !isSentinel(m))).toEqual([])
  })

  test('deleted user receives nothing after the memo expires', async () => {
    const { url, logSocket } = await start({ membershipTtlMs: 10 })
    const victim = connect(url, ['bearer', sign(ids.deleted)])
    const admin = connect(url, ['bearer', sign(ids.admin)])
    await Promise.all([victim.opened, admin.opened])
    logSocket.broadcast('[x][corpus=1] before')
    await victim.waitFor((m) => m.endsWith('before'))

    authDb.prepare('DELETE FROM users WHERE id = ?').run(ids.deleted)
    await delay(30)
    victim.messages.length = 0
    emitMixed(logSocket)
    logSocket.broadcastEvent(SENTINEL(null))
    await admin.waitFor(isSentinel)
    await delay(50)
    expect(victim.messages).toEqual([])
  })

  test('stub mode: no token required, everything delivered', async () => {
    const { url, logSocket } = await start({ isStub: () => true })
    const client = connect(url)
    await client.opened
    emitMixed(logSocket)
    logSocket.broadcastEvent(SENTINEL(null))
    await client.waitFor(isSentinel)
    expect(client.messages.filter((m) => !isSentinel(m))).toHaveLength(5)
    expect(client.ws.readyState).toBe(WebSocket.OPEN)
  })

  test('handleProtocols selects bearer', async () => {
    const { url } = await start()
    const client = connect(url, ['bearer', sign(ids.member)])
    await client.opened
    expect(client.ws.protocol).toBe('bearer')
    await delay(20)
    expect(client.ws.readyState).toBe(WebSocket.OPEN)
  })
})
