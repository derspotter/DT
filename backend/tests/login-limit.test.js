import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { jest } from '@jest/globals'
import bcrypt from 'bcryptjs'
import Database from 'better-sqlite3'
import request from 'supertest'

// The login route awaits getPasswordHasher().compare(). While `held.on` is
// set, each compare returns a promise of its own and pushes `{ release() }`
// into `held.list`; release() resolves that one compare with the real
// bcrypt result, independently of the others. `held.waiters` lets a test wait
// until a given number of compares are held (no wall-clock sleeps).
const held = { on: false, list: [], waiters: [] }
jest.unstable_mockModule('../src/passwordHasher.js', () => {
  const hasher = {
    async hash(password, rounds = 4) {
      return bcrypt.hash(password, rounds)
    },
    compare(password, hash) {
      if (!held.on) return bcrypt.compare(password, hash)
      return new Promise((resolve, reject) => {
        held.list.push({
          release() {
            bcrypt.compare(password, hash).then(resolve, reject)
          },
        })
        for (const waiter of [...held.waiters]) waiter()
      })
    },
    async close() {},
  }
  return { createPasswordHasher: () => hasher, getPasswordHasher: () => hasher }
})

const { createLoginLimiter, loginLimiterKey } = await import('../src/loginLimiter.js')

const LOCKED_15 = 'Too many failed sign-ins for this account. Try again in 15 minutes.'

describe('createLoginLimiter', () => {
  let t
  let limiter
  const make = (options = {}) => createLoginLimiter({ now: () => t, ...options })
  const failTimes = (lim, key, n, options) => {
    for (let i = 0; i < n; i += 1) {
      const gate = lim.reserve(key, options)
      expect(gate.allowed).toBe(true)
      lim.fail(gate.ticket)
    }
  }
  const allowedCount = (lim, key, cap = 50) => {
    let n = 0
    while (n < cap && lim.reserve(key).allowed) n += 1
    return n
  }

  beforeEach(() => {
    t = 0
    limiter = make()
  })

  test('the 11th reservation in a window is refused with the time left', () => {
    failTimes(limiter, 'u', 10)
    expect(limiter.reserve('u')).toEqual({ allowed: false, retryAfterSeconds: 900 })
    t = 899_500
    expect(limiter.reserve('u')).toEqual({ allowed: false, retryAfterSeconds: 1 })
    t = 900_000
    const gate = limiter.reserve('u')
    expect(gate.allowed).toBe(true)
    expect(gate.ticket).toEqual({ key: loginLimiterKey('u'), windowId: expect.any(Number), known: false })
  })

  test('reservations in flight count against the limit', () => {
    for (let i = 0; i < 10; i += 1) expect(limiter.reserve('u').allowed).toBe(true)
    expect(limiter.reserve('u')).toEqual({ allowed: false, retryAfterSeconds: 900 })
  })

  test('success clears settled failures but keeps reservations in flight', () => {
    failTimes(limiter, 'u', 5)
    const a = limiter.reserve('u')
    const b = limiter.reserve('u')
    const c = limiter.reserve('u')
    limiter.succeed(c.ticket)
    limiter.fail(a.ticket)
    limiter.fail(b.ticket)
    expect(allowedCount(limiter, 'u')).toBe(8)
  })

  test('refund releases the slot without counting a failure', () => {
    for (let i = 0; i < 20; i += 1) {
      const gate = limiter.reserve('u')
      expect(gate.allowed).toBe(true)
      limiter.refund(gate.ticket)
    }
    failTimes(limiter, 'u', 9)
    const gate = limiter.reserve('u')
    limiter.refund(gate.ticket)
    expect(allowedCount(limiter, 'u')).toBe(1)
  })

  test('settling a ticket from a passed window touches nothing in the new one', () => {
    const old = limiter.reserve('u')
    t = 900_000
    failTimes(limiter, 'u', 9)
    limiter.succeed(old.ticket)
    limiter.refund(old.ticket)
    limiter.fail(old.ticket)
    expect(allowedCount(limiter, 'u')).toBe(1)
  })

  test('a full unknown map evicts its oldest entry, locked or not, and never fails open', () => {
    limiter = make({ maxKeys: 2 })
    failTimes(limiter, 'a', 10)
    failTimes(limiter, 'b', 10)
    expect(limiter.reserve('a').allowed).toBe(false)
    const c = limiter.reserve('c') // evicts 'a', the oldest entry, although it is locked
    expect(c.allowed).toBe(true)
    expect(c.ticket).not.toBeNull()
    limiter.fail(c.ticket)
    expect(allowedCount(limiter, 'a', 10)).toBe(10) // 'a' starts over (and evicts 'b')
    expect(limiter.reserve('a').allowed).toBe(false)
    limiter.fail(null)
    limiter.refund(null)
    limiter.succeed(null)
  })

  test('a known name is tracked outside the cap and stays locked however many unknown names arrive', () => {
    limiter = make({ maxKeys: 2 })
    failTimes(limiter, 'alice', 10, { known: true })
    for (const junk of ['j1', 'j2', 'j3', 'j4']) failTimes(limiter, junk, 10)
    expect(limiter.reserve('alice', { known: true })).toEqual({ allowed: false, retryAfterSeconds: 900 })
    // the two maps count apart: the same name without a row has its own entry
    expect(limiter.reserve('alice').allowed).toBe(true)
    // many known names are all tracked, with no cap
    for (let i = 0; i < 5; i += 1) failTimes(limiter, `user-${i}`, 10, { known: true })
    for (let i = 0; i < 5; i += 1) expect(limiter.reserve(`user-${i}`, { known: true }).allowed).toBe(false)
  })

  test('an expired entry is deleted before the oldest one', () => {
    limiter = make({ maxKeys: 2 })
    const warm = () => limiter.refund(limiter.reserve('warm', { known: true }).ticket) // runs the sweep
    warm() // t=0: sweep
    t = 100
    failTimes(limiter, 'x', 1) // oldest by insertion
    t = 200
    failTimes(limiter, 'y', 1)
    t = 900_000
    warm() // sweep: nothing has expired yet
    t = 900_100
    failTimes(limiter, 'x', 1) // x's window passed: it starts a new one in place, no sweep due
    t = 900_200 // y's window has passed, x's has not
    expect(limiter.reserve('z').allowed).toBe(true) // must delete y, not x
    // x kept its one failure: 9 more are allowed, then refused
    expect(allowedCount(limiter, 'x')).toBe(9)
  })

  test('inserting at capacity touches O(1) entries, not the whole map (BE-40)', () => {
    const maxKeys = 10_000
    limiter = make({ maxKeys })
    for (let i = 0; i < maxKeys; i += 1) limiter.reserve(`fill-${i}`) // fresh, in flight
    expect(limiter.size().unknown).toBe(maxKeys)
    // Count every Map iterator step while new names arrive at capacity.
    let steps = 0
    const originals = {}
    for (const name of ['entries', 'keys', 'values']) {
      originals[name] = Map.prototype[name]
      Map.prototype[name] = function counted(...args) {
        const it = originals[name].apply(this, args)
        return {
          next() {
            steps += 1
            return it.next()
          },
          [Symbol.iterator]() {
            return this
          },
        }
      }
    }
    const originalIterator = Map.prototype[Symbol.iterator]
    Map.prototype[Symbol.iterator] = Map.prototype.entries
    try {
      for (let i = 0; i < 100; i += 1) expect(limiter.reserve(`new-${i}`).allowed).toBe(true)
    } finally {
      for (const name of Object.keys(originals)) Map.prototype[name] = originals[name]
      Map.prototype[Symbol.iterator] = originalIterator
    }
    expect(limiter.size().unknown).toBe(maxKeys)
    expect(steps).toBeLessThanOrEqual(100 * 2)
  })

  test('expired entries of both maps are swept without a later reserve of the same key', () => {
    failTimes(limiter, 'id:1', 3, { known: true })
    failTimes(limiter, 'stranger', 2)
    expect(limiter.size()).toEqual({ known: 1, unknown: 1 })
    t = 899_999
    limiter.refund(limiter.reserve('other').ticket)
    expect(limiter.size()).toEqual({ known: 1, unknown: 1 })
    t = 900_000
    limiter.refund(limiter.reserve('other').ticket)
    expect(limiter.size()).toEqual({ known: 0, unknown: 0 })
  })

  test('map keys are fixed-size digests', () => {
    const long = 'a'.repeat(100_000)
    expect(loginLimiterKey(long)).toHaveLength(64)
    expect(loginLimiterKey(long)).toMatch(/^[0-9a-f]+$/)
    expect(loginLimiterKey('b')).not.toBe(loginLimiterKey('B'))
    const gate = limiter.reserve(long)
    expect(gate.ticket.key).toBe(loginLimiterKey(long))
    limiter.fail(gate.ticket)
    failTimes(limiter, long, 9)
    expect(limiter.reserve(long)).toEqual({ allowed: false, retryAfterSeconds: 900 })
    expect(limiter.reserve('a'.repeat(99_999)).allowed).toBe(true)
  })
})

describe('/api/auth/login with the per-username limit', () => {
  const ADMIN = 'limit-admin'
  const ADMIN_PASSWORD = 'limit-admin-password'
  let tmpDir = ''
  let createApp
  let app
  let clock = 0

  const login = (username, password) => request(app).post('/api/auth/login').send({ username, password })
  // Starts the request now (supertest is lazy until then() is called).
  const start = (username, password) => login(username, password).then((res) => res)
  const heldReach = (n) =>
    new Promise((resolve) => {
      const check = () => {
        if (held.list.length >= n) {
          held.waiters = held.waiters.filter((w) => w !== check)
          resolve()
        }
      }
      held.waiters.push(check)
      check()
    })
  const statuses = async (n, username, password) => {
    const out = []
    for (let i = 0; i < n; i += 1) out.push((await login(username, password)).status)
    return out
  }

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-login-limit-'))
    process.env.RAG_FEEDER_DB_PATH = path.join(tmpDir, 'login-limit.db')
    process.env.RAG_FEEDER_JWT_SECRET = 'login-limit-secret'
    process.env.RAG_ADMIN_USER = ADMIN
    process.env.RAG_ADMIN_PASSWORD = ADMIN_PASSWORD
    ;({ createApp } = await import('../src/app.js'))
    createApp({ broadcast() {} }) // creates the schema
    const db = new Database(process.env.RAG_FEEDER_DB_PATH)
    try {
      const insert = db.prepare(
        `INSERT INTO users (username, password_hash, email, account_status, password_set_at)
         VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`
      )
      insert.run('b', bcrypt.hashSync('pw-b', 4), 'b@example.test', 'active')
      insert.run('c', bcrypt.hashSync('pw-b', 4), 'c@example.test', 'invited')
      // legacy rows stored with surrounding whitespace
      insert.run(' alice ', bcrypt.hashSync('pw-alice', 4), 'alice@example.test', 'active')
      insert.run('dup', bcrypt.hashSync('pw-dup', 4), 'dup@example.test', 'active')
      insert.run(' dup ', bcrypt.hashSync('pw-spaced-dup', 4), 'spaced-dup@example.test', 'active')
    } finally {
      db.close()
    }
  })

  afterAll(() => {
    for (const key of ['RAG_FEEDER_DB_PATH', 'RAG_FEEDER_JWT_SECRET', 'RAG_ADMIN_USER', 'RAG_ADMIN_PASSWORD']) {
      delete process.env[key]
    }
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  beforeEach(() => {
    clock = 0
    held.on = false
    held.list = []
    held.waiters = []
    app = createApp({ broadcast() {}, loginLimiter: createLoginLimiter({ now: () => clock }) })
  })

  afterEach(() => {
    jest.restoreAllMocks()
    held.on = false
    for (const entry of held.list) entry.release()
  })

  test('10 failed sign-ins lock the username; the 11th attempt answers 429 with Retry-After', async () => {
    expect(await statuses(10, 'b', 'wrong')).toEqual(Array(10).fill(401))
    const res = await login('b', 'wrong')
    expect(res.status).toBe(429)
    expect(res.headers['retry-after']).toBe('900')
    expect(res.body.error).toBe(LOCKED_15)
  })

  test('a locked username refuses the right password until the window passes', async () => {
    await statuses(10, 'b', 'wrong')
    expect((await login('b', 'pw-b')).status).toBe(429)
    clock += 899_000
    const late = await login('b', 'pw-b')
    expect(late.status).toBe(429)
    expect(late.headers['retry-after']).toBe('1')
    expect(late.body.error).toBe('Too many failed sign-ins for this account. Try again in 1 minute.')
    clock += 1_000
    expect((await login('b', 'pw-b')).status).toBe(200)
  })

  test('a successful sign-in clears the count', async () => {
    expect(await statuses(9, 'b', 'wrong')).toEqual(Array(9).fill(401))
    expect((await login('b', 'pw-b')).status).toBe(200)
    expect(await statuses(9, 'b', 'wrong')).toEqual(Array(9).fill(401))
  })

  test('concurrent wrong passwords get at most 10 checks', async () => {
    const results = await Promise.all(Array.from({ length: 30 }, () => login('b', 'wrong')))
    const codes = results.map((r) => r.status)
    expect(codes.filter((s) => s === 401)).toHaveLength(10)
    expect(codes.filter((s) => s === 429)).toHaveLength(20)
  })

  test('active, invited and unknown names get the same 429 after 10 failures', async () => {
    expect(await statuses(10, 'b', 'wrong')).toEqual(Array(10).fill(401))
    expect(await statuses(10, 'c', 'pw-b')).toEqual(Array(10).fill(403))
    expect(await statuses(10, 'nobody', 'x')).toEqual(Array(10).fill(401))
    const locked = [await login('b', 'wrong'), await login('c', 'pw-b'), await login('nobody', 'x')]
    expect(locked.map((r) => r.status)).toEqual([429, 429, 429])
    expect(new Set(locked.map((r) => r.body.error))).toEqual(new Set([LOCKED_15]))
    expect(new Set(locked.map((r) => r.headers['retry-after']))).toEqual(new Set(['900']))
    expect((await login(' b ', 'pw-b')).status).toBe(429) // the trimmed key is 'b'
  })

  test('other usernames are not affected', async () => {
    await statuses(10, 'b', 'wrong')
    expect((await login('b', 'pw-b')).status).toBe(429)
    expect((await login(ADMIN, ADMIN_PASSWORD)).status).toBe(200)
  })

  test('a failed user lookup takes no slot', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    jest.spyOn(app.authDb, 'prepare').mockImplementationOnce(() => {
      throw new Error('boom')
    })
    const failed = await login('b', 'wrong')
    expect(failed.status).toBe(500)
    expect(failed.body.error).toBe('Login failed')
    errorSpy.mockRestore()
    expect(await statuses(10, 'b', 'wrong')).toEqual(Array(10).fill(401))
    expect((await login('b', 'wrong')).status).toBe(429)
    clock += 900_000
    expect((await login('b', 'pw-b')).status).toBe(200)
  })

  test('a username must be a non-empty string, and the lookup uses the trimmed name', async () => {
    const cases = [
      { username: ['b'], password: 'x' },
      { username: 'b', password: 1 },
      { username: '   ', password: 'x' },
    ]
    for (const body of cases) {
      const res = await request(app).post('/api/auth/login').send(body)
      expect(res.status).toBe(400)
      expect(res.body.error).toBe('Missing username or password')
    }
    const ok = await login(' b ', 'pw-b')
    expect(ok.status).toBe(200)
    expect(ok.body.user.username).toBe('b')

    // an empty password is a string: it is checked, answers 401 and counts
    const empty = await login('b', '')
    expect(empty.status).toBe(401)
    expect(empty.body.error).toBe('Invalid credentials')
    expect(await statuses(9, 'b', 'wrong')).toEqual(Array(9).fill(401))
    expect((await login('b', 'wrong')).status).toBe(429)
  })

  test('a legacy name stored with whitespace signs in exactly; the exact row wins over the trimmed one', async () => {
    const legacy = await login(' alice ', 'pw-alice')
    expect(legacy.status).toBe(200)
    expect(legacy.body.user.username).toBe(' alice ')
    expect((await login('alice', 'pw-alice')).status).toBe(401) // no 'alice' row

    const spaced = await login(' dup ', 'pw-spaced-dup')
    expect(spaced.status).toBe(200)
    expect(spaced.body.user.username).toBe(' dup ')
    const plain = await login('dup', 'pw-dup')
    expect(plain.status).toBe(200)
    expect(plain.body.user.username).toBe('dup')
    expect((await login(' dup ', 'pw-dup')).status).toBe(401) // the exact row is checked, not 'dup'
  })

  test('two accounts that differ only by whitespace keep separate counters', async () => {
    expect(await statuses(9, 'dup', 'wrong')).toEqual(Array(9).fill(401))
    expect((await login(' dup ', 'pw-spaced-dup')).status).toBe(200) // does not clear 'dup'
    expect((await login('dup', 'wrong')).status).toBe(401) // the 10th for 'dup'
    expect((await login('dup', 'pw-dup')).status).toBe(429)
    expect((await login(' dup ', 'pw-spaced-dup')).status).toBe(200) // not locked by 'dup'
  })

  test('with the unknown map full of locked junk names, existing accounts still lock after 10 failures', async () => {
    app = createApp({ broadcast() {}, loginLimiter: createLoginLimiter({ now: () => clock, maxKeys: 2 }) })
    for (const junk of ['junk-1', 'junk-2']) {
      expect(await statuses(10, junk, 'x')).toEqual(Array(10).fill(401))
      expect((await login(junk, 'x')).status).toBe(429)
    }
    // a third unknown name evicts the oldest lock (the accepted residual)
    expect(await statuses(10, 'junk-3', 'x')).toEqual(Array(10).fill(401))
    expect((await login('junk-3', 'x')).status).toBe(429)
    expect((await login('junk-1', 'x')).status).toBe(401)

    expect(await statuses(10, 'b', 'wrong')).toEqual(Array(10).fill(401))
    expect(await statuses(10, 'c', 'pw-b')).toEqual(Array(10).fill(403))
    for (const junk of ['junk-4', 'junk-5', 'junk-6']) await statuses(10, junk, 'x')
    const locked = [await login('b', 'pw-b'), await login('c', 'pw-b'), await login('junk-6', 'x')]
    expect(locked.map((r) => r.status)).toEqual([429, 429, 429])
    expect(new Set(locked.map((r) => r.body.error))).toEqual(new Set([LOCKED_15]))
    expect(new Set(locked.map((r) => r.headers['retry-after']))).toEqual(new Set(['900']))
  })

  describe('deferred compares', () => {
    test('a late success clears settled failures but keeps in-flight reservations', async () => {
      expect(await statuses(3, 'b', 'wrong')).toEqual([401, 401, 401])
      held.on = true
      const wrong1 = start('b', 'wrong')
      await heldReach(1)
      const wrong2 = start('b', 'wrong')
      await heldReach(2)
      const right = start('b', 'pw-b')
      await heldReach(3)
      held.on = false
      held.list[2].release()
      expect((await right).status).toBe(200)
      held.list[0].release()
      held.list[1].release()
      expect((await wrong1).status).toBe(401)
      expect((await wrong2).status).toBe(401)
      expect(await statuses(8, 'b', 'wrong')).toEqual(Array(8).fill(401))
      expect((await login('b', 'wrong')).status).toBe(429)
    })

    test('a late rejection after a rollover touches neither counter of the new window', async () => {
      held.on = true
      const oldWrong = start('b', 'wrong')
      await heldReach(1)
      clock = 900_000
      const newWrong = start('b', 'wrong')
      await heldReach(2)
      const newRight = start('b', 'pw-b')
      await heldReach(3)
      held.on = false
      held.list[0].release()
      expect((await oldWrong).status).toBe(401)
      held.list[2].release()
      expect((await newRight).status).toBe(200)
      // the new window holds one reservation (newWrong) and no failures
      expect(await statuses(9, 'b', 'wrong')).toEqual(Array(9).fill(401))
      expect((await login('b', 'wrong')).status).toBe(429)
      held.list[1].release()
      expect((await newWrong).status).toBe(401)
    })

    test('a late success after a rollover does not clear the new window', async () => {
      expect(await statuses(5, 'b', 'wrong')).toEqual(Array(5).fill(401))
      held.on = true
      const oldRight = start('b', 'pw-b')
      await heldReach(1)
      held.on = false
      clock = 900_000
      expect(await statuses(9, 'b', 'wrong')).toEqual(Array(9).fill(401))
      held.list[0].release()
      expect((await oldRight).status).toBe(200)
      expect((await login('b', 'wrong')).status).toBe(401)
      expect((await login('b', 'wrong')).status).toBe(429)
    })
  })
})
