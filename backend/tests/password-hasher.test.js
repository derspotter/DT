import { execFile } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import bcrypt from 'bcryptjs'
import { createPasswordHasher, getPasswordHasher } from '../src/passwordHasher.js'

const HASHER_URL = pathToFileURL(path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'passwordHasher.js')).href

const hashers = []
const make = (options) => {
  const hasher = createPasswordHasher(options)
  hashers.push(hasher)
  return hasher
}

afterAll(async () => {
  await Promise.all(hashers.map((hasher) => hasher.close()))
  await getPasswordHasher().close()
})

test('hashes are bcryptjs-compatible both ways', async () => {
  const hasher = make()
  const h = await hasher.hash('pw')
  expect(h.startsWith('$2b$10$')).toBe(true)
  expect(bcrypt.compareSync('pw', h)).toBe(true)
  expect(await hasher.compare('pw', bcrypt.hashSync('pw', 10))).toBe(true)
  expect(await hasher.compare('other', h)).toBe(false)
})

test('the main thread keeps running while hashes run', async () => {
  const hasher = make()
  const ticks = []
  let last = Date.now()
  const timer = setInterval(() => {
    const now = Date.now()
    ticks.push(now - last)
    last = now
  }, 10)
  try {
    await Promise.all([1, 2, 3, 4].map(() => hasher.hash('pw', 12)))
  } finally {
    clearInterval(timer)
  }
  expect(ticks.length).toBeGreaterThanOrEqual(10)
  // A synchronous rounds-12 hash blocks the loop for ~250 ms.
  expect(Math.max(...ticks)).toBeLessThan(60)
})

test('at most size hashes run at once and the queue bound holds', async () => {
  const hasher = make({ size: 1, maxQueue: 2 })
  const first = [1, 2, 3].map(() => hasher.hash('pw', 12))
  const startedAt = Date.now()
  await expect(hasher.hash('pw', 12)).rejects.toMatchObject({ code: 'HASHER_BUSY' })
  // The rejection is immediate, not after a queue slot frees up.
  expect(Date.now() - startedAt).toBeLessThan(100)
  const hashes = await Promise.all(first)
  for (const h of hashes) expect(bcrypt.compareSync('pw', h)).toBe(true)
  // The queue drained, so new work is accepted again.
  expect(bcrypt.compareSync('pw', await hasher.hash('pw', 4))).toBe(true)
})

test('a dead worker is replaced and its call rejects', async () => {
  const hasher = make({ size: 1 })
  const pending = hasher.hash('pw', 14)
  const queued = hasher.hash('queued', 4)
  const [worker] = hasher.workers()
  expect(worker).toBeDefined()
  await worker.terminate()
  await expect(pending).rejects.toThrow(/password worker/)
  // The queued call runs on the replacement worker.
  expect(bcrypt.compareSync('queued', await queued)).toBe(true)
  expect(await hasher.compare('pw', bcrypt.hashSync('pw', 4))).toBe(true)
  expect(hasher.workers()).toHaveLength(1)
  expect(hasher.workers()[0]).not.toBe(worker)
})

test('close rejects queued work and later calls', async () => {
  const hasher = make({ size: 1 })
  const running = hasher.hash('pw', 12)
  const queued = hasher.hash('pw', 4)
  running.catch(() => {})
  queued.catch(() => {})
  await hasher.close()
  await expect(queued).rejects.toThrow(/closed/)
  await expect(running).rejects.toThrow()
  await expect(hasher.hash('pw')).rejects.toThrow(/closed/)
})

test('getPasswordHasher returns one shared instance', () => {
  expect(getPasswordHasher()).toBe(getPasswordHasher())
})

test('the shared pool finishes a pending hash and then lets the process exit', async () => {
  // A ref'd idle worker would hang this child; an unref'd busy one would drop the hash.
  const script = `import { getPasswordHasher } from ${JSON.stringify(HASHER_URL)}
const h = await getPasswordHasher().hash('pw', 4)
console.log(h.slice(0, 7))`
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-hasher-exit-'))
  const file = path.join(dir, 'hash-once.mjs')
  fs.writeFileSync(file, script)
  try {
    const { code, stdout, stderr } = await new Promise((resolve) => {
      execFile(process.execPath, [file], { timeout: 10_000 }, (error, out, err) => {
        resolve({ code: error ? (error.code ?? error.signal) : 0, stdout: out, stderr: err })
      })
    })
    expect({ code, stderr }).toEqual({ code: 0, stderr: '' })
    expect(stdout.trim()).toBe('$2b$04$')
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
}, 15_000)
