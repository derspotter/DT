import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'

describe('graph routes are scoped to readable corpora', () => {
  let app
  let appModule
  let dbPath = ''
  let tmpDir = ''
  let snapshotDir = ''
  const adminUsername = 'graph-access-admin'
  const adminPassword = 'graph-access-admin-password'
  const fx = {}

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
    return res.body.token
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

  const insertCorpus = (db, name, ownerId) => {
    const corpusId = Number(
      db.prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)').run(name, ownerId).lastInsertRowid
    )
    db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')").run(ownerId, corpusId)
    db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpusId, ownerId)
    return corpusId
  }

  const insertWork = (db, title, corpusId) => {
    const workId = Number(
      db
        .prepare(
          `INSERT INTO works (title, normalized_title, authors, year, metadata_status, download_status)
           VALUES (?, ?, 'Graph Author', 2026, 'matched', 'downloaded')`
        )
        .run(title, title.toLowerCase()).lastInsertRowid
    )
    if (corpusId) db.prepare('INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)').run(corpusId, workId)
    return workId
  }

  // Pre-creates a snapshot dir so the routes serve it instead of spawning a build.
  const writeSnapshot = (key, scope) => {
    const dir = path.join(snapshotDir, key)
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(
      path.join(dir, 'manifest.json'),
      JSON.stringify({ schema_version: 2, generated_at: `gen-${key}`, files: {} })
    )
    fs.writeFileSync(path.join(dir, 'clusters.json'), JSON.stringify([{ id: 1, label: `cluster-${key}` }]))
    if (scope !== undefined) fs.writeFileSync(path.join(dir, 'scope.json'), JSON.stringify(scope))
  }

  const snapshotKeyFor = (query) =>
    appModule.graph3dSnapshotKey(appModule.buildGraph3dRequest({ query }))

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-graph-access-'))
    dbPath = path.join(tmpDir, 'graph-access.db')
    snapshotDir = path.join(tmpDir, 'snapshots')
    process.env.RAG_FEEDER_DB_PATH = dbPath
    process.env.RAG_FEEDER_GRAPH_3D_SNAPSHOT_DIR = snapshotDir
    process.env.RAG_FEEDER_JWT_SECRET = 'graph-access-secret'
    process.env.RAG_ADMIN_USER = adminUsername
    process.env.RAG_ADMIN_PASSWORD = adminPassword

    appModule = await import('../src/app.js')
    app = appModule.createApp({ broadcast: () => {} })

    withDb((db) => {
      const ownerA = insertUser(db, 'graph-owner-a', 'owner-a-password')
      const ownerB = insertUser(db, 'graph-owner-b', 'owner-b-password')
      const viewer3 = insertUser(db, 'graph-viewer3', 'viewer3-password')
      insertUser(db, 'graph-lonely', 'lonely-password')
      fx.corpusA = insertCorpus(db, 'Graph corpus A', ownerA)
      fx.corpusB = insertCorpus(db, 'Graph corpus B', ownerB)
      db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'viewer')").run(viewer3, fx.corpusA)
      db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(fx.corpusA, viewer3)
      fx.workA = insertWork(db, 'Graph work in A', fx.corpusA)
      fx.workB = insertWork(db, 'Graph work in B', fx.corpusB)
      fx.workNone = insertWork(db, 'Graph work in no corpus', null)
    })

    fx.adminToken = await login(adminUsername, adminPassword)
    fx.ownerAToken = await login('graph-owner-a', 'owner-a-password')
    fx.viewer3Token = await login('graph-viewer3', 'viewer3-password')
    fx.lonelyToken = await login('graph-lonely', 'lonely-password')

    fx.keyA = snapshotKeyFor({ corpus_id: String(fx.corpusA) })
    fx.keyAll = snapshotKeyFor({})
    writeSnapshot(fx.keyA, { corpusId: fx.corpusA })
    writeSnapshot(fx.keyAll, { corpusId: null })

    fx.k1 = 'a1a1a1a1a1a1a1a1a1a1'
    fx.k2 = 'b2b2b2b2b2b2b2b2b2b2'
    fx.k3 = 'c3c3c3c3c3c3c3c3c3c3'
    writeSnapshot(fx.k1, { corpusId: fx.corpusA })
    writeSnapshot(fx.k2, { corpusId: null })
    writeSnapshot(fx.k3)
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_DB_PATH
    delete process.env.RAG_FEEDER_GRAPH_3D_SNAPSHOT_DIR
    delete process.env.RAG_FEEDER_JWT_SECRET
    delete process.env.RAG_ADMIN_USER
    delete process.env.RAG_ADMIN_PASSWORD
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  const get = (url, token) => request(app).get(url).set('Authorization', `Bearer ${token}`)

  test('snapshot: corpus_id of a foreign corpus -> 403; own -> not 403', async () => {
    const foreign = await get(`/api/graph/3d/snapshot?corpus_id=${fx.corpusB}`, fx.viewer3Token)
    expect(foreign.status).toBe(403)
    expect(foreign.body).toEqual({ error: 'No access to this corpus' })

    const own = await get(`/api/graph/3d/snapshot?corpus_id=${fx.corpusA}`, fx.viewer3Token)
    expect(own.status).toBe(200)
    expect(own.body.snapshot_key).toBe(fx.keyA)

    const admin = await get(`/api/graph/3d/snapshot?corpus_id=${fx.corpusA}`, fx.adminToken)
    expect(admin.status).toBe(200)
    expect(admin.body.snapshot_key).toBe(fx.keyA)
  })

  test('snapshot: all is admin only; a non-admin is scoped to req.corpusId; none -> 403 Select a corpus first', async () => {
    const adminAll = await get('/api/graph/3d/snapshot?corpus_id=all', fx.adminToken)
    expect(adminAll.status).toBe(200)
    expect(adminAll.body.snapshot_key).toBe(fx.keyAll)
    const adminAbsent = await get('/api/graph/3d/snapshot', fx.adminToken)
    expect(adminAbsent.body.snapshot_key).toBe(fx.keyAll)

    const viewerAll = await get('/api/graph/3d/snapshot?corpus_id=all', fx.viewer3Token)
    const viewerA = await get(`/api/graph/3d/snapshot?corpus_id=${fx.corpusA}`, fx.viewer3Token)
    expect(viewerAll.status).toBe(200)
    expect(viewerAll.body.snapshot_key).toBe(fx.keyA)
    expect(viewerAll.body).toEqual(viewerA.body)
    const viewerAbsent = await get('/api/graph/3d/snapshot', fx.viewer3Token)
    expect(viewerAbsent.body.snapshot_key).toBe(fx.keyA)

    const lonely = await get('/api/graph/3d/snapshot?corpus_id=all', fx.lonelyToken)
    expect(lonely.status).toBe(403)
    expect(lonely.body).toEqual({ error: 'Select a corpus first' })
  })

  test('node: foreign work -> 404, own -> 200, admin -> 200 for both; a work in no corpus -> 404 for members', async () => {
    const own = await get(`/api/graph/3d/node/${fx.workA}`, fx.viewer3Token)
    expect(own.status).toBe(200)
    expect(own.body.title).toBe('Graph work in A')

    const foreign = await get(`/api/graph/3d/node/${fx.workB}`, fx.viewer3Token)
    expect(foreign.status).toBe(404)
    expect(foreign.body).toEqual({ error: 'Node not found' })

    const orphan = await get(`/api/graph/3d/node/${fx.workNone}`, fx.ownerAToken)
    expect(orphan.status).toBe(404)

    for (const workId of [fx.workA, fx.workB, fx.workNone]) {
      const admin = await get(`/api/graph/3d/node/${workId}`, fx.adminToken)
      expect(admin.status).toBe(200)
    }
  })

  test('cluster and snapshot files: k1 member 200, k2 member 403 admin 200, k3 member 403 admin 200', async () => {
    const expectations = [
      [fx.k1, 200, 200],
      [fx.k2, 403, 200],
      [fx.k3, 403, 200],
    ]
    for (const [key, memberStatus, adminStatus] of expectations) {
      for (const url of [`/api/graph/3d/cluster/${key}/1`, `/api/graph/3d/snapshot/${key}/clusters.json`]) {
        const member = await get(url, fx.viewer3Token)
        expect([url, member.status]).toEqual([url, memberStatus])
        if (memberStatus === 403) expect(member.body).toEqual({ error: 'No access to this corpus' })
        const admin = await get(url, fx.adminToken)
        expect([url, admin.status]).toEqual([url, adminStatus])
      }
    }
    // A user who is not a member of k1's corpus is refused as well.
    const lonely = await get(`/api/graph/3d/cluster/${fx.k1}/1`, fx.lonelyToken)
    expect(lonely.status).toBe(403)
    // A client-supplied corpus never widens access.
    const spoofed = await get(`/api/graph/3d/snapshot/${fx.k2}/manifest.json?corpus_id=${fx.corpusA}`, fx.viewer3Token)
    expect(spoofed.status).toBe(403)
  })

  test('writeGraph3dScope / readGraph3dScope round-trip, and a missing sidecar reads as null', () => {
    const key = 'd4d4d4d4d4d4d4d4d4d4'
    expect(appModule.readGraph3dScope(key)).toBeNull()
    appModule.writeGraph3dScope(key, fx.corpusB)
    expect(appModule.graph3dScopePath(key)).toBe(path.join(snapshotDir, key, 'scope.json'))
    expect(JSON.parse(fs.readFileSync(path.join(snapshotDir, key, 'scope.json'), 'utf-8'))).toEqual({ corpusId: fx.corpusB })
    expect(appModule.readGraph3dScope(key)).toEqual({ corpusId: fx.corpusB })
    appModule.writeGraph3dScope(key, null)
    expect(appModule.readGraph3dScope(key)).toEqual({ corpusId: null })
    expect(appModule.readGraph3dScope(fx.k3)).toBeNull()
  })
})
