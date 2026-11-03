import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'

describe('explicit-corpus routes enforce corpus roles', () => {
  let app
  let dbPath = ''
  let tmpDir = ''
  let logDir = ''
  let artifactDir = ''
  const adminUsername = 'access-routes-admin'
  const adminPassword = 'access-routes-admin-password'
  const password = 'access-routes-password'
  const tokens = {}
  const ids = {}

  const withDb = (fn) => {
    const db = new Database(dbPath)
    try {
      return fn(db)
    } finally {
      db.close()
    }
  }

  const insertUser = (db, username) =>
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

  const login = async (username, pw = password) => {
    const res = await request(app).post('/api/auth/login').send({ username, password: pw })
    expect(res.status).toBe(200)
    return res.body.token
  }

  const as = (who) => ({ Authorization: `Bearer ${tokens[who]}` })

  const seedArtifact = (bibtexKey) =>
    withDb((db) => {
      const runId = Number(
        db
          .prepare("INSERT INTO scraper_runs (source_type, query_json, status) VALUES ('eurlex', '{}', 'completed')")
          .run().lastInsertRowid
      )
      const sourceFile = path.join(artifactDir, `${bibtexKey}.pdf`)
      fs.writeFileSync(sourceFile, 'PDF bytes')
      return Number(
        db
          .prepare(
            `INSERT INTO scraper_artifacts (
               run_id, source_type, source_id, title, year, source_url, local_artifact_path,
               file_name, raw_metadata_json, bibtex_key, status
             ) VALUES (?, 'eurlex', ?, 'Access Act', 2024, 'https://example.test/act', ?, ?, '{}', ?, 'downloaded')`
          )
          .run(runId, bibtexKey, sourceFile, `${bibtexKey}.pdf`, bibtexKey).lastInsertRowid
      )
    })

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-access-routes-'))
    dbPath = path.join(tmpDir, 'access-routes.db')
    logDir = path.join(tmpDir, 'logs')
    artifactDir = path.join(tmpDir, 'artifacts')
    const targetADir = path.join(tmpDir, 'target-a')
    const targetBDir = path.join(tmpDir, 'target-b')
    for (const dir of [logDir, artifactDir, targetADir, targetBDir]) fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(path.join(targetADir, 'metadata.bib'), '@book{existing,\n  title = {Existing}\n}\n', 'utf8')
    fs.writeFileSync(path.join(targetBDir, 'metadata.bib'), '', 'utf8')

    process.env.RAG_FEEDER_DB_PATH = dbPath
    process.env.RAG_FEEDER_JWT_SECRET = 'access-routes-secret'
    process.env.RAG_ADMIN_USER = adminUsername
    process.env.RAG_ADMIN_PASSWORD = adminPassword
    process.env.RAG_FEEDER_LOG_DIR = logDir
    process.env.RAG_FEEDER_ARTIFACTS_DIR = artifactDir
    process.env.RAG_FEEDER_KANTROPOS_CORPORA = JSON.stringify([
      { id: 'target-a', name: 'Target A', path: targetADir },
      { id: 'target-b', name: 'Target B', path: targetBDir },
    ])

    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast: () => {} })

    withDb((db) => {
      ids.owner1 = insertUser(db, 'owner1')
      ids.owner2 = insertUser(db, 'owner2')
      ids.viewer3 = insertUser(db, 'viewer3')
      ids.editor4 = insertUser(db, 'editor4')
      ids.outsider5 = insertUser(db, 'outsider5')
      ids.nocorpus6 = insertUser(db, 'nocorpus6')
      ids.A = insertCorpus(db, 'Corpus A', ids.owner1)
      ids.B = insertCorpus(db, 'Corpus B', ids.owner2)
      ids.C = insertCorpus(db, 'Corpus C', ids.outsider5)
      ids.D = insertCorpus(db, 'Corpus D', ids.owner2)
      db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'viewer')").run(ids.viewer3, ids.A)
      db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'editor')").run(ids.editor4, ids.A)
    })

    fs.writeFileSync(
      path.join(logDir, 'backend-pipeline.log'),
      [`[p] corpus=${ids.A} line-a`, `[p] corpus=${ids.B} line-b`].join('\n') + '\n',
      'utf8'
    )
    fs.writeFileSync(path.join(logDir, 'backend-app.log'), '[app] one\n', 'utf8')

    tokens.admin = await login(adminUsername, adminPassword)
    for (const name of ['owner1', 'owner2', 'viewer3', 'editor4', 'outsider5', 'nocorpus6']) {
      tokens[name] = await login(name)
    }

    const assign = await request(app)
      .put(`/api/corpora/${ids.A}/kantropos-assignment`)
      .set(as('owner1'))
      .send({ targetId: 'target-a' })
    expect(assign.status).toBe(200)
  })

  afterAll(() => {
    for (const key of [
      'RAG_FEEDER_DB_PATH',
      'RAG_FEEDER_JWT_SECRET',
      'RAG_ADMIN_USER',
      'RAG_ADMIN_PASSWORD',
      'RAG_FEEDER_LOG_DIR',
      'RAG_FEEDER_ARTIFACTS_DIR',
      'RAG_FEEDER_KANTROPOS_CORPORA',
    ]) {
      delete process.env[key]
    }
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  test('logs: type=app is admin only', async () => {
    const viewer = await request(app).get('/api/logs/tail?type=app').set(as('viewer3'))
    expect(viewer.status).toBe(403)
    expect(viewer.body.error).toBe('Admin access required')

    const admin = await request(app).get('/api/logs/tail?type=app').set(as('admin'))
    expect(admin.status).toBe(200)
    expect(admin.body.entries).toContain('[app] one')
  })

  test('logs: pipeline corpus_id of a foreign corpus -> 403', async () => {
    const foreign = await request(app).get(`/api/logs/tail?type=pipeline&corpus_id=${ids.B}`).set(as('viewer3'))
    expect(foreign.status).toBe(403)
    expect(foreign.body.error).toBe('No access to this corpus')

    const own = await request(app).get(`/api/logs/tail?type=pipeline&corpus_id=${ids.A}`).set(as('viewer3'))
    expect(own.status).toBe(200)
    expect(own.body.entries).toEqual([`[p] corpus=${ids.A} line-a`])
  })

  test('logs: pipeline without corpus_id uses req.corpusId; none -> 403 Select a corpus first', async () => {
    const scoped = await request(app).get('/api/logs/tail?type=pipeline').set(as('viewer3'))
    expect(scoped.status).toBe(200)
    expect(scoped.body.entries).toEqual([`[p] corpus=${ids.A} line-a`])

    const none = await request(app).get('/api/logs/tail?type=pipeline').set(as('nocorpus6'))
    expect(none.status).toBe(403)
    expect(none.body.error).toBe('Select a corpus first')
  })

  test('select: non-member 403, member 200', async () => {
    const foreign = await request(app).post(`/api/corpora/${ids.B}/select`).set(as('viewer3'))
    expect(foreign.status).toBe(403)

    const shared = await request(app).post(`/api/corpora/${ids.A}/select`).set(as('viewer3'))
    expect(shared.status).toBe(200)
    expect(shared.body).toEqual({ selected: ids.A })
    expect(withDb((db) => db.prepare('SELECT last_corpus_id FROM users WHERE id = ?').get(ids.viewer3).last_corpus_id))
      .toBe(ids.A)
  })

  test('share and delete need owner', async () => {
    const viewerShare = await request(app)
      .post(`/api/corpora/${ids.A}/share`)
      .set(as('viewer3'))
      .send({ username: 'outsider5', role: 'viewer' })
    expect(viewerShare.status).toBe(403)
    expect(viewerShare.body.error).toBe('Only owners can share corpora')

    const editorShare = await request(app)
      .post(`/api/corpora/${ids.A}/share`)
      .set(as('editor4'))
      .send({ username: 'outsider5', role: 'viewer' })
    expect(editorShare.status).toBe(403)

    const ownerShare = await request(app)
      .post(`/api/corpora/${ids.A}/share`)
      .set(as('owner1'))
      .send({ username: 'viewer3', role: 'viewer' })
    expect(ownerShare.status).toBe(200)
    expect(ownerShare.body).toEqual({ shared: true })

    const outsiderDelete = await request(app).delete(`/api/corpora/${ids.D}`).set(as('viewer3'))
    expect(outsiderDelete.status).toBe(403)
    expect(outsiderDelete.body.error).toBe('No access to this corpus')

    const editorDelete = await request(app).delete(`/api/corpora/${ids.A}`).set(as('editor4'))
    expect(editorDelete.status).toBe(403)
    expect(editorDelete.body.error).toBe('Only owners can delete corpora')

    const ownerDelete = await request(app).delete(`/api/corpora/${ids.D}`).set(as('owner2'))
    expect(ownerDelete.status).toBe(200)
    expect(withDb((db) => db.prepare('SELECT id FROM corpora WHERE id = ?').get(ids.D))).toBeUndefined()
  })

  test('kantropos-assignment: GET member, PUT owner/editor', async () => {
    const viewerGet = await request(app).get(`/api/corpora/${ids.A}/kantropos-assignment`).set(as('viewer3'))
    expect(viewerGet.status).toBe(200)
    expect(viewerGet.body.assignment.target_id).toBe('target-a')
    expect(viewerGet.body.can_edit).toBe(false)

    const outsiderGet = await request(app).get(`/api/corpora/${ids.A}/kantropos-assignment`).set(as('outsider5'))
    expect(outsiderGet.status).toBe(403)

    const viewerPut = await request(app)
      .put(`/api/corpora/${ids.A}/kantropos-assignment`)
      .set(as('viewer3'))
      .send({ targetId: 'target-b' })
    expect(viewerPut.status).toBe(403)
    expect(viewerPut.body.error).toBe(
      'Only corpus owners, editors, or the global admin can assign a Kantropos corpus'
    )

    const editorPut = await request(app)
      .put(`/api/corpora/${ids.A}/kantropos-assignment`)
      .set(as('editor4'))
      .send({ targetId: 'target-a' })
    expect(editorPut.status).toBe(200)
    expect(editorPut.body.assignment.target_id).toBe('target-a')

    const adminGet = await request(app).get(`/api/corpora/${ids.A}/kantropos-assignment`).set(as('admin'))
    expect(adminGet.status).toBe(200)
    expect(adminGet.body.can_edit).toBe(true)
  })

  test('scraper apply: viewer 403, owner of an assigned corpus 200 path reached, admin reached', async () => {
    const artifactId = seedArtifact('access_apply_one')
    const send = (who, targetId) =>
      request(app).post('/api/scraper/apply').set(as(who)).send({ targetId, artifactIds: [artifactId] })

    for (const who of ['viewer3', 'outsider5']) {
      const res = await send(who, 'target-a')
      expect(res.status).toBe(403)
    }
    // target-b is assigned to no corpus: only the admin may apply to it.
    expect((await send('owner1', 'target-b')).status).toBe(403)
    expect((await send('admin', 'target-b')).status).not.toBe(403)

    const owner = await send('owner1', 'target-a')
    expect(owner.status).toBe(200)
    expect(owner.body.applied_count).toBe(1)
    expect((await send('editor4', 'target-a')).status).not.toBe(403)
  })

  test('scraper metadata-bib: applier, assigned owner/editor and admin may download; others 403', async () => {
    const artifactId = seedArtifact('access_apply_two')
    const apply = await request(app)
      .post('/api/scraper/apply')
      .set(as('owner1'))
      .send({ targetId: 'target-a', artifactIds: [artifactId] })
    expect(apply.status).toBe(200)
    const url = apply.body.generated_bib_url
    expect(url).toContain('/metadata-bib')

    for (const who of ['owner1', 'editor4', 'admin']) {
      const res = await request(app).get(url).set(as(who))
      expect(res.status).toBe(200)
      expect(res.text).toContain('access_apply_two')
    }
    for (const who of ['viewer3', 'outsider5', 'owner2']) {
      const res = await request(app).get(url).set(as(who))
      expect(res.status).toBe(403)
    }

    // The applier keeps access to their own draft even without an assigned corpus.
    const draftPath = path.join(artifactDir, 'bibliographies', 'outsider-draft.bib')
    fs.mkdirSync(path.dirname(draftPath), { recursive: true })
    fs.writeFileSync(draftPath, '@misc{outsider_draft}\n', 'utf8')
    const eventId = withDb((db) =>
      Number(
        db
          .prepare(
            `INSERT INTO scraper_apply_events (target_id, target_path, user_id, artifact_ids_json, generated_bib_path)
             VALUES ('target-b', ?, ?, '[]', ?)`
          )
          .run(path.join(tmpDir, 'target-b'), ids.outsider5, draftPath).lastInsertRowid
      )
    )
    const ownUrl = `/api/scraper/apply-events/${eventId}/metadata-bib`
    const applier = await request(app).get(ownUrl).set(as('outsider5'))
    expect(applier.status).toBe(200)
    expect(applier.text).toContain('outsider_draft')
    expect((await request(app).get(ownUrl).set(as('owner1'))).status).toBe(403)
  })
})
