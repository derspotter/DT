import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import bcrypt from 'bcryptjs'
import request from 'supertest'
import { jest } from '@jest/globals'

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
      path.join(logDir, 'backend-pipeline-v2.log'),
      [
        `[2026-01-01T00:00:00.000Z] [get_bib_pages][corpus=${ids.A}] line-a`,
        `[2026-01-01T00:00:00.000Z] [get_bib_pages][corpus=${ids.B}] line-b`,
      ].join('\n') + '\n',
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
    expect(own.body.entries).toEqual([`[2026-01-01T00:00:00.000Z] [get_bib_pages][corpus=${ids.A}] line-a`])
  })

  test('logs: pipeline without corpus_id uses req.corpusId; none -> 403 Select a corpus first', async () => {
    const scoped = await request(app).get('/api/logs/tail?type=pipeline').set(as('viewer3'))
    expect(scoped.status).toBe(200)
    expect(scoped.body.entries).toEqual([`[2026-01-01T00:00:00.000Z] [get_bib_pages][corpus=${ids.A}] line-a`])

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

  test('daemon status: non-admins see no foreign corpus job details; admin sees all', async () => {
    withDb((db) => {
      const upsert = db.prepare(
        `INSERT INTO daemon_heartbeat (daemon_name, worker_id, status, details_json, last_seen_at, updated_at)
         VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT(daemon_name) DO UPDATE SET worker_id = excluded.worker_id, status = excluded.status,
           details_json = excluded.details_json, last_seen_at = excluded.last_seen_at`
      )
      upsert.run('enrich', 'worker-enrich', 'error', JSON.stringify({
        state: 'job_failed', job_id: 98765, job_type: 'enrich', corpus_id: ids.B, error: 'secret-corpus-b-failure',
      }))
      upsert.run('download', 'worker-download', 'running', JSON.stringify({
        state: 'running_job', job_id: 4321, job_type: 'download', corpus_id: ids.A,
      }))
    })
    const urls = ['/api/pipeline/daemon/status', '/api/pipeline/jobs/summary']
    for (const url of urls) {
      const member = await request(app).get(url).set(as('owner1'))
      expect(member.status).toBe(200)
      const text = JSON.stringify(member.body.daemon)
      expect(text).not.toContain('secret-corpus-b-failure')
      expect(text).not.toContain('98765')
      // Health fields stay for the daemon display.
      expect(member.body.daemon.workers.enrich).toMatchObject({ online: true, status: 'error', worker_id: 'worker-enrich' })
      expect(member.body.daemon.workers.enrich.details).toEqual({ state: 'job_failed' })
      // The member's own corpus job keeps its details.
      expect(member.body.daemon.workers.download.details).toMatchObject({ job_id: 4321, corpus_id: ids.A })

      const admin = await request(app).get(url).set(as('admin'))
      expect(admin.status).toBe(200)
      expect(admin.body.daemon.workers.enrich.details).toMatchObject({
        job_id: 98765, corpus_id: ids.B, error: 'secret-corpus-b-failure',
      })
    }
  })

  test('corpus writes: admin passes on a non-member corpus, a viewer still gets 403', async () => {
    const select = await request(app).post(`/api/corpora/${ids.C}/select`).set(as('admin'))
    expect(select.status).toBe(200)
    const adminWrite = await request(app).post('/api/corpus/works/remove').set(as('admin')).send({ workIds: [999999] })
    expect(adminWrite.status).toBe(200)
    expect(adminWrite.body).toMatchObject({ removed: 0, skipped_work_ids: [999999] })

    expect((await request(app).post(`/api/corpora/${ids.A}/select`).set(as('viewer3'))).status).toBe(200)
    const viewerWrite = await request(app).post('/api/corpus/works/remove').set(as('viewer3')).send({ workIds: [999999] })
    expect(viewerWrite.status).toBe(403)
    expect(viewerWrite.body).toEqual({ error: 'Only corpus owners or editors can change corpus data' })
  })

  test('admin bypass covers only existing corpora; invalid ids never pass', async () => {
    const adminId = withDb((db) => db.prepare('SELECT id FROM users WHERE username = ?').get(adminUsername).id)
    const lastCorpus = () =>
      withDb((db) => db.prepare('SELECT last_corpus_id FROM users WHERE id = ?').get(adminId).last_corpus_id)
    expect((await request(app).post(`/api/corpora/${ids.A}/select`).set(as('admin'))).status).toBe(200)
    expect(lastCorpus()).toBe(ids.A)

    for (const bad of ['999999', '-1', '1.5', 'abc', '0']) {
      const res = await request(app).post(`/api/corpora/${bad}/select`).set(as('admin'))
      expect([bad, res.status]).toEqual([bad, 403])
      expect(['Select a corpus first', 'No access to this corpus']).toContain(res.body.error)
      expect(lastCorpus()).toBe(ids.A)
    }
    const missing = await request(app).post('/api/corpora/999999/select').set(as('admin'))
    expect(missing.body.error).toBe('No access to this corpus')

    const userCorporaRows = () =>
      withDb((db) => db.prepare('SELECT COUNT(*) AS n FROM user_corpora WHERE corpus_id IN (999999, -1)').get().n)
    for (const bad of ['999999', '-1']) {
      const share = await request(app)
        .post(`/api/corpora/${bad}/share`)
        .set(as('admin'))
        .send({ username: 'outsider5', role: 'viewer' })
      expect(share.status).toBe(403)
      const assign = await request(app)
        .put(`/api/corpora/${bad}/kantropos-assignment`)
        .set(as('admin'))
        .send({ targetId: 'target-b' })
      expect(assign.status).toBe(403)
    }
    expect(userCorporaRows()).toBe(0)
    expect(
      withDb((db) =>
        db.prepare('SELECT COUNT(*) AS n FROM corpus_kantropos_assignments WHERE corpus_id IN (999999, -1)').get().n
      )
    ).toBe(0)
  })

  // D11: the readable scope, the corpus-tag trust boundary, the v2 log
  // generation and the post-read access recheck. Own users, corpora and log dir.
  describe('logs: readable scope and the post-read recheck', () => {
    const ts = '[2026-01-01T00:00:00.000Z]'
    let caseDir = ''
    const realOpen = fs.promises.open.bind(fs.promises)

    // One-shot: `fn(filePath)` runs right after the first real open whose path
    // satisfies `match`.
    const onOpen = (match, fn) => {
      let fired = false
      return jest.spyOn(fs.promises, 'open').mockImplementation(async (...args) => {
        const handle = await realOpen(...args)
        if (!fired && match(String(args[0]))) {
          fired = true
          fn(String(args[0]))
        }
        return handle
      })
    }

    beforeAll(async () => {
      withDb((db) => {
        ids.owner11 = insertUser(db, 'owner11')
        ids.member9 = insertUser(db, 'member9')
        ids.member10 = insertUser(db, 'member10')
        const corpus = (name) =>
          Number(db.prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)').run(name, ids.owner11).lastInsertRowid)
        ids.E = corpus('Corpus E')
        ids.F = corpus('Corpus F')
        ids.G = corpus('Corpus G')
        const grant = db.prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'viewer')")
        grant.run(ids.member9, ids.E)
        grant.run(ids.member9, ids.F)
        grant.run(ids.member10, ids.A)
        db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(ids.E, ids.member9)
        db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(ids.A, ids.member10)
        ids.admin = db.prepare('SELECT id FROM users WHERE username = ?').get(adminUsername).id
      })
      tokens.member9 = await login('member9')
      tokens.member10 = await login('member10')
    })

    beforeEach(() => {
      caseDir = fs.mkdtempSync(path.join(tmpDir, 'logs-case-'))
      process.env.RAG_FEEDER_LOG_DIR = caseDir
      fs.writeFileSync(
        path.join(caseDir, 'backend-pipeline-v2.log'),
        [
          `${ts} [get_bib_pages][corpus=${ids.E}] line-e`,
          `${ts} [get_bib_pages][corpus=${ids.F}] line-f`,
          `${ts} [get_bib_pages][corpus=${ids.G}] line-g`,
          `${ts} [p] untagged-line`,
          `${ts} [get_bib_pages][corpus=none] line-n`,
          `${ts} [DAEMON] Job 9 failed: corpus=${ids.E} boom`,
        ].join('\n') + '\n',
        'utf8'
      )
      fs.writeFileSync(
        path.join(caseDir, 'backend-pipeline.log'),
        [`${ts} [DAEMON] Job 8 failed: Traceback`, `${ts} [get_bib_pages][corpus=${ids.E}] forged-legacy`].join('\n') + '\n',
        'utf8'
      )
      fs.writeFileSync(path.join(caseDir, 'backend-app.log'), '[app] one\n', 'utf8')
    })

    afterEach(() => {
      jest.restoreAllMocks()
      process.env.RAG_FEEDER_LOG_DIR = logDir
      withDb((db) => {
        db.prepare('UPDATE users SET username = ? WHERE id = ?').run(adminUsername, ids.admin)
        db.prepare("INSERT OR IGNORE INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'viewer')").run(
          ids.member9,
          ids.F
        )
      })
    })

    const lineE = () => `${ts} [get_bib_pages][corpus=${ids.E}] line-e`
    const lineF = () => `${ts} [get_bib_pages][corpus=${ids.F}] line-f`

    test('logs: scope=readable shows what the live socket would', async () => {
      const viewer = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('member9'))
      expect(viewer.status).toBe(200)
      expect(viewer.body.entries).toEqual([lineE(), lineF()])
      const admin = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('admin'))
      expect(admin.status).toBe(200)
      expect(admin.body.entries).toHaveLength(8)
      expect(admin.body.entries.slice(0, 2)).toEqual([
        `${ts} [DAEMON] Job 8 failed: Traceback`,
        `${ts} [get_bib_pages][corpus=${ids.E}] forged-legacy`,
      ])
      expect(admin.body.entries.slice(2)[0]).toBe(lineE())
      expect(admin.body).not.toHaveProperty('total_lines')
      expect(viewer.body).not.toHaveProperty('total_lines')
    })

    test('logs: a legacy continuation with a complete recognized prefix never reaches a member', async () => {
      const base = '/api/logs/tail?type=pipeline&include_rotated=1&cursor=0'
      const cases = [
        [`${base}&scope=readable`, [lineE(), lineF()]],
        [`${base}&corpus_id=${ids.E}`, [lineE()]],
        [base, [lineE()]],
      ]
      for (const [url, expected] of cases) {
        const res = await request(app).get(url).set(as('member9'))
        expect([url, res.status, res.body.entries, res.body.has_more]).toEqual([url, 200, expected, false])
      }
    })

    test('logs: a scoped admin read never includes the old generation', async () => {
      const res = await request(app)
        .get(`/api/logs/tail?type=pipeline&corpus_id=${ids.E}&include_rotated=1&cursor=0`)
        .set(as('admin'))
      expect(res.status).toBe(200)
      expect(res.body.entries).toEqual([lineE()])
      expect(res.body.has_more).toBe(false)
    })

    test('logs: an admin demoted during a merged read that includes the old generation gets no window', async () => {
      onOpen(
        (p) => path.basename(p) === 'backend-pipeline.log',
        () => withDb((db) => db.prepare("UPDATE users SET username = 'demoted' WHERE id = ?").run(ids.admin))
      )
      const res = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('admin'))
      expect(res.status).toBe(409)
      expect(res.body).toEqual({ error: 'Log access changed during the read' })
    })

    test('logs: an explicit foreign corpus_id with scope=readable is still 403', async () => {
      const res = await request(app)
        .get(`/api/logs/tail?type=pipeline&scope=readable&corpus_id=${ids.G}`)
        .set(as('member9'))
      expect(res.status).toBe(403)
      expect(res.body.error).toBe('No access to this corpus')
    })

    test('logs: a scope value other than readable keeps the current-corpus fallback', async () => {
      const none = await request(app).get('/api/logs/tail?type=pipeline').set(as('member9'))
      expect(none.body.entries).toEqual([lineE()])
      for (const scope of ['READABLE', 'all']) {
        const res = await request(app).get(`/api/logs/tail?type=pipeline&scope=${scope}`).set(as('member9'))
        expect([scope, res.status, res.body.entries]).toEqual([scope, 200, none.body.entries])
      }
    })

    test('logs: type=app stays admin-only with scope=readable', async () => {
      const res = await request(app).get('/api/logs/tail?type=app&scope=readable').set(as('member9'))
      expect(res.status).toBe(403)
      expect(res.body.error).toBe('Admin access required')
    })

    test('logs: a membership revoked during the read discards the window', async () => {
      onOpen(
        () => true,
        () =>
          withDb((db) =>
            db.prepare('DELETE FROM user_corpora WHERE user_id = ? AND corpus_id = ?').run(ids.member9, ids.F)
          )
      )
      const res = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('member9'))
      expect(res.status).toBe(409)
      expect(res.body).toEqual({ error: 'Log access changed during the read' })
    })

    test('logs: an admin who loses admin access during an app-log read gets no window', async () => {
      onOpen(
        () => true,
        () => withDb((db) => db.prepare("UPDATE users SET username = 'demoted' WHERE id = ?").run(ids.admin))
      )
      const res = await request(app).get('/api/logs/tail?type=app').set(as('admin'))
      expect(res.status).toBe(409)
      expect(res.body).toEqual({ error: 'Log access changed during the read' })
    })

    test('logs: a member response names no server path; the admin keeps source_file', async () => {
      for (const url of ['/api/logs/tail?type=pipeline&scope=readable', `/api/logs/tail?type=pipeline&corpus_id=${ids.E}`, '/api/logs/tail?type=pipeline']) {
        const member = await request(app).get(url).set(as('member9'))
        expect([url, member.status, Object.prototype.hasOwnProperty.call(member.body, 'source_file')]).toEqual([url, 200, false])
        expect(JSON.stringify(member.body)).not.toContain(caseDir)
      }
      const admin = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('admin'))
      expect(admin.body.source_file).toBe(path.join(caseDir, 'backend-pipeline-v2.log'))
    })

    test('logs: a read error answers a member with a generic message, the admin with the detail', async () => {
      const failOpen = () =>
        jest.spyOn(fs.promises, 'open').mockImplementation(async (p) => {
          throw Object.assign(new Error(`EACCES: permission denied, open '${p}'`), { code: 'EACCES' })
        })
      const consoleError = jest.spyOn(console, 'error').mockImplementation(() => {})
      failOpen()
      for (const url of ['/api/logs/tail?type=pipeline&scope=readable', `/api/logs/tail?type=pipeline&corpus_id=${ids.E}`, '/api/logs/tail?type=pipeline']) {
        const member = await request(app).get(url).set(as('member9'))
        expect([url, member.status, member.body]).toEqual([url, 500, { error: 'Could not read the log' }])
      }
      const admin = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('admin'))
      expect(admin.status).toBe(500)
      expect(admin.body.error).toContain('EACCES')
      expect(admin.body.error).toContain(caseDir)
      expect(consoleError).toHaveBeenCalledWith('[/api/logs/tail] Error:', expect.objectContaining({ code: 'EACCES' }))
    })

    test('logs: a user deleted during the read gets no window', async () => {
      onOpen(
        () => true,
        () =>
          withDb((db) => {
            db.prepare('DELETE FROM user_corpora WHERE user_id = ?').run(ids.member10)
            db.prepare('DELETE FROM users WHERE id = ?').run(ids.member10)
          })
      )
      const res = await request(app).get('/api/logs/tail?type=pipeline').set(as('member10'))
      expect(res.status).toBe(409)
      expect(res.body).toEqual({ error: 'Log access changed during the read' })
    })
  })
})
