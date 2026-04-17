import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import request from 'supertest'
import Database from 'better-sqlite3'

function createGraphDb(dbPath) {
  const db = new Database(dbPath)

  db.exec(`
    CREATE TABLE works (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT,
      type TEXT,
      year INTEGER,
      doi TEXT,
      normalized_doi TEXT,
      openalex_id TEXT,
      normalized_title TEXT,
      normalized_authors TEXT,
      crossref_json TEXT,
      openalex_json TEXT,
      metadata_status TEXT,
      download_status TEXT,
      source_work_id TEXT,
      relationship_type TEXT,
      run_id INTEGER,
      source_pdf TEXT,
      origin_key TEXT
    );

    CREATE TABLE corpus_works (
      corpus_id INTEGER NOT NULL,
      work_id INTEGER NOT NULL,
      added_at TEXT DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (corpus_id, work_id)
    );

    CREATE TABLE citation_edges (
      source_id TEXT NOT NULL,
      target_id TEXT NOT NULL,
      relationship_type TEXT NOT NULL,
      run_id INTEGER
    );
  `)

  const worksInsert = db.prepare(`
    INSERT INTO works
    (source_pdf, origin_key, title, type, year, doi, normalized_doi, openalex_id, normalized_title, normalized_authors, metadata_status, download_status, source_work_id, relationship_type, run_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `)

  worksInsert.run('seed.pdf', 'seed_pdf', 'Seed Paper', 'journal-article', 2020, '10.1000/seed', '10.1000/seed', 'W100', 'seed paper', 'author-a', 'matched', 'not_requested', null, 'references', 1)
  worksInsert.run('seed.pdf', 'seed_pdf', 'Downstream Paper', 'book-chapter', 2021, '10.1000/downstream', '10.1000/downstream', 'W200', 'downstream paper', 'author-b', 'matched', 'queued', null, 'references', 1)
  worksInsert.run('seed.pdf', 'seed_pdf', 'Queued Cite Source', 'conference-paper', 2022, '10.1000/queued', '10.1000/queued', 'W300', 'queued cite source', 'author-c', 'matched', 'queued', 'W200', 'references', 1)

  const corpusInsert = db.prepare(`
    INSERT INTO corpus_works (corpus_id, work_id)
    VALUES (?, ?)
  `)
  corpusInsert.run(1, 1)
  corpusInsert.run(1, 2)
  corpusInsert.run(1, 3)

  const edges = db.prepare(`
    INSERT INTO citation_edges (source_id, target_id, relationship_type, run_id)
    VALUES (?, ?, ?, ?)
  `)
  edges.run('W100', 'W200', 'references', 1)
  edges.run('W200', 'W300', 'cited_by', 1)

  db.close()
}

describe('GET /api/graph (fixture db)', () => {
  let app = null
  let token = ''
  let dbPath = ''
  let tmpDir = ''

  beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-graph-fixture-'))
    dbPath = path.join(tmpDir, 'graph_fixture.db')
    createGraphDb(dbPath)

    process.env.RAG_FEEDER_STUB = '0'
    process.env.RAG_FEEDER_DB_PATH = dbPath
    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast: () => {} })

    const login = await request(app)
      .post('/api/auth/login')
      .set('Content-Type', 'application/json')
      .send({ username: 'admin', password: 'admin' })
    token = login.body?.token || ''
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_DB_PATH
    delete process.env.RAG_FEEDER_STUB
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  test('returns deterministic graph payload', async () => {
    const response = await request(app)
      .get('/api/graph?max_nodes=200&relationship=both&status=all&hide_isolates=0')
      .set('Authorization', `Bearer ${token}`)

    expect(response.status).toBe(200)
    expect(response.body).toHaveProperty('nodes')
    expect(response.body).toHaveProperty('edges')
    expect(response.body).toHaveProperty('stats')
    expect(response.body.nodes).toHaveLength(3)
    expect(response.body.edges).toHaveLength(2)
    expect(response.body.stats).toMatchObject({
      node_count: 3,
      edge_count: 2,
      relationship_counts: { references: 1, cited_by: 1 },
    })
    expect(response.body.nodes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: 'W100', title: 'Seed Paper' }),
        expect.objectContaining({ id: 'W200', title: 'Downstream Paper' }),
        expect.objectContaining({ id: 'W300', title: 'Queued Cite Source' }),
      ])
    )
  }, 30000)

  test('filters by relationship type', async () => {
    const references = await request(app)
      .get('/api/graph?relationship=references&status=all&hide_isolates=0')
      .set('Authorization', `Bearer ${token}`)
    expect(references.status).toBe(200)
    expect(references.body.edges).toHaveLength(1)
    expect(references.body.edges[0].relationship_type).toBe('references')

    const citedBy = await request(app)
      .get('/api/graph?relationship=cited_by&status=all&hide_isolates=0')
      .set('Authorization', `Bearer ${token}`)
    expect(citedBy.status).toBe(200)
    expect(citedBy.body.edges).toHaveLength(1)
    expect(citedBy.body.edges[0].relationship_type).toBe('cited_by')
  })

  test('respects status filters', async () => {
    const queued = await request(app)
      .get('/api/graph?status=queued_download&relationship=both&hide_isolates=0')
      .set('Authorization', `Bearer ${token}`)
    expect(queued.status).toBe(200)
    expect(queued.body.nodes).toHaveLength(2)
    expect(queued.body.nodes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: 'W200', status: 'queued_download' }),
        expect.objectContaining({ id: 'W300', status: 'queued_download' }),
      ])
    )
  })
})
