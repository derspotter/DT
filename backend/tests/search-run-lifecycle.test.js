import Database from 'better-sqlite3'
import { markOrphanedSearchRuns } from '../src/app.js'

test('runs still "running" from before this backend started are marked failed', () => {
  const db = new Database(':memory:')
  db.exec(`CREATE TABLE search_runs (id INTEGER PRIMARY KEY, query TEXT, status TEXT, error TEXT, finished_at TIMESTAMP, created_at TIMESTAMP)`)
  db.prepare(`INSERT INTO search_runs (id, query, status, created_at) VALUES (1, 'old', 'running', '2026-01-01T00:00:00Z')`).run()
  db.prepare(`INSERT INTO search_runs (id, query, status, created_at) VALUES (2, 'new', 'running', '2026-12-31T00:00:00Z')`).run()
  db.prepare(`INSERT INTO search_runs (id, query, status, created_at) VALUES (3, 'done', 'done', '2026-01-01T00:00:00Z')`).run()
  const n = markOrphanedSearchRuns(db, '2026-06-01T00:00:00Z')
  expect(n).toBe(1)
  expect(db.prepare('SELECT status, error FROM search_runs WHERE id = 1').get()).toEqual({ status: 'failed', error: 'backend restarted' })
  expect(db.prepare('SELECT status FROM search_runs WHERE id = 2').get().status).toBe('running')
  expect(db.prepare('SELECT status FROM search_runs WHERE id = 3').get().status).toBe('done')
  db.close()
})
