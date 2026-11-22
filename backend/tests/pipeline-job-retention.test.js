import Database from 'better-sqlite3'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { jest } from '@jest/globals'
import {
  PRUNE_BUSY_TIMEOUT_MS,
  PRUNE_STATEMENTS,
  pruneFinishedPipelineJobs,
  startPipelineJobRetention,
} from '../src/pipelineJobRetention.js'

// pipeline_jobs as app.js creates it, plus phase 6's (status, finished_at) index.
const SCHEMA = `
  CREATE TABLE IF NOT EXISTS pipeline_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corpus_id INTEGER,
    job_type TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    parameters_json TEXT,
    result_json TEXT,
    worker_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
  );
  CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_finished ON pipeline_jobs(status, finished_at);
`

let tmpDir
let dbPath
let db

function openDb({ schema = true } = {}) {
  const conn = new Database(dbPath)
  conn.pragma('journal_mode = WAL')
  if (schema) conn.exec(SCHEMA)
  return conn
}

function insertJob(conn, { status, finished = null, created, started = null }) {
  const at = (offset) => (offset === null ? 'NULL' : `datetime('now', '${offset}')`)
  conn.prepare(
    `INSERT INTO pipeline_jobs (job_type, status, finished_at, created_at, started_at)
     VALUES ('enrich', ?, ${at(finished)}, ${at(created)}, ${at(started)})`
  ).run(status)
}

function statuses(conn) {
  return conn.prepare('SELECT status, finished_at IS NULL AS open FROM pipeline_jobs ORDER BY id').all()
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-retention-'))
  dbPath = path.join(tmpDir, 'literature.db')
  db = openDb()
})

afterEach(() => {
  jest.useRealTimers()
  jest.restoreAllMocks()
  if (db.open) db.close()
  fs.rmSync(tmpDir, { recursive: true, force: true })
})

test('prunes finished jobs older than 30 days and nothing else', () => {
  insertJob(db, { status: 'completed', finished: '-31 days', created: '-40 days' })
  insertJob(db, { status: 'failed', finished: '-31 days', created: '-40 days' })
  insertJob(db, { status: 'cancelled', created: '-31 days' })
  insertJob(db, { status: 'cancelled', created: '-29 days' })
  insertJob(db, { status: 'completed', finished: '-29 days', created: '-40 days' })
  insertJob(db, { status: 'pending', created: '-400 days' })
  insertJob(db, { status: 'running', created: '-400 days', started: '-400 days' })

  expect(pruneFinishedPipelineJobs(db)).toBe(3)
  expect(statuses(db)).toEqual([
    { status: 'cancelled', open: 1 },
    { status: 'completed', open: 0 },
    { status: 'pending', open: 1 },
    { status: 'running', open: 1 },
  ])
})

test('both prune statements search the (status, finished_at) index', () => {
  expect(PRUNE_STATEMENTS).toHaveLength(2)
  for (const sql of PRUNE_STATEMENTS) {
    const params = Array.from({ length: (sql.match(/\?/g) || []).length }, () => '-30 days')
    const plan = db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...params)
    const details = plan.map((row) => row.detail).join('\n')
    expect(details).toMatch(/USING INDEX idx_pipeline_jobs_status_finished \(status=\? AND finished_at[<=]\?\)/)
  }
})

test('prune without the table returns 0', () => {
  db.close()
  fs.rmSync(dbPath, { force: true })
  db = openDb({ schema: false })
  expect(pruneFinishedPipelineJobs(db)).toBe(0)
})

test('a locked database fails the prune within its busy budget and restores busy_timeout', () => {
  expect(PRUNE_BUSY_TIMEOUT_MS).toBe(250)
  insertJob(db, { status: 'completed', finished: '-31 days', created: '-40 days' })
  db.pragma('busy_timeout = 15000')
  const other = new Database(dbPath)
  try {
    other.exec('BEGIN IMMEDIATE')
    const started = Date.now()
    expect(() => pruneFinishedPipelineJobs(db)).toThrow(/locked|busy/i)
    expect(Date.now() - started).toBeLessThan(2000)
    expect(db.pragma('busy_timeout', { simple: true })).toBe(15000)
    other.exec('ROLLBACK')
  } finally {
    other.close()
  }
  expect(db.prepare('SELECT COUNT(*) AS n FROM pipeline_jobs').get().n).toBe(1)
})

test('an exclusive lock in rollback-journal mode fails the prune within its busy budget', () => {
  insertJob(db, { status: 'completed', finished: '-31 days', created: '-40 days' })
  db.pragma('journal_mode = DELETE')
  expect(db.pragma('journal_mode', { simple: true })).toBe('delete')
  db.pragma('busy_timeout = 15000')
  const other = new Database(dbPath)
  try {
    // Blocks readers too, so even the schema read waits on the busy handler.
    other.exec('BEGIN EXCLUSIVE')
    const started = Date.now()
    expect(() => pruneFinishedPipelineJobs(db)).toThrow(/locked|busy/i)
    expect(Date.now() - started).toBeLessThan(2000)
    expect(db.pragma('busy_timeout', { simple: true })).toBe(15000)
    other.exec('ROLLBACK')
  } finally {
    other.close()
  }
  expect(db.prepare('SELECT COUNT(*) AS n FROM pipeline_jobs').get().n).toBe(1)
})

test('retention prunes at once, then daily, logs, never throws, and stops', () => {
  jest.useFakeTimers()
  const log = jest.spyOn(console, 'log').mockImplementation(() => {})
  const error = jest.spyOn(console, 'error').mockImplementation(() => {})
  const pruned = '[pipeline-jobs] pruned 1 finished job(s) older than 30 days'

  insertJob(db, { status: 'completed', finished: '-31 days', created: '-40 days' })
  const stop = startPipelineJobRetention(db)
  expect(log).toHaveBeenCalledTimes(1)
  expect(log).toHaveBeenLastCalledWith(pruned)

  insertJob(db, { status: 'failed', finished: '-31 days', created: '-40 days' })
  jest.advanceTimersByTime(86_400_000)
  expect(log).toHaveBeenCalledTimes(2)
  expect(log).toHaveBeenLastCalledWith(pruned)

  // Nothing left to prune: no log line.
  jest.advanceTimersByTime(86_400_000)
  expect(log).toHaveBeenCalledTimes(2)
  expect(error).not.toHaveBeenCalled()

  db.close()
  expect(() => jest.advanceTimersByTime(86_400_000)).not.toThrow()
  expect(error).toHaveBeenCalledTimes(1)
  expect(error.mock.calls[0][0]).toEqual(expect.stringMatching(/^\[pipeline-jobs\] retention failed: /))

  stop()
  jest.advanceTimersByTime(86_400_000)
  expect(log).toHaveBeenCalledTimes(2)
  expect(error).toHaveBeenCalledTimes(1)
})
