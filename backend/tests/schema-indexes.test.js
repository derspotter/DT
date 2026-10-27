import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import Database from 'better-sqlite3'
import { ensureAuthSchema } from '../src/app.js'

const NEW = ['idx_works_state', 'idx_works_year_id', 'idx_corpus_works_added']
const DROPPED = [
  'idx_corpus_works_lookup',
  'idx_corpus_works_corpus',
  'idx_work_aliases_norm_title',
  'idx_work_aliases_owner',
  'idx_search_results_run',
]

test('ensureAuthSchema creates the state/year/added indexes and drops the duplicates, twice', () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-schema-indexes-'))
  const db = new Database(path.join(tmpDir, 'idx.db'))
  try {
    db.exec(`
      CREATE TABLE corpus_works (corpus_id INTEGER, work_id INTEGER, added_at TIMESTAMP, PRIMARY KEY (corpus_id, work_id));
      CREATE INDEX idx_corpus_works_lookup ON corpus_works(corpus_id, work_id);
      CREATE INDEX idx_corpus_works_corpus ON corpus_works(corpus_id);
      CREATE TABLE work_aliases (id INTEGER PRIMARY KEY, owner TEXT, normalized_alias_title TEXT);
      CREATE INDEX idx_work_aliases_norm_title ON work_aliases(normalized_alias_title);
      CREATE INDEX idx_work_aliases_owner ON work_aliases(owner);
    `)
    ensureAuthSchema(db)
    ensureAuthSchema(db)
    const names = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='index'").all().map((r) => r.name))
    for (const n of NEW) expect(names.has(n)).toBe(true)
    for (const n of DROPPED) expect(names.has(n)).toBe(false)
  } finally {
    db.close()
    fs.rmSync(tmpDir, { recursive: true, force: true })
  }
})
