import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import Database from 'better-sqlite3';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const GRAPH_EXPORT_SCRIPT = path.resolve(__dirname, '..', 'scripts', 'graph_export.py');

function createTestDb(filename, setupRows) {
  const db = new Database(filename);

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
      abstract TEXT,
      crossref_json TEXT,
      openalex_json TEXT,
      metadata_status TEXT,
      download_status TEXT,
      source_work_id INTEGER,
      relationship_type TEXT,
      run_id INTEGER,
      origin_key TEXT,
      source_pdf TEXT
    )
  `);

  setupRows(db);
  db.close();
}

function runGraphExport(dbPath, extraArgs = []) {
  const args = [
    GRAPH_EXPORT_SCRIPT,
    '--db-path',
    dbPath,
    '--max-nodes',
    '200',
    '--relationship',
    'both',
    '--status',
    'all',
    '--hide-isolates',
    '0',
    ...extraArgs,
  ];

  const proc = spawnSync('python3', args, { encoding: 'utf8' });
  if (proc.status !== 0) {
    throw new Error(`graph_export.py failed: ${proc.stderr}`);
  }
  return JSON.parse(proc.stdout);
}

describe('graph_export fallback edge mapping', () => {
  test('maps source_work_id by row id and openalex id', () => {
    const dbPath = path.join(os.tmpdir(), `graph-export-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);

    try {
      createTestDb(dbPath, (db) => {
        const insert = db.prepare(`
          INSERT INTO works
          (title, type, year, doi, normalized_doi, openalex_id, normalized_title, normalized_authors, metadata_status, download_status, source_work_id, relationship_type, run_id)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);

        insert.run('Source work', 'journal-article', 2020, '10.1/source', '10.1/source', 'W1', 'source work', 'a1', 'matched', 'not_requested', null, null, 1);
        insert.run('Row-id edge target', 'journal-article', 2021, '10.1/row', '10.1/row', 'W2', 'row-id edge target', 'a2', 'matched', 'not_requested', 1, 'references', 1);
        insert.run('OpenAlex edge target', 'journal-article', 2022, '10.1/oa', '10.1/oa', 'W3', 'openalex edge target', 'a3', 'matched', 'not_requested', 'W1', 'cited_by', 1);
      });

      const payload = runGraphExport(dbPath);

      expect(payload.nodes).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 'W1', title: 'Source work' }),
          expect.objectContaining({ id: 'W2', title: 'Row-id edge target' }),
          expect.objectContaining({ id: 'W3', title: 'OpenAlex edge target' }),
        ])
      );

      expect(payload.edges).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ source: 'W1', target: 'W2', relationship_type: 'references' }),
          expect.objectContaining({ source: 'W1', target: 'W3', relationship_type: 'cited_by' }),
        ])
      );
    } finally {
      fs.unlinkSync(dbPath);
    }
  });
});
