import express from 'express';
import cors from 'cors';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { spawn, spawnSync } from 'child_process';
import Database from 'better-sqlite3';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function findUpwards(startDir, childName) {
  // Walk up parents until we find a directory or file named `childName`.
  // This keeps paths working both in Docker (/usr/src/app/...) and locally.
  let current = startDir;
  while (true) {
    const candidate = path.join(current, childName);
    if (fs.existsSync(candidate)) {
      return candidate;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function isValidDlLitProjectDir(candidate) {
  if (!candidate) return false;
  const dlLitDir = path.join(candidate, 'dl_lit');
  const dbManagerFile = path.join(dlLitDir, 'db_manager.py');
  return fs.existsSync(dlLitDir) && fs.existsSync(dbManagerFile);
}

function findDlLitProjectDir(startDir) {
  let current = startDir;
  while (true) {
    const direct = path.basename(current) === 'dl_lit_project' ? current : null;
    if (isValidDlLitProjectDir(direct)) {
      return direct;
    }
    const nested = path.join(current, 'dl_lit_project');
    if (isValidDlLitProjectDir(nested)) {
      return nested;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function resolveUploadsDir(repoRoot) {
  const fromEnv = process.env.RAG_FEEDER_UPLOADS_DIR;
  if (fromEnv) {
    return fromEnv;
  }
  const containerRoot = '/usr/src/app';
  if (fs.existsSync(containerRoot)) {
    return path.join(containerRoot, 'uploads');
  }
  return path.join(repoRoot, 'uploads');
}

// --- Define Container Paths ---
const envProjectDir = process.env.RAG_FEEDER_DL_LIT_PROJECT_DIR;
const fallbackProjectDir = path.join(__dirname, '..', '..', 'dl_lit_project');
const DL_LIT_PROJECT_DIR =
  [envProjectDir, findDlLitProjectDir(__dirname), findUpwards(__dirname, 'dl_lit_project'), fallbackProjectDir]
    .find((dir) => isValidDlLitProjectDir(dir)) || fallbackProjectDir;
const DL_LIT_CODE_DIR = path.join(DL_LIT_PROJECT_DIR, 'dl_lit');
const REPO_ROOT = path.dirname(DL_LIT_PROJECT_DIR);
const UPLOADS_DIR = resolveUploadsDir(REPO_ROOT);
const ARTIFACTS_DIR = process.env.RAG_FEEDER_ARTIFACTS_DIR || path.join(DL_LIT_PROJECT_DIR, 'artifacts');
const BIB_OUTPUT_DIR = path.join(ARTIFACTS_DIR, 'bibliographies');
const GET_BIB_PAGES_SCRIPT = path.join(DL_LIT_CODE_DIR, 'get_bib_pages.py');
const API_SCRAPER_SCRIPT = path.join(DL_LIT_CODE_DIR, 'APIscraper_v2.py');
const DEFAULT_DB_PATH = path.join(DL_LIT_PROJECT_DIR, 'data', 'literature.db');
const DB_PATH = process.env.RAG_FEEDER_DB_PATH || DEFAULT_DB_PATH;
const PYTHON_SCRIPTS_DIR = path.join(__dirname, '..', 'scripts');
const KEYWORD_SEARCH_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'keyword_search.py');
const CORPUS_LIST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'corpus_list.py');
const DOWNLOADS_LIST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'downloads_list.py');
const GRAPH_EXPORT_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'graph_export.py');
const INGEST_LATEST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_latest.py');
const INGEST_ENQUEUE_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_enqueue.py');
const INGEST_MARK_NEXT_RAW_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_mark_next_raw.py');
const INGEST_PROCESS_MARKED_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_process_marked.py');
const INGEST_RUNS_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_runs.py');
const INGEST_STATS_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_stats.py');
const DOWNLOAD_WORKER_ONCE_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'download_worker_once.py');
const EXPORT_BUNDLE_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'export_bundle.py');
const PYTHON_EXEC = process.env.RAG_FEEDER_PYTHON || 'python3';

function isStubMode() {
  return process.env.RAG_FEEDER_STUB === '1';
}

function resolveAuthConfig() {
  if (isStubMode()) {
    return {
      jwtSecret: 'stub-jwt-secret',
      adminUsername: 'stub-admin',
      adminPassword: 'stub-password',
    };
  }

  const jwtSecret = String(process.env.RAG_FEEDER_JWT_SECRET || '').trim();
  const adminUsername = String(process.env.RAG_ADMIN_USER || '').trim();
  const adminPassword = String(process.env.RAG_ADMIN_PASSWORD || '');
  const missing = [];

  if (!jwtSecret) missing.push('RAG_FEEDER_JWT_SECRET');
  if (!adminUsername) missing.push('RAG_ADMIN_USER');
  if (!adminPassword) missing.push('RAG_ADMIN_PASSWORD');

  if (missing.length) {
    throw new Error(`[auth] Missing required environment variables: ${missing.join(', ')}`);
  }

  return { jwtSecret, adminUsername, adminPassword };
}

const STUB_RESULTS = {
  keywordResults: [
    {
      id: 'W2175056322',
      openalex_id: 'W2175056322',
      title: 'The New Institutional Economics',
      authors: 'Ronald H. Coase',
      year: 2002,
      type: 'book-chapter',
      doi: '10.1017/cbo9780511613807.002',
    },
    {
      id: 'W2015930340',
      openalex_id: 'W2015930340',
      title: 'The Nature of the Firm',
      authors: 'Ronald H. Coase',
      year: 1937,
      type: 'journal-article',
      doi: '10.1111/j.1468-0335.1937.tb00002.x',
    },
  ],
  corpus: [
    {
      id: 1,
      title: 'The New Institutional Economics',
      year: 2002,
      source: 'Seed bibliography',
      status: 'with_metadata',
      doi: '10.1017/cbo9780511613807.002',
      openalex_id: 'W2175056322',
    },
    {
      id: 2,
      title: 'Markets and Hierarchies',
      year: 1975,
      source: 'Keyword search',
      status: 'to_download_references',
      doi: null,
      openalex_id: 'W1974636593',
    },
  ],
  downloads: [
    {
      id: 101,
      title: 'The New Institutional Economics',
      status: 'queued',
      attempts: 0,
    },
    {
      id: 102,
      title: 'Markets and Hierarchies',
      status: 'in_progress',
      attempts: 1,
    },
  ],
  graph: {
    nodes: [
      { id: 'W2175056322', title: 'The New Institutional Economics', year: 2002, type: 'book-chapter' },
      { id: 'W2015930340', title: 'The Nature of the Firm', year: 1937, type: 'journal-article' },
    ],
    edges: [{ source: 'W2175056322', target: 'W2015930340', relationship_type: 'references' }],
    stats: {
      node_count: 2,
      edge_count: 1,
      relationship_counts: { references: 1, cited_by: 0 },
    },
  },
};

function tableExists(db, tableName) {
  const row = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(tableName);
  return Boolean(row);
}

function ensureColumn(db, tableName, columnName, columnType) {
  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all().map((row) => row.name);
  if (!columns.includes(columnName)) {
    db.prepare(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnType}`).run();
  }
}

function ensureAuthSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      last_corpus_id INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS corpora (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      owner_user_id INTEGER NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS user_corpora (
      user_id INTEGER NOT NULL,
      corpus_id INTEGER NOT NULL,
      role TEXT DEFAULT 'viewer',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (user_id, corpus_id)
    );
    CREATE TABLE IF NOT EXISTS corpus_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      corpus_id INTEGER NOT NULL,
      table_name TEXT NOT NULL,
      row_id INTEGER NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(corpus_id, table_name, row_id)
    );
    CREATE TABLE IF NOT EXISTS ingest_source_metadata (
      corpus_id INTEGER NOT NULL DEFAULT 0,
      ingest_source TEXT NOT NULL,
      source_pdf TEXT,
      title TEXT,
      authors TEXT,
      year INTEGER,
      doi TEXT,
      source TEXT,
      publisher TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (corpus_id, ingest_source)
    );
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
  `);
  if (tableExists(db, 'ingest_entries')) {
    ensureColumn(db, 'ingest_entries', 'corpus_id', 'INTEGER');
  }
  if (tableExists(db, 'pipeline_jobs')) {
    ensureColumn(db, 'pipeline_jobs', 'worker_id', 'TEXT');
  }
  db.exec(`CREATE INDEX IF NOT EXISTS idx_corpus_items_lookup ON corpus_items(corpus_id, table_name, row_id);`);
}

function bootstrapDefaultCorpus(db, authConfig) {
  const admin = db.prepare('SELECT id, last_corpus_id FROM users WHERE username = ?').get(authConfig.adminUsername);
  let adminId = admin?.id;
  if (!adminId) {
    const hash = bcrypt.hashSync(authConfig.adminPassword, 10);
    const result = db
      .prepare('INSERT OR IGNORE INTO users (username, password_hash) VALUES (?, ?)')
      .run(authConfig.adminUsername, hash);
    if (result.changes === 0) {
      adminId = db.prepare('SELECT id FROM users WHERE username = ?').get(authConfig.adminUsername)?.id;
    } else {
      adminId = result.lastInsertRowid;
    }
  }

  let corpus = db
    .prepare('SELECT id FROM corpora WHERE owner_user_id = ? ORDER BY id LIMIT 1')
    .get(adminId);
  if (!corpus) {
    const result = db
      .prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)')
      .run('Default Corpus', adminId);
    corpus = { id: result.lastInsertRowid };
  }

  db.prepare(
    "INSERT OR IGNORE INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')"
  ).run(adminId, corpus.id);

  if (!admin?.last_corpus_id) {
    db.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpus.id, adminId);
  }

  return corpus.id;
}

function migrateExistingToCorpus(db, corpusId) {
  const tables = [
    'no_metadata',
    'with_metadata',
    'to_download_references',
    'downloaded_references',
  ];
  tables.forEach((tableName) => {
    if (!tableExists(db, tableName)) return;
    db.exec(
      `INSERT OR IGNORE INTO corpus_items (corpus_id, table_name, row_id)
       SELECT ${corpusId}, '${tableName}', id FROM ${tableName}`
    );
  });

  if (tableExists(db, 'ingest_entries')) {
    db.prepare('UPDATE ingest_entries SET corpus_id = ? WHERE corpus_id IS NULL').run(corpusId);
  }
}

function pruneStaleCorpusItems(db) {
  const managedTables = ['no_metadata', 'with_metadata', 'to_download_references', 'downloaded_references'];
  managedTables.forEach((tableName) => {
    if (!tableExists(db, tableName)) return;
    db.exec(
      `DELETE FROM corpus_items
        WHERE table_name = '${tableName}'
          AND NOT EXISTS (
            SELECT 1 FROM ${tableName} t
             WHERE t.id = corpus_items.row_id
          )`
    );
  });
}

function listCorporaForUser(db, userId) {
  return db
    .prepare(
      `SELECT c.id, c.name, c.owner_user_id, uc.role, c.created_at
       FROM corpora c
       JOIN user_corpora uc ON uc.corpus_id = c.id
       WHERE uc.user_id = ?
       ORDER BY c.created_at DESC`
    )
    .all(userId);
}

function getCorpusRoleForUser(db, userId, corpusId) {
  if (!userId || !corpusId) return null;
  return db
    .prepare('SELECT role FROM user_corpora WHERE user_id = ? AND corpus_id = ?')
    .get(userId, corpusId)?.role;
}

function getUserById(db, userId) {
  return db
    .prepare('SELECT id, username, last_corpus_id FROM users WHERE id = ?')
    .get(userId);
}

function requireAuth(db, jwtSecret) {
  return (req, res, next) => {
    if (isStubMode()) {
      req.user = { id: 0, username: 'stub', last_corpus_id: null };
      req.corpusId = null;
      return next();
    }
    const header = req.headers.authorization || '';
    const token = header.startsWith('Bearer ') ? header.slice(7) : null;
    if (!token) {
      return res.status(401).json({ error: 'Missing Authorization header' });
    }
    try {
      const payload = jwt.verify(token, jwtSecret);
      const user = getUserById(db, payload.sub);
      if (!user) {
        return res.status(401).json({ error: 'Unknown user' });
      }
      req.user = user;
      req.corpusId = user.last_corpus_id || null;
      return next();
    } catch (error) {
      return res.status(401).json({ error: 'Invalid token' });
    }
  };
}

function parseJsonFromOutput(output) {
  const lines = String(output || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const tryParse = (text) => {
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      return null;
    }
  };

  // Fast path: find a valid JSON line near the bottom while skipping log lines
  // such as "[DB Manager] ...", which are not JSON arrays.
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i];
    const bracketIndex = line.search(/[\[{]/);
    if (bracketIndex < 0) continue;
    const inlineCandidate = line.slice(bracketIndex).trim();
    const parsedInline = tryParse(inlineCandidate);
    if (parsedInline !== null) return parsedInline;
  }

  // Fallback: recover pretty-printed/multiline JSON blocks from stdout.
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i];
    const bracketIndex = line.search(/[\[{]/);
    if (bracketIndex < 0) continue;
    for (let j = lines.length; j > i; j -= 1) {
      const segment = lines.slice(i, j);
      segment[0] = segment[0].slice(bracketIndex);
      const parsedSegment = tryParse(segment.join('\n'));
      if (parsedSegment !== null) return parsedSegment;
    }
  }

  throw new Error(`No valid JSON output from python. Output was: ${output}`);
}

function normalizeIngestSourceMetadata(value) {
  if (!value || typeof value !== 'object') return null;
  const out = {};
  if (typeof value.title === 'string' && value.title.trim()) out.title = value.title.trim();
  if (Array.isArray(value.authors)) {
    const authors = value.authors
      .map((item) => String(item || '').trim())
      .filter(Boolean);
    if (authors.length > 0) out.authors = authors;
  }
  if (typeof value.year === 'number' && Number.isFinite(value.year)) {
    out.year = Math.trunc(value.year);
  } else if (typeof value.year === 'string' && value.year.trim()) {
    out.year = value.year.trim();
  }
  if (typeof value.doi === 'string' && value.doi.trim()) out.doi = value.doi.trim();
  if (typeof value.source === 'string' && value.source.trim()) out.source = value.source.trim();
  if (typeof value.publisher === 'string' && value.publisher.trim()) out.publisher = value.publisher.trim();
  return Object.keys(out).length > 0 ? out : null;
}

function upsertIngestSourceMetadata(db, { corpusId, ingestSource, sourcePdf, sourceMetadata }) {
  if (!ingestSource || !sourceMetadata) return false;
  const corpusKey = Number.isFinite(Number(corpusId)) ? Number(corpusId) : 0;
  let yearInt = null;
  if (Number.isInteger(sourceMetadata.year)) {
    yearInt = sourceMetadata.year;
  } else if (typeof sourceMetadata.year === 'string' && /^\d+$/.test(sourceMetadata.year.trim())) {
    yearInt = Number(sourceMetadata.year.trim());
  }
  const authorsJson = Array.isArray(sourceMetadata.authors)
    ? JSON.stringify(sourceMetadata.authors)
    : null;

  db.prepare(
    `INSERT INTO ingest_source_metadata
      (corpus_id, ingest_source, source_pdf, title, authors, year, doi, source, publisher)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(corpus_id, ingest_source)
     DO UPDATE SET
       source_pdf = excluded.source_pdf,
       title = COALESCE(excluded.title, ingest_source_metadata.title),
       authors = COALESCE(excluded.authors, ingest_source_metadata.authors),
       year = COALESCE(excluded.year, ingest_source_metadata.year),
       doi = COALESCE(excluded.doi, ingest_source_metadata.doi),
       source = COALESCE(excluded.source, ingest_source_metadata.source),
       publisher = COALESCE(excluded.publisher, ingest_source_metadata.publisher),
       updated_at = CURRENT_TIMESTAMP`
  ).run(
    corpusKey,
    String(ingestSource),
    sourcePdf || null,
    sourceMetadata.title || null,
    authorsJson,
    yearInt,
    sourceMetadata.doi || null,
    sourceMetadata.source || null,
    sourceMetadata.publisher || null
  );
  return true;
}

function coercePositiveInt(value, fallback, minValue = 1) {
  const normalized = coerceInt(value, fallback);
  if (!Number.isFinite(normalized)) {
    return fallback;
  }
  return normalized >= minValue ? normalized : minValue;
}

function buildKeywordSearchExpansion(body = {}) {
  const explicitDepth = coerceInt(body?.relatedDepth, null);
  const parsed = {
    relatedDepthDownstream: coercePositiveInt(
      coerceInt(body?.relatedDepthDownstream, explicitDepth ?? null),
      RECURSION_DEFAULTS.keyword.relatedDepthDownstream,
      1
    ),
    relatedDepthUpstream: coercePositiveInt(
      coerceInt(body?.relatedDepthUpstream, explicitDepth ?? null),
      RECURSION_DEFAULTS.keyword.relatedDepthUpstream,
      1
    ),
    maxRelated: coercePositiveInt(body?.maxRelated, RECURSION_DEFAULTS.keyword.maxRelated, 1),
    includeDownstream: body?.includeDownstream === undefined
      ? RECURSION_DEFAULTS.keyword.includeDownstream
      : Boolean(body.includeDownstream),
    includeUpstream: body?.includeUpstream === undefined
      ? RECURSION_DEFAULTS.keyword.includeUpstream
      : Boolean(body.includeUpstream),
  };
  return parsed;
}

function applyKeywordSearchExpansionArgs(args, expansion) {
  if (expansion.includeDownstream || expansion.includeUpstream) {
    args.push('--related-depth-downstream', String(expansion.relatedDepthDownstream));
    if (!expansion.includeDownstream) {
      args.push('--no-include-downstream');
    }
    if (expansion.includeUpstream) {
      args.push('--include-upstream');
      args.push('--related-depth-upstream', String(expansion.relatedDepthUpstream));
    }
  } else {
    args.push('--related-depth', String(1));
    args.push('--no-include-downstream');
  }
  args.push('--max-related', String(expansion.maxRelated));
}

function buildUploadedDocsExpansion(body = {}) {
  const explicitDepth = coerceInt(body?.relatedDepth, null)
  return {
    relatedDepth: coercePositiveInt(
      explicitDepth,
      RECURSION_DEFAULTS.uploadedDocs.relatedDepth,
      1
    ),
    relatedDepthDownstream: coercePositiveInt(
      coerceInt(body?.relatedDepthDownstream, explicitDepth ?? null),
      RECURSION_DEFAULTS.uploadedDocs.relatedDepthDownstream,
      1
    ),
    relatedDepthUpstream: coercePositiveInt(
      coerceInt(body?.relatedDepthUpstream, explicitDepth ?? null),
      RECURSION_DEFAULTS.uploadedDocs.relatedDepthUpstream,
      1
    ),
    maxRelated: coercePositiveInt(
      body?.maxRelated,
      RECURSION_DEFAULTS.uploadedDocs.maxRelated,
      1
    ),
    includeDownstream: body?.includeDownstream === undefined
      ? RECURSION_DEFAULTS.uploadedDocs.includeDownstream
      : Boolean(body.includeDownstream),
    includeUpstream: body?.includeUpstream === undefined
      ? RECURSION_DEFAULTS.uploadedDocs.includeUpstream
      : Boolean(body.includeUpstream),
  };
}

function applyUploadedDocsExpansionArgs(args, expansion) {
  if (expansion.includeDownstream || expansion.includeUpstream) {
    args.push('--related-depth-downstream', String(expansion.relatedDepthDownstream));
    if (!expansion.includeDownstream) {
      args.push('--no-include-downstream');
    }
    if (expansion.includeUpstream) {
      args.push('--include-upstream');
      args.push('--related-depth-upstream', String(expansion.relatedDepthUpstream));
    } else {
      args.push('--no-include-upstream');
    }
  } else {
    args.push('--related-depth-downstream', String(1));
    args.push('--no-include-downstream');
    args.push('--no-include-upstream');
  }
  args.push('--max-related', String(expansion.maxRelated));
}

function runPythonJson(scriptPath, args, { dbPath, corpusId } = {}) {
  const { done } = spawnPythonJson(scriptPath, args, { dbPath, corpusId });
  return done;
}

function spawnPythonJson(scriptPath, args, { dbPath, corpusId, onStdoutLine, onStderrLine } = {}) {
  const env = {
    ...process.env,
    PYTHONPATH: [DL_LIT_PROJECT_DIR, REPO_ROOT, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
    RAG_FEEDER_DL_LIT_PROJECT_DIR: DL_LIT_PROJECT_DIR,
    RAG_FEEDER_DB_PATH: dbPath || DB_PATH,
  };

  const finalArgs = [...args];
  if (corpusId !== undefined && corpusId !== null) {
    finalArgs.push('--corpus-id', String(corpusId));
  }

  const child = spawn(PYTHON_EXEC, [scriptPath, ...finalArgs], { env });
  let stdout = '';
  let stderr = '';
  let stdoutBuf = '';
  let stderrBuf = '';

  const flushLines = (buffer, handler, flushPartial = false) => {
    if (typeof handler !== 'function') {
      return buffer;
    }
    const parts = String(buffer).split(/\r?\n/);
    const pending = parts.pop() ?? '';
    parts
      .map((line) => line.trim())
      .filter(Boolean)
      .forEach((line) => handler(line));
    if (flushPartial && pending.trim()) {
      handler(pending.trim());
      return '';
    }
    return pending;
  };

  child.stdout.on('data', (data) => {
    const text = data.toString();
    stdout += text;
    stdoutBuf += text;
    stdoutBuf = flushLines(stdoutBuf, onStdoutLine);
  });

  child.stderr.on('data', (data) => {
    const text = data.toString();
    stderr += text;
    stderrBuf += text;
    stderrBuf = flushLines(stderrBuf, onStderrLine);
  });

  const done = new Promise((resolve, reject) => {
    child.on('close', (code) => {
      stdoutBuf = flushLines(stdoutBuf, onStdoutLine, true);
      stderrBuf = flushLines(stderrBuf, onStderrLine, true);
      if (code !== 0) {
        return reject(new Error(`Python script failed (${code}): ${stderr || stdout}`));
      }
      try {
        const payload = parseJsonFromOutput(stdout);
        return resolve(payload);
      } catch (error) {
        return reject(error);
      }
    });
  });

  return { child, done };
}

function coerceInt(value, fallback = null) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function coerceBoolEnv(value, fallback) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'off'].includes(normalized)) {
    return false;
  }
  return fallback;
}

const RECURSION_DEFAULTS = {
  keyword: {
    relatedDepthDownstream: Math.max(1, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH, 1) || 1),
    relatedDepthUpstream: Math.max(1, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH_UPSTREAM, 1) || 1),
    maxRelated: Math.max(1, coerceInt(process.env.RAG_FEEDER_MAX_RELATED, 30) || 30),
    includeDownstream: coerceBoolEnv(process.env.RAG_FEEDER_INCLUDE_DOWNSTREAM, true),
    includeUpstream: coerceBoolEnv(process.env.RAG_FEEDER_INCLUDE_UPSTREAM, false),
  },
  uploadedDocs: {
    relatedDepth: Math.max(1, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH, 1) || 1),
    relatedDepthDownstream: Math.max(1, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH, 1) || 1),
    relatedDepthUpstream: Math.max(1, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH_UPSTREAM, 1) || 1),
    maxRelated: Math.max(1, coerceInt(process.env.RAG_FEEDER_MAX_RELATED, 30) || 30),
    includeDownstream: coerceBoolEnv(process.env.RAG_FEEDER_INCLUDE_DOWNSTREAM, true),
    includeUpstream: coerceBoolEnv(process.env.RAG_FEEDER_INCLUDE_UPSTREAM, false),
  },
};

function getLogSettings() {
  const logDir = process.env.RAG_FEEDER_LOG_DIR || path.join(REPO_ROOT, 'logs');
  const maxFiles = Math.max(0, coerceInt(process.env.RAG_FEEDER_LOG_MAX_FILES, 5) || 5);
  return {
    logDir,
    maxFiles,
    appLogFile: path.join(logDir, 'backend-app.log'),
    pipelineLogFile: path.join(logDir, 'backend-pipeline.log'),
  };
}

function readTailLinesFromFile(filePath, maxLines) {
  try {
    if (!filePath || !fs.existsSync(filePath) || maxLines <= 0) return [];
    const raw = fs.readFileSync(filePath, 'utf8');
    if (!raw) return [];
    const lines = raw
      .split(/\r?\n/)
      .map((line) => line.trimEnd())
      .filter(Boolean);
    return lines.slice(-maxLines);
  } catch (error) {
    console.warn(`[logs/tail] Failed reading ${filePath}: ${error?.message || error}`);
    return [];
  }
}

function readAllLogLines(baseFilePath, { includeRotated = true, maxFiles = 5 } = {}) {
  const lines = [];
  if (includeRotated) {
    // Load oldest rotated files first for chronological order.
    for (let i = maxFiles; i >= 1; i -= 1) {
      const backupFile = `${baseFilePath}.${i}`;
      lines.push(...readTailLinesFromFile(backupFile, Number.MAX_SAFE_INTEGER));
    }
  }
  lines.push(...readTailLinesFromFile(baseFilePath, Number.MAX_SAFE_INTEGER));
  return lines;
}

function extractLogCorpusTag(line) {
  const match = String(line || '').match(/\bcorpus=([a-zA-Z0-9_-]+)/);
  return match ? match[1] : '';
}

function readLogWindow(
  baseFilePath,
  {
    limit = 200,
    cursor = 0,
    includeRotated = true,
    maxFiles = 5,
    corpusId = null,
  } = {}
) {
  let allLines = readAllLogLines(baseFilePath, { includeRotated, maxFiles });
  const requestedCorpusTag = String(corpusId ?? '').trim();
  if (requestedCorpusTag) {
    allLines = allLines.filter((line) => {
      const tag = extractLogCorpusTag(line);
      if (!tag) {
        return false;
      }
      return tag === requestedCorpusTag;
    });
  }
  const totalLines = allLines.length;
  const safeLimit = Math.max(1, Number(limit) || 200);
  const safeCursor = Math.max(0, Math.min(Number(cursor) || 0, totalLines));
  const end = Math.max(0, totalLines - safeCursor);
  const start = Math.max(0, end - safeLimit);
  const entries = allLines.slice(start, end);
  return {
    entries,
    totalLines,
    hasMore: start > 0,
    nextCursor: safeCursor + entries.length,
    cursor: safeCursor,
  };
}

function getPdfPageCountSync(pdfPath) {
  if (!pdfPath || !fs.existsSync(pdfPath)) return null;
  const probeCode = [
    'import sys',
    'import pikepdf',
    'pdf = pikepdf.Pdf.open(sys.argv[1])',
    'print(len(pdf.pages))',
    'pdf.close()',
  ].join('; ');
  try {
    const result = spawnSync(PYTHON_EXEC, ['-c', probeCode, pdfPath], {
      env: {
        ...process.env,
        PYTHONPATH: [DL_LIT_PROJECT_DIR, REPO_ROOT, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
      },
      encoding: 'utf8',
    });
    if (result.status !== 0) {
      console.warn(`[/api/extract-bibliography] Could not read page count for ${pdfPath}: ${result.stderr || result.stdout}`);
      return null;
    }
    const parsed = Number.parseInt(String(result.stdout || '').trim(), 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  } catch (error) {
    console.warn(`[/api/extract-bibliography] Page count probe failed for ${pdfPath}: ${error?.message || error}`);
    return null;
  }
}

function geminiUploadPdfSync(pdfPath) {
  const code = [
    'import json',
    'import os',
    'import shutil',
    'import sys',
    'import tempfile',
    'import unicodedata',
    'from google import genai',
    'key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")',
    'assert key, "Missing GEMINI_API_KEY/GOOGLE_API_KEY"',
    'client = genai.Client(api_key=key)',
    'pdf_path = sys.argv[1]',
    'base = os.path.basename(pdf_path)',
    'norm = unicodedata.normalize("NFKD", base).encode("ascii", "ignore").decode("ascii")',
    'safe = "".join((c if (c.isalnum() or c in "._-") else "_") for c in norm).strip("._") or "upload"',
    'if not safe.lower().endswith(".pdf"):',
    '    safe += ".pdf"',
    'is_ascii = all(ord(c) < 128 for c in pdf_path) and all(ord(c) < 128 for c in base)',
    'tmpdir = None',
    'upload_path = pdf_path',
    'display_name = base',
    'if not is_ascii:',
    '    tmpdir = tempfile.mkdtemp(prefix="gemini_upload_")',
    '    upload_path = os.path.join(tmpdir, safe)',
    '    shutil.copy2(pdf_path, upload_path)',
    '    display_name = safe',
    'f = client.files.upload(file=upload_path, config={"mime_type":"application/pdf","display_name":display_name})',
    'print(json.dumps({"name": f.name}))',
    'if tmpdir:',
    '    shutil.rmtree(tmpdir, ignore_errors=True)',
  ].join('\n');
  const result = spawnSync(PYTHON_EXEC, ['-c', code, pdfPath], {
    env: {
      ...process.env,
      PYTHONPATH: [DL_LIT_PROJECT_DIR, REPO_ROOT, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
    },
    encoding: 'utf8',
  });
  if (result.status !== 0) {
    throw new Error((result.stderr || result.stdout || 'Gemini upload failed').trim());
  }
  const parsed = parseJsonFromOutput(result.stdout || '');
  if (!parsed?.name) {
    throw new Error(`Gemini upload returned invalid payload: ${result.stdout}`);
  }
  return parsed.name;
}

function geminiDeleteUploadedSync(uploadedFileName) {
  if (!uploadedFileName) return;
  const code = [
    'import os, sys',
    'from google import genai',
    'key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")',
    'assert key, "Missing GEMINI_API_KEY/GOOGLE_API_KEY"',
    'client = genai.Client(api_key=key)',
    'client.files.delete(name=sys.argv[1])',
    'print("ok")',
  ].join('; ');
  const result = spawnSync(PYTHON_EXEC, ['-c', code, uploadedFileName], {
    env: {
      ...process.env,
      PYTHONPATH: [DL_LIT_PROJECT_DIR, REPO_ROOT, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
    },
    encoding: 'utf8',
  });
  if (result.status !== 0) {
    console.warn(
      `[/api/extract-bibliography] Failed to delete uploaded Gemini file ${uploadedFileName}: ${result.stderr || result.stdout}`
    );
  }
}

export function createApp({ broadcast, broadcastEvent } = {}) {
  const app = express();
  const send = typeof broadcast === 'function' ? broadcast : () => {};
  const sendEvent = typeof broadcastEvent === 'function' ? broadcastEvent : () => {};
  const downloadWorkers = new Map(); // corpusId -> state
  const pipelineWorkers = new Map(); // corpusId -> state
  const authConfig = resolveAuthConfig();

  // Ensure temporary and output directories exist
  fs.mkdirSync(ARTIFACTS_DIR, { recursive: true });
  fs.mkdirSync(BIB_OUTPUT_DIR, { recursive: true });

  fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
  const authDb = new Database(DB_PATH, { timeout: 3_000 });
  // Test suites spin up multiple app instances in parallel; these settings
  // reduce transient SQLITE_BUSY / "database is locked" bootstrapping failures.
  try {
    authDb.pragma('journal_mode = WAL');
  } catch (error) {
    // keep default journal mode if WAL is unavailable
  }
  try {
    authDb.pragma('busy_timeout = 3000');
  } catch (error) {
    // timeout option already helps; pragma is best-effort
  }
  ensureAuthSchema(authDb);
  const defaultCorpusId = bootstrapDefaultCorpus(authDb, authConfig);
  migrateExistingToCorpus(authDb, defaultCorpusId);
  pruneStaleCorpusItems(authDb);
  const requireAuthMiddleware = requireAuth(authDb, authConfig.jwtSecret);
  const hasWorkerAccess = (req) => {
    if (isStubMode()) return true;
    const role = getCorpusRoleForUser(authDb, req.user?.id, req.corpusId);
    return role === 'owner' || role === 'editor';
  };

  // Ensure uploads directory exists (already present, keeping for clarity)
  const uploadDir = UPLOADS_DIR;
  if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir, { recursive: true });
  }

  function getOrInitWorkerState(corpusId) {
    const key = corpusId || 0;
    if (!downloadWorkers.has(key)) {
      downloadWorkers.set(key, {
        running: false,
        intervalId: null,
        inFlight: false,
        child: null,
        startedAt: null,
        lastTickAt: null,
        lastResult: null,
        lastError: null,
        config: { intervalSeconds: 60, batchSize: 3 },
      });
    }
    return downloadWorkers.get(key);
  }

  function getOrInitPipelineWorkerState(corpusId) {
    const key = corpusId || 0;
    if (!pipelineWorkers.has(key)) {
      pipelineWorkers.set(key, {
        running: false,
        intervalId: null,
        inFlight: false,
        children: [],
        startedAt: null,
        lastTickAt: null,
        lastResult: null,
        lastError: null,
        resolvedDownloadWorkers: 0,
        config: {
          intervalSeconds: 15,
          promoteBatchSize: 25,
          promoteWorkers: 6,
          downloadBatchSize: 5,
          downloadWorkers: 0, // 0 = auto
        },
      });
    }
    return pipelineWorkers.get(key);
  }

  function tickDownloadWorker(corpusId) {
    const state = getOrInitWorkerState(corpusId);
    if (!state.running || state.inFlight) return;
    state.inFlight = true;
    state.lastTickAt = new Date().toISOString();
    state.lastError = null;
    const { batchSize } = state.config;
    const corpusTag = corpusId || 'none';
    
    try {
      enqueuePipelineJob({
        corpusId,
        jobType: 'download',
        parameters: { batchSize },
        dedupeStatuses: ['pending', 'running'],
      });
    } catch (error) {
      state.lastError = error?.message || String(error);
      console.error(`[download-worker ERROR] corpus=${corpusTag} ${state.lastError}`);
    } finally {
      state.inFlight = false;
    }
  }

  function startDownloadWorker(corpusId, config = {}) {
    const state = getOrInitWorkerState(corpusId);
    const intervalSeconds = coerceInt(config.intervalSeconds, state.config.intervalSeconds) || state.config.intervalSeconds;
    const batchSize = coerceInt(config.batchSize, state.config.batchSize) || state.config.batchSize;

    state.config = { intervalSeconds, batchSize };
    if (state.running) return state;

    state.running = true;
    state.startedAt = new Date().toISOString();
    // Kick once immediately, then on interval.
    tickDownloadWorker(corpusId);
    state.intervalId = setInterval(() => tickDownloadWorker(corpusId), intervalSeconds * 1000);
    return state;
  }

  function stopDownloadWorker(corpusId, { force = false } = {}) {
    const state = getOrInitWorkerState(corpusId);
    state.running = false;
    if (state.intervalId) {
      clearInterval(state.intervalId);
      state.intervalId = null;
    }
    return state;
  }

  function cancelDownloadJobs(corpusId, { includeRunning = true, reason = 'stopped_by_user' } = {}) {
    if (!tableExists(authDb, 'pipeline_jobs')) {
      return { pending: 0, running: 0 };
    }
    const resultPayload = JSON.stringify({
      cancelled: true,
      reason: String(reason || 'stopped_by_user'),
      at: new Date().toISOString(),
    });
    let pending = 0;
    let running = 0;
    try {
      pending = Number(
        authDb
          .prepare(
            `UPDATE pipeline_jobs
             SET status = 'cancelled',
                 finished_at = CURRENT_TIMESTAMP,
                 result_json = COALESCE(result_json, ?)
             WHERE job_type = 'download'
               AND status = 'pending'
               AND corpus_id IS ?`
          )
          .run(resultPayload, corpusId ?? null).changes || 0
      );
      if (includeRunning) {
        running = Number(
          authDb
            .prepare(
              `UPDATE pipeline_jobs
               SET status = 'cancelled',
                   finished_at = CURRENT_TIMESTAMP,
                   result_json = COALESCE(result_json, ?)
               WHERE job_type = 'download'
                 AND status = 'running'
                 AND corpus_id IS ?`
            )
            .run(resultPayload, corpusId ?? null).changes || 0
        );
      }
    } catch (error) {
      console.warn(`[download-worker] Failed to cancel download jobs for corpus=${corpusId ?? 'none'}: ${error?.message || error}`);
    }
    return { pending, running };
  }

  function countDownloadPipelineJobs(corpusId) {
    if (!tableExists(authDb, 'pipeline_jobs')) {
      return { pending: 0, running: 0 };
    }
    try {
      const pending = Number(
        authDb
          .prepare(
            `SELECT COUNT(*) AS count
             FROM pipeline_jobs
             WHERE job_type = 'download'
               AND status = 'pending'
               AND corpus_id IS ?`
          )
          .get(corpusId ?? null)?.count || 0
      );
      const running = Number(
        authDb
          .prepare(
            `SELECT COUNT(*) AS count
             FROM pipeline_jobs
             WHERE job_type = 'download'
               AND status = 'running'
               AND corpus_id IS ?`
          )
          .get(corpusId ?? null)?.count || 0
      );
      return { pending, running };
    } catch (error) {
      return { pending: 0, running: 0 };
    }
  }

  function findExistingPipelineJob(jobType, corpusId, statuses = ['pending', 'running']) {
    if (!statuses.length) return null;
    const placeholders = statuses.map(() => '?').join(', ');
    return authDb
      .prepare(
        `SELECT id, status
         FROM pipeline_jobs
         WHERE job_type = ?
           AND corpus_id IS ?
           AND status IN (${placeholders})
         ORDER BY created_at ASC, id ASC
         LIMIT 1`
      )
      .get(jobType, corpusId ?? null, ...statuses);
  }

  function enqueuePipelineJob({ corpusId = null, jobType, parameters = {}, dedupeStatuses = [] }) {
    if (!tableExists(authDb, 'pipeline_jobs')) {
      throw new Error('pipeline_jobs table is not available');
    }
    const tx = authDb.transaction(() => {
      const existing = dedupeStatuses.length
        ? findExistingPipelineJob(jobType, corpusId, dedupeStatuses)
        : null;
      if (existing) {
        return {
          inserted: false,
          jobId: Number(existing.id),
          status: existing.status,
        };
      }

      const result = authDb
        .prepare("INSERT INTO pipeline_jobs (corpus_id, job_type, status, parameters_json) VALUES (?, ?, 'pending', ?)")
        .run(corpusId ?? null, jobType, JSON.stringify(parameters ?? {}));
      return {
        inserted: true,
        jobId: Number(result.lastInsertRowid),
        status: 'pending',
      };
    });

    return tx();
  }

  function countQueuedDownloads(corpusId) {
    try {
      if (corpusId !== undefined && corpusId !== null) {
        const row = authDb
          .prepare(
            `SELECT COUNT(*) AS count
             FROM with_metadata wm
             JOIN corpus_items ci ON ci.table_name = 'with_metadata' AND ci.row_id = wm.id
             WHERE ci.corpus_id = ?
               AND wm.download_state IN ('queued', 'in_progress')`
          )
          .get(corpusId);
        return Number(row?.count || 0);
      }
      const row = authDb
        .prepare("SELECT COUNT(*) AS count FROM with_metadata WHERE download_state IN ('queued', 'in_progress')")
        .get();
      return Number(row?.count || 0);
    } catch (error) {
      return 0;
    }
  }

  function resolveAutoDownloadBatchSize(corpusId) {
    const queued = countQueuedDownloads(corpusId);
    const minBatch = 25;
    const maxBatch = Math.max(minBatch, coerceInt(process.env.RAG_FEEDER_DOWNLOAD_BATCH_MAX, 200) || 200);
    if (queued <= 0) return minBatch;
    return Math.max(minBatch, Math.min(maxBatch, queued));
  }

  function resolveDownloadWorkerCount({ corpusId, requestedWorkers = 0, batchSize = 3 }) {
    const cpuCount = Math.max(1, Number(os.cpus()?.length || 1));
    const maxRecommended = Math.max(1, Math.min(6, cpuCount));
    const minBatch = Math.max(1, Number(batchSize || 1));
    if (Number(requestedWorkers) > 0) {
      return Math.max(1, Math.min(12, Number(requestedWorkers)));
    }
    const queued = countQueuedDownloads(corpusId);
    if (queued <= 0) {
      return 1;
    }
    const estimated = Math.ceil(queued / (minBatch * 2));
    return Math.max(1, Math.min(maxRecommended, estimated));
  }

  function aggregateDownloadPayloads(payloads) {
    const summary = {
      processed: 0,
      downloaded: 0,
      failed: 0,
      skipped: 0,
      errors: [],
      runs: payloads.length,
    };
    payloads.forEach((payload) => {
      summary.processed += Number(payload?.processed || 0);
      summary.downloaded += Number(payload?.downloaded || 0);
      summary.failed += Number(payload?.failed || 0);
      summary.skipped += Number(payload?.skipped || 0);
      if (Array.isArray(payload?.errors) && payload.errors.length) {
        summary.errors.push(...payload.errors);
      }
    });
    return summary;
  }

  function emitPromotionEvent({
    corpusId,
    transition,
    fromStage,
    toStage,
    itemId,
    destItemId = null,
    title = null,
  }) {
    const sourceId = Number(itemId);
    if (!Number.isFinite(sourceId)) return;
    const targetId = Number(destItemId);
    sendEvent({
      kind: 'promotion',
      corpus_id: Number.isFinite(Number(corpusId)) ? Number(corpusId) : null,
      transition,
      from_stage: fromStage,
      to_stage: toStage,
      item_id: sourceId,
      dest_item_id: Number.isFinite(targetId) ? targetId : null,
      title: title ? String(title) : null,
      ts: new Date().toISOString(),
    });
  }

  async function tickPipelineWorker(corpusId) {
    const state = getOrInitPipelineWorkerState(corpusId);
    if (!state.running || state.inFlight) return;
    state.inFlight = true;
    const corpusTag = corpusId || 'none';
    state.lastTickAt = new Date().toISOString();
    state.lastError = null;
    
    try {
      enqueuePipelineJob({
        corpusId,
        jobType: 'pipeline_tick',
        parameters: { config: state.config },
        dedupeStatuses: ['pending', 'running'],
      });
    } catch (error) {
      state.lastError = error?.message || String(error);
      console.error(`[pipeline-worker ERROR] corpus=${corpusTag} ${state.lastError}`);
    } finally {
      state.inFlight = false;
    }
  }

  function startPipelineWorker(corpusId, config = {}) {
    const state = getOrInitPipelineWorkerState(corpusId);
    const intervalSeconds = coerceInt(config.intervalSeconds, state.config.intervalSeconds) || state.config.intervalSeconds;
    const promoteBatchSize = coerceInt(config.promoteBatchSize, state.config.promoteBatchSize) || state.config.promoteBatchSize;
    const promoteWorkers =
      coerceInt(config.promoteWorkers, state.config.promoteWorkers ?? 6) || state.config.promoteWorkers || 6;
    const downloadBatchSize = coerceInt(config.downloadBatchSize, state.config.downloadBatchSize) || state.config.downloadBatchSize;
    const downloadWorkersRequested = Math.max(0, coerceInt(config.downloadWorkers, state.config.downloadWorkers) || 0);

    state.config = {
      intervalSeconds: Math.max(10, intervalSeconds),
      promoteBatchSize: Math.max(1, promoteBatchSize),
      promoteWorkers: Math.max(1, Math.min(32, promoteWorkers)),
      downloadBatchSize: Math.max(1, downloadBatchSize),
      downloadWorkers: downloadWorkersRequested,
    };

    // Avoid running legacy download-only worker alongside the orchestration worker.
    const legacy = getOrInitWorkerState(corpusId);
    if (legacy.running || legacy.inFlight) {
      stopDownloadWorker(corpusId, { force: false });
    }

    if (state.running) return state;
    state.running = true;
    state.startedAt = new Date().toISOString();
    tickPipelineWorker(corpusId);
    state.intervalId = setInterval(() => tickPipelineWorker(corpusId), state.config.intervalSeconds * 1000);
    return state;
  }

  function pausePipelineWorker(corpusId, { force = false } = {}) {
    const state = getOrInitPipelineWorkerState(corpusId);
    state.running = false;
    if (state.intervalId) {
      clearInterval(state.intervalId);
      state.intervalId = null;
    }
    return state;
  }

  // Middleware
  // Configure CORS to allow requests specifically from the frontend origin
  const allowedOrigins = (process.env.RAG_FEEDER_FRONTEND_ORIGIN || 'http://localhost:5175,http://localhost:3000')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);
  const corsOptions = {
    origin(origin, callback) {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(new Error('Not allowed by CORS'));
      }
    },
    optionsSuccessStatus: 200 // Some legacy browsers (IE11, various SmartTVs) choke on 204
  };
  app.use(cors(corsOptions));
  app.use(express.json());

  // --- Auth endpoints ---
  app.post('/api/auth/login', (req, res) => {
    const { username, password } = req.body || {};
    if (!username || !password) {
      return res.status(400).json({ error: 'Missing username or password' });
    }
    const user = authDb
      .prepare('SELECT id, username, password_hash, last_corpus_id FROM users WHERE username = ?')
      .get(username);
    if (!user || !bcrypt.compareSync(password, user.password_hash)) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    let lastCorpusId = user.last_corpus_id;
    if (!lastCorpusId) {
      const corpus = authDb
        .prepare('SELECT corpus_id FROM user_corpora WHERE user_id = ? ORDER BY created_at DESC LIMIT 1')
        .get(user.id);
      lastCorpusId = corpus?.corpus_id || defaultCorpusId;
      authDb.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(lastCorpusId, user.id);
    }
    const token = jwt.sign({ sub: user.id, username: user.username }, authConfig.jwtSecret, {
      expiresIn: '7d',
    });
    return res.json({ token, user: { id: user.id, username: user.username, last_corpus_id: lastCorpusId } });
  });

  app.post('/api/auth/register', (req, res) => {
    if (!isStubMode()) {
      return res.status(404).json({ error: 'Not found' });
    }
    const { username, password } = req.body || {};
    if (!username || !password) {
      return res.status(400).json({ error: 'Missing username or password' });
    }
    const existing = authDb.prepare('SELECT id FROM users WHERE username = ?').get(username);
    if (existing) {
      return res.status(409).json({ error: 'User already exists' });
    }
    const hash = bcrypt.hashSync(password, 10);
    const result = authDb
      .prepare('INSERT INTO users (username, password_hash) VALUES (?, ?)')
      .run(username, hash);
    const userId = result.lastInsertRowid;
    const corpus = authDb
      .prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)')
      .run('Default Corpus', userId);
    authDb
      .prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')")
      .run(userId, corpus.lastInsertRowid);
    authDb.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpus.lastInsertRowid, userId);
    return res.status(201).json({ id: userId, username });
  });

  app.get('/api/auth/me', requireAuthMiddleware, (req, res) => {
    const corpora = listCorporaForUser(authDb, req.user.id);
    return res.json({ user: req.user, corpora });
  });

  // --- Corpus management ---
  app.get('/api/corpora', requireAuthMiddleware, (req, res) => {
    const corpora = listCorporaForUser(authDb, req.user.id);
    return res.json({ corpora });
  });

  app.post('/api/corpora', requireAuthMiddleware, (req, res) => {
    const { name } = req.body || {};
    if (!name) {
      return res.status(400).json({ error: 'Missing corpus name' });
    }
    const result = authDb
      .prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)')
      .run(name, req.user.id);
    const corpusId = result.lastInsertRowid;
    authDb
      .prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')")
      .run(req.user.id, corpusId);
    authDb.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpusId, req.user.id);
    return res.status(201).json({ id: corpusId, name });
  });

  app.post('/api/corpora/:id/select', requireAuthMiddleware, (req, res) => {
    const corpusId = Number(req.params.id);
    const access = authDb
      .prepare('SELECT role FROM user_corpora WHERE user_id = ? AND corpus_id = ?')
      .get(req.user.id, corpusId);
    if (!access) {
      return res.status(403).json({ error: 'No access to corpus' });
    }
    authDb.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpusId, req.user.id);
    return res.json({ selected: corpusId });
  });

  app.post('/api/corpora/:id/share', requireAuthMiddleware, (req, res) => {
    const corpusId = Number(req.params.id);
    const { username, role = 'viewer' } = req.body || {};
    if (!username) {
      return res.status(400).json({ error: 'Missing username' });
    }
    const access = authDb
      .prepare('SELECT role FROM user_corpora WHERE user_id = ? AND corpus_id = ?')
      .get(req.user.id, corpusId);
    if (!access || access.role !== 'owner') {
      return res.status(403).json({ error: 'Only owners can share corpora' });
    }
    const target = authDb.prepare('SELECT id FROM users WHERE username = ?').get(username);
    if (!target) {
      return res.status(404).json({ error: 'User not found' });
    }
    authDb
      .prepare('INSERT OR REPLACE INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, ?)')
      .run(target.id, corpusId, role);
    return res.json({ shared: true });
  });

  app.delete('/api/corpora/:id', requireAuthMiddleware, (req, res) => {
    const corpusId = Number(req.params.id);
    if (!Number.isFinite(corpusId) || corpusId <= 0) {
      return res.status(400).json({ error: 'Invalid corpus id' });
    }

    const access = authDb
      .prepare('SELECT role FROM user_corpora WHERE user_id = ? AND corpus_id = ?')
      .get(req.user.id, corpusId);
    if (!access || access.role !== 'owner') {
      return res.status(403).json({ error: 'Only owners can delete corpora' });
    }

    const corpus = authDb
      .prepare('SELECT id, name FROM corpora WHERE id = ?')
      .get(corpusId);
    if (!corpus) {
      return res.status(404).json({ error: 'Corpus not found' });
    }

    // Stop in-memory schedulers first so they do not keep queueing jobs for a deleted corpus.
    pausePipelineWorker(corpusId, { force: false });
    stopDownloadWorker(corpusId, { force: false });
    pipelineWorkers.delete(corpusId);
    downloadWorkers.delete(corpusId);

    try {
      const result = authDb.transaction(() => {
        const affectedUsers = authDb
          .prepare('SELECT DISTINCT user_id FROM user_corpora WHERE corpus_id = ?')
          .all(corpusId)
          .map((row) => Number(row.user_id))
          .filter((id) => Number.isFinite(id) && id > 0);

        if (tableExists(authDb, 'pipeline_jobs')) {
          authDb.prepare('DELETE FROM pipeline_jobs WHERE corpus_id = ?').run(corpusId);
        }
        if (tableExists(authDb, 'ingest_entries')) {
          authDb.prepare('DELETE FROM ingest_entries WHERE corpus_id = ?').run(corpusId);
        }
        if (tableExists(authDb, 'ingest_source_metadata')) {
          authDb.prepare('DELETE FROM ingest_source_metadata WHERE corpus_id = ?').run(corpusId);
        }
        authDb.prepare('DELETE FROM corpus_items WHERE corpus_id = ?').run(corpusId);
        authDb.prepare('DELETE FROM user_corpora WHERE corpus_id = ?').run(corpusId);
        authDb.prepare('DELETE FROM corpora WHERE id = ?').run(corpusId);

        let fallbackCreated = 0;
        let requesterSelectedCorpusId = null;

        for (const userId of affectedUsers) {
          let nextCorpusId = authDb
            .prepare(
              `SELECT c.id
               FROM corpora c
               JOIN user_corpora uc ON uc.corpus_id = c.id
               WHERE uc.user_id = ?
               ORDER BY c.created_at DESC, c.id DESC
               LIMIT 1`
            )
            .get(userId)?.id;

          if (!nextCorpusId) {
            const created = authDb
              .prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)')
              .run('Default Corpus', userId);
            nextCorpusId = created.lastInsertRowid;
            authDb
              .prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')")
              .run(userId, nextCorpusId);
            fallbackCreated += 1;
          }

          authDb.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(nextCorpusId, userId);
          if (userId === req.user.id) {
            requesterSelectedCorpusId = nextCorpusId;
          }
        }

        return {
          deleted: true,
          deleted_id: corpusId,
          deleted_name: corpus.name,
          selected_corpus_id: requesterSelectedCorpusId,
          affected_users: affectedUsers.length,
          fallback_corpora_created: fallbackCreated,
        };
      })();

      return res.json(result);
    } catch (error) {
      console.error('[/api/corpora/:id DELETE] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to delete corpus' });
    }
  });

  // Legacy `/api/bibliographies/*` endpoints were removed.
  // The app is DB-first; ingestion results are accessed via `/api/ingest/*`.

  // Configure multer for file upload
  const storage = multer.diskStorage({
    destination: function (req, file, cb) {
      cb(null, uploadDir);
    },
    filename: function (req, file, cb) {
      const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
      cb(null, uniqueSuffix + '-' + file.originalname);
    }
  });

  const upload = multer({ storage: storage });

  // --- MODIFIED ENDPOINT: Upload PDF ---
  // Renamed conceptually, but keeping path for minimal frontend changes initially
  app.post('/api/process_pdf', requireAuthMiddleware, upload.single('file'), (req, res) => { // Made non-async
    console.log('[/api/process_pdf -> /api/upload] Received upload request');
    try {
      console.log('[/api/upload] Inside TRY block.');
      if (!req.file) {
        console.log('[/api/upload] No file uploaded in request');
        return res.status(400).json({ error: 'No file uploaded' });
      }

      console.log('[/api/upload] req.file object:', JSON.stringify(req.file, null, 2));

      const storedFilename = req.file.filename; // Get filename generated by multer
      const filePath = req.file.path;
      console.log(`[/api/upload] File uploaded successfully: ${filePath}`);
      console.log(`[/api/upload] Stored filename: ${storedFilename}`);

      // Return the generated filename to the client
      console.log('[/api/upload] Attempting to send success response...');
      res.json({
        message: 'File uploaded successfully.',
        filename: storedFilename // Send back the name used on the server
      });
      console.log('[/api/upload] Success response sent.');

    } catch (error) {
      console.error('[/api/upload] CAUGHT ERROR in /api/process_pdf:', error);
      // Check if response has already been sent
      if (!res.headersSent) {
        res.status(500).json({ error: 'Server error during upload', details: error.message });
      } else {
        console.error('[/api/upload] Error occurred after headers sent.');
      }
      // Attempt to clean up file if upload failed mid-request?
      if (req.file && req.file.path) {
        fs.unlink(req.file.path, (unlinkErr) => {
          if (unlinkErr) console.error('[/api/upload] Error deleting partially uploaded file:', unlinkErr);
        });
      }
    }
  });
  // --- END MODIFIED UPLOAD ENDPOINT ---

  // --- NEW ENDPOINT: Extract Bibliography ---
  app.post('/api/extract-bibliography/:filename', requireAuthMiddleware, async (req, res) => {
    const { filename } = req.params;
    const corpusId = req.corpusId;
    const corpusTag = String(corpusId || 'none');
    const inputPdfPath = path.join(UPLOADS_DIR, filename);
    const baseName = path.basename(filename, path.extname(filename)); // stable ingest_source
    const existingPagesDir = path.join(BIB_OUTPUT_DIR, baseName);

    console.log(`[/api/extract-bibliography] Received request for: ${filename}`);
    console.log(`[/api/extract-bibliography] Input PDF path: ${inputPdfPath}`);
    console.log(`[/api/extract-bibliography] Final bib output dir: ${BIB_OUTPUT_DIR}`);

    const inputExists = fs.existsSync(inputPdfPath);
    const pagesExist =
      fs.existsSync(existingPagesDir) &&
      fs.readdirSync(existingPagesDir).some((file) => file.toLowerCase().endsWith('.pdf'));

    // 1. Check if we can proceed: either we have the uploaded PDF, or we can reuse extracted pages.
    if (!inputExists && !pagesExist) {
      console.error(`[/api/extract-bibliography] Input PDF not found: ${inputPdfPath}`);
      console.error(`[/api/extract-bibliography] No existing extracted pages found in: ${existingPagesDir}`);
      return res.status(404).json({
        error: 'Uploaded PDF not found (and no extracted pages exist). Please re-upload the PDF.',
      });
    }

    // --- Send Immediate Response ---
    res.status(202).json({
      message: inputExists
        ? 'Bibliography extraction process started.'
        : 'Bibliography parsing started (reusing previously extracted reference pages).',
    });
    // Status 202 Accepted indicates the request is accepted for processing, but is not complete.
    // --- End Immediate Response ---

    let sharedUploadedFileName = null;
    const cleanupSharedUpload = () => {
      if (!sharedUploadedFileName) return;
      const toDelete = sharedUploadedFileName;
      sharedUploadedFileName = null;
      geminiDeleteUploadedSync(toDelete);
    };

    // --- Run Scripts in Background ---
    try {
      const inputPageCount = inputExists ? getPdfPageCountSync(inputPdfPath) : null;
      if (inputPageCount) {
        console.log(`[/api/extract-bibliography] PDF page count: ${inputPageCount}`);
      }

      // For small PDFs, pre-upload once and reuse the same Gemini file in
      // get_bib_pages and inline fallback to avoid a second upload.
      if (inputExists && inputPageCount && inputPageCount <= 50) {
        try {
          sharedUploadedFileName = geminiUploadPdfSync(inputPdfPath);
          console.log(`[/api/extract-bibliography] Reusing uploaded Gemini file: ${sharedUploadedFileName}`);
          send(`[/api/extract-bibliography][corpus=${corpusTag}] Uploaded once for reuse: ${sharedUploadedFileName}`);
        } catch (uploadErr) {
          console.warn(`[/api/extract-bibliography] Shared upload failed, continuing without reuse: ${uploadErr?.message || uploadErr}`);
          send(`[/api/extract-bibliography][corpus=${corpusTag}] Shared upload unavailable; continuing with regular flow.`);
          sharedUploadedFileName = null;
        }
      }
      const runApiScraper = ({
        inputDir = null,
        inputPdf = null,
        extractMode = 'page',
        chunkPages = 0,
        uploadedFileName = null,
        modelOverride = null,
        label = 'APIscraper',
      }) => {
        return new Promise((resolve) => {
          const dbPath = DB_PATH;
          const scrapeApiArgs = ['--db-path', dbPath, '--ingest-source', baseName, '--extract-mode', extractMode];
          if (inputDir) scrapeApiArgs.push('--input-dir', inputDir);
          if (inputPdf) scrapeApiArgs.push('--input-pdf', inputPdf);
          if (chunkPages && Number(chunkPages) > 0) scrapeApiArgs.push('--chunk-pages', String(chunkPages));
          if (uploadedFileName) scrapeApiArgs.push('--uploaded-file-name', uploadedFileName);
          if (corpusId) scrapeApiArgs.push('--corpus-id', String(corpusId));

          console.log(`[/api/extract-bibliography] Spawning ${label}: python ${API_SCRAPER_SCRIPT} ${scrapeApiArgs.join(' ')}`);
          send(`[/api/extract-bibliography][corpus=${corpusTag}] Starting ${label}...`);
          const child = spawn(PYTHON_EXEC, [API_SCRAPER_SCRIPT, ...scrapeApiArgs], {
            env: {
              ...process.env,
              ...(modelOverride ? { RAG_FEEDER_GEMINI_MODEL: modelOverride } : {}),
              PYTHONPATH: [DL_LIT_PROJECT_DIR, REPO_ROOT, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
              RAG_FEEDER_DL_LIT_PROJECT_DIR: DL_LIT_PROJECT_DIR,
              RAG_FEEDER_DB_PATH: DB_PATH,
            },
          });

          child.stdout.on('data', (data) => {
            const message = data.toString();
            console.log(`[${label} stdout]: ${message}`);
            send(`[${label}] [corpus=${corpusTag}] ${message}`);
          });

          child.stderr.on('data', (data) => {
            const message = data.toString();
            console.error(`[${label} stderr]: ${message}`);
            send(`[${label} ERROR] [corpus=${corpusTag}] ${message}`);
          });

          child.on('close', (code) => {
            if (code !== 0) {
              console.error(`[/api/extract-bibliography] ${label} exited with code ${code}`);
              send(`[/api/extract-bibliography][corpus=${corpusTag}] ${label} failed with code ${code}`);
              resolve(false);
              return;
            }
            console.log(`[/api/extract-bibliography] Finished ${label}.`);
            resolve(true);
          });
        });
      };

      const runInlineFallback = async (reason) => {
        if (!inputExists) {
          console.error(`[/api/extract-bibliography] Cannot run inline fallback: input PDF missing (${inputPdfPath}).`);
          send(`[/api/extract-bibliography][corpus=${corpusTag}] Inline fallback skipped (input PDF missing).`);
          return false;
        }
        const useSharedUpload = Boolean(sharedUploadedFileName);
        const chunkPages = useSharedUpload ? 0 : 50;
        const why = reason || 'reference page extraction returned no usable pages';
        console.warn(`[/api/extract-bibliography] Triggering inline citation fallback: ${why}`);
        send(
          `[/api/extract-bibliography][corpus=${corpusTag}] Triggering inline citation fallback (${why}, chunk_pages=${chunkPages || 0}).`
        );
        const ok = await runApiScraper({
          inputPdf: inputPdfPath,
          extractMode: 'inline',
          chunkPages,
          uploadedFileName: sharedUploadedFileName,
          modelOverride: 'gemini-3.1-flash-lite-preview',
          label: 'APIscraper inline fallback',
        });
        return ok;
      };

      // Fast-path: if the upload is missing but we already have extracted page PDFs, run the scraper only.
      if (!inputExists && pagesExist) {
        console.log(`[/api/extract-bibliography] Upload missing; reusing extracted pages in ${existingPagesDir}`);
        send(`[/api/extract-bibliography][corpus=${corpusTag}] Upload missing; reusing extracted pages in ${existingPagesDir}`);
        try {
          const sourceMetadataPath = path.join(existingPagesDir, 'source_metadata.json');
          if (fs.existsSync(sourceMetadataPath)) {
            const parsed = JSON.parse(fs.readFileSync(sourceMetadataPath, 'utf8'));
            const sourceMetadata = normalizeIngestSourceMetadata(parsed?.source_metadata);
            if (sourceMetadata) {
              const saved = upsertIngestSourceMetadata(authDb, {
                corpusId,
                ingestSource: baseName,
                sourcePdf: null,
                sourceMetadata,
              });
              if (saved) {
                send(`[/api/extract-bibliography][corpus=${corpusTag}] Loaded source metadata from existing extraction.`);
              }
            }
          }
        } catch (sourceMetaErr) {
          console.warn(`[/api/extract-bibliography] Failed to read source metadata sidecar from reused pages: ${sourceMetaErr?.message || sourceMetaErr}`);
          send(`[/api/extract-bibliography][corpus=${corpusTag}] Source metadata sidecar could not be loaded from reused pages.`);
        }
        runApiScraper({
          inputDir: existingPagesDir,
          extractMode: 'page',
          label: 'APIscraper (reused pages)',
        }).finally(() => cleanupSharedUpload());
        return;
      }

      // 3. Run get_bib_pages.py in background
      const getPagesArgs = [
        '--input-pdf', inputPdfPath,
        '--output-dir', BIB_OUTPUT_DIR // Keep this, it seems to place output in BIB_OUTPUT_DIR/baseName/
      ];
      if (sharedUploadedFileName) {
        getPagesArgs.push('--uploaded-file-name', sharedUploadedFileName);
      }

      console.log(`[/api/extract-bibliography] Spawning: python ${GET_BIB_PAGES_SCRIPT} ${getPagesArgs.join(' ')}`);
      const getPagesProcess = spawn(PYTHON_EXEC, [GET_BIB_PAGES_SCRIPT, ...getPagesArgs], {
        env: {
          ...process.env,
          PYTHONPATH: [DL_LIT_PROJECT_DIR, REPO_ROOT, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
          RAG_FEEDER_DL_LIT_PROJECT_DIR: DL_LIT_PROJECT_DIR,
          RAG_FEEDER_DB_PATH: DB_PATH,
        },
      });

      getPagesProcess.stdout.on('data', (data) => {
        const message = data.toString();
        console.log(`[get_bib_pages stdout]: ${message}`);
        send(`[get_bib_pages][corpus=${corpusTag}] ${message}`);
      });

      getPagesProcess.stderr.on('data', (data) => {
        const message = data.toString();
        console.error(`[get_bib_pages stderr]: ${message}`);
        send(`[get_bib_pages ERROR][corpus=${corpusTag}] ${message}`);
      });

      getPagesProcess.on('close', async (code) => {
        try {
          console.log(`[/api/extract-bibliography] ${GET_BIB_PAGES_SCRIPT} exited with code ${code}`);
          if (code !== 0) {
            const errorMessage = `[/api/extract-bibliography] get_bib_pages.py exited with code ${code}.`;
            send(`[/api/extract-bibliography][corpus=${corpusTag}] get_bib_pages.py exited with code ${code}.`);
            console.error(errorMessage);
            await runInlineFallback(`get_bib_pages failed with code ${code}`);
            return;
          }

          // get_bib_pages now writes source_metadata.json from the same Gemini call
          // that detects reference sections; ingest it before page/inline extraction.
          try {
            const sourceMetadataPath = path.join(existingPagesDir, 'source_metadata.json');
            if (fs.existsSync(sourceMetadataPath)) {
              const parsed = JSON.parse(fs.readFileSync(sourceMetadataPath, 'utf8'));
              const sourceMetadata = normalizeIngestSourceMetadata(parsed?.source_metadata);
              if (sourceMetadata) {
                const saved = upsertIngestSourceMetadata(authDb, {
                  corpusId,
                  ingestSource: baseName,
                  sourcePdf: inputPdfPath,
                  sourceMetadata,
                });
                if (saved) {
                  send(`[/api/extract-bibliography][corpus=${corpusTag}] Source metadata extracted and saved.`);
                }
              } else {
                send(`[/api/extract-bibliography][corpus=${corpusTag}] No source metadata inferred from section-detection call.`);
              }
            } else {
              send(`[/api/extract-bibliography][corpus=${corpusTag}] Source metadata sidecar not found after section extraction.`);
            }
          } catch (sourceMetaErr) {
            console.warn(`[/api/extract-bibliography] Failed to read source metadata sidecar: ${sourceMetaErr?.message || sourceMetaErr}`);
            send(`[/api/extract-bibliography][corpus=${corpusTag}] Source metadata sidecar could not be parsed.`);
          }

          // Proceed to page-level parsing when extracted pages exist; otherwise fallback to inline citation extraction.
          const jsonSubDir = existingPagesDir; // Subdirectory named after the base PDF name
          const pageFiles = fs.existsSync(jsonSubDir)
            ? fs.readdirSync(jsonSubDir).filter((file) => file.endsWith('.pdf'))
            : [];

          if (pageFiles.length === 0) {
            const errorMessage = `[/api/extract-bibliography] No extracted page PDFs found in: ${jsonSubDir}.`;
            console.error(errorMessage);
            send(`[/api/extract-bibliography][corpus=${corpusTag}] No extracted page PDFs found in: ${jsonSubDir}.`);
            await runInlineFallback('no extracted bibliography pages found');
            return;
          }

          const ok = await runApiScraper({
            inputDir: jsonSubDir,
            extractMode: 'page',
            label: 'APIscraper (page mode)',
          });
          if (ok) {
            console.log(`[/api/extract-bibliography] Bibliography extraction complete for ${filename}. Output in ${BIB_OUTPUT_DIR}.`);
            send(`[/api/extract-bibliography][corpus=${corpusTag}] Bibliography extraction complete for ${filename}.`);
          }
        } finally {
          cleanupSharedUpload();
        }
      }); // End get_bib_pages callback

    } catch (error) {
      // This catch block likely only catches synchronous errors like fs.mkdirSync failure
      console.error(`[/api/extract-bibliography] Synchronous error setting up processing for ${filename}:`, error);
      cleanupSharedUpload();
      // Cannot send response here as it might have already been sent.
    }
    // Removed Finally block as cleanup is handled in callbacks now.
    // finally {
    //   // 6. Clean up temporary directory - MOVED
    // }
  });
  // --- END NEW ENDPOINT ---

  // Legacy `/api/bibliographies/consolidate` endpoint removed (DB-first pipeline).

  app.post('/api/keyword-search', requireAuthMiddleware, async (req, res) => {
    const query = req.body?.query?.trim();
    const seedJson = req.body?.seedJson;
    if (!query && !seedJson) {
      return res.status(400).json({ error: 'query or seedJson is required' });
    }

    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ runId: 0, results: STUB_RESULTS.keywordResults, source: 'stub' });
    }

    const field = req.body?.field || 'default';
    const maxResults = coerceInt(req.body?.maxResults, 200);
    const yearFrom = coerceInt(req.body?.yearFrom, null);
    const yearTo = coerceInt(req.body?.yearTo, null);
    const mailto = req.body?.mailto || '';
    const enqueue = Boolean(req.body?.enqueue);
    const dbPath = DB_PATH;

    const args = ['--db-path', dbPath, '--max-results', String(maxResults), '--field', String(field)];
    const expansion = buildKeywordSearchExpansion({
      relatedDepth: coerceInt(req.body?.relatedDepth, null),
      relatedDepthDownstream: coerceInt(req.body?.relatedDepthDownstream, null),
      relatedDepthUpstream: coerceInt(req.body?.relatedDepthUpstream, null),
      maxRelated: coerceInt(req.body?.maxRelated, null),
      includeDownstream: req.body?.includeDownstream,
      includeUpstream: req.body?.includeUpstream,
    });

    if (query) args.push('--query', query);
    if (!query && seedJson) {
      const seedPayload = typeof seedJson === 'string' ? seedJson : JSON.stringify(seedJson);
      args.push('--seed-json', seedPayload);
    }
    if (query && yearFrom) args.push('--year-from', String(yearFrom));
    if (query && yearTo) args.push('--year-to', String(yearTo));
    if (mailto) args.push('--mailto', String(mailto));
    if (enqueue) args.push('--enqueue');
    applyKeywordSearchExpansionArgs(args, expansion);

    try {
      const payload = await runPythonJson(KEYWORD_SEARCH_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/keyword-search] Error:', error);
      return res.status(500).json({ error: error.message || 'Keyword search failed' });
    }
  });

  app.get('/api/corpus', requireAuthMiddleware, async (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ items: STUB_RESULTS.corpus, total: STUB_RESULTS.corpus.length, source: 'stub' });
    }
    const limit = coerceInt(req.query?.limit, 200);
    const offset = coerceInt(req.query?.offset, 0);
    const dbPath = DB_PATH;

    const args = ['--db-path', dbPath, '--limit', String(limit), '--offset', String(offset)];
    try {
      const payload = await runPythonJson(CORPUS_LIST_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/corpus] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to fetch corpus' });
    }
  });

  app.get('/api/recursion-config', requireAuthMiddleware, (req, res) => {
    res.json({
      keyword: RECURSION_DEFAULTS.keyword,
      uploadedDocs: RECURSION_DEFAULTS.uploadedDocs,
    });
  });

  app.get('/api/downloads', requireAuthMiddleware, async (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ items: STUB_RESULTS.downloads, total: STUB_RESULTS.downloads.length, source: 'stub' });
    }
    const limit = coerceInt(req.query?.limit, 200);
    const offset = coerceInt(req.query?.offset, 0);
    const dbPath = DB_PATH;

    const args = ['--db-path', dbPath, '--limit', String(limit), '--offset', String(offset)];
    try {
      const payload = await runPythonJson(DOWNLOADS_LIST_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/downloads] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to fetch download queue' });
    }
  });

  app.get('/api/exports/:format', requireAuthMiddleware, async (req, res) => {
    const rawFormat = String(req.params?.format || '').toLowerCase();
    const formatMap = { json: 'json', bibtex: 'bibtex', pdfs: 'pdfs_zip', bundle: 'bundle_zip' };
    const selectedFormat = formatMap[rawFormat];
    if (!selectedFormat) {
      return res.status(400).json({ error: 'format must be one of: json, bibtex, pdfs, bundle' });
    }

    const statusFilter = String(req.query?.status || '').trim().toLowerCase();
    const yearFrom = coerceInt(req.query?.year_from || req.query?.yearFrom, null);
    const yearTo = coerceInt(req.query?.year_to || req.query?.yearTo, null);
    const sourceFilter = String(req.query?.source || '').trim();

    if (yearFrom !== null && yearTo !== null && yearFrom > yearTo) {
      return res.status(400).json({ error: 'year_from must be <= year_to' });
    }

    const dbPath = DB_PATH;
    let exportDir = path.join(ARTIFACTS_DIR, 'exports');
    try {
      fs.mkdirSync(exportDir, { recursive: true });
      fs.accessSync(exportDir, fs.constants.W_OK);
    } catch (error) {
      exportDir = path.join('/tmp', 'rag-feeder-exports');
      fs.mkdirSync(exportDir, { recursive: true });
    }

    const args = ['--db-path', dbPath, '--output-dir', exportDir, '--format', selectedFormat];
    if (statusFilter) {
      args.push('--status', statusFilter);
    }
    if (yearFrom !== null) {
      args.push('--year-from', String(yearFrom));
    }
    if (yearTo !== null) {
      args.push('--year-to', String(yearTo));
    }
    if (sourceFilter) {
      args.push('--source', sourceFilter);
    }

    try {
      const payload = await runPythonJson(EXPORT_BUNDLE_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      const outputPath = payload?.output_path ? path.resolve(String(payload.output_path)) : '';
      const safeRoot = path.resolve(exportDir) + path.sep;
      if (!outputPath || !outputPath.startsWith(safeRoot) || !fs.existsSync(outputPath)) {
        return res.status(500).json({ error: 'Export failed: output file missing' });
      }

      const downloadName = payload?.filename || path.basename(outputPath);
      return res.download(outputPath, downloadName);
    } catch (error) {
      console.error('[/api/exports] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to export corpus data' });
    }
  });
  app.get('/api/graph', requireAuthMiddleware, async (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ ...STUB_RESULTS.graph, source: 'stub' });
    }
    const maxNodes = coerceInt(req.query?.max_nodes || req.query?.maxNodes, 200);
    const relationship = req.query?.relationship || 'both';
    const status = req.query?.status || 'all';
    const yearFrom = coerceInt(req.query?.year_from || req.query?.yearFrom, null);
    const yearTo = coerceInt(req.query?.year_to || req.query?.yearTo, null);
    const hideIsolates = req.query?.hide_isolates !== '0' && req.query?.hideIsolates !== '0';
    const dbPath = DB_PATH;

    const args = [
      '--db-path',
      dbPath,
      '--max-nodes',
      String(maxNodes),
      '--relationship',
      relationship,
      '--status',
      status,
      '--hide-isolates',
      hideIsolates ? '1' : '0',
    ];
    if (yearFrom !== null) {
      args.push('--year-from', String(yearFrom));
    }
    if (yearTo !== null) {
      args.push('--year-to', String(yearTo));
    }
    try {
      const payload = await runPythonJson(GRAPH_EXPORT_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/graph] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to build graph' });
    }
  });

  app.get('/api/ingest/latest', requireAuthMiddleware, async (req, res) => {
    const baseName = req.query?.baseName || '';
    const limit = coerceInt(req.query?.limit, 200);
    const dbPath = DB_PATH;

    const args = ['--db-path', dbPath, '--limit', String(limit)];
    if (baseName) {
      args.push('--base-name', String(baseName));
    }
    try {
      const payload = await runPythonJson(INGEST_LATEST_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/ingest/latest] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to fetch ingest entries' });
    }
  });

  app.post('/api/ingest/enqueue', requireAuthMiddleware, async (req, res) => {
    const rawIds = req.body?.entryIds;
    if (!Array.isArray(rawIds) || rawIds.length === 0) {
      return res.status(400).json({ error: 'entryIds must be a non-empty array' });
    }
    const ids = [...new Set(rawIds.map((v) => Number(v)).filter((v) => Number.isFinite(v) && v > 0))];
    if (ids.length === 0) {
      return res.status(400).json({ error: 'No valid entry ids provided' });
    }

    const dbPath = DB_PATH;
    const args = ['--db-path', dbPath, '--entry-ids', ids.join(',')];
    try {
      const payload = await runPythonJson(INGEST_ENQUEUE_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/ingest/enqueue] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to enqueue ingest entries' });
    }
  });

  app.post('/api/ingest/process-marked', requireAuthMiddleware, async (req, res) => {
    const limit = coerceInt(req.body?.limit, 10);
    const workers = coerceInt(req.body?.workers, 6);
    const enqueueDownload = Boolean(req.body?.enqueueDownload);
    const downloadBatchSize = coerceInt(req.body?.downloadBatchSize, 25);
    const expansion = buildUploadedDocsExpansion({
      relatedDepth: req.body?.relatedDepth,
      maxRelated: req.body?.maxRelated,
      includeDownstream: req.body?.includeDownstream,
      includeUpstream: req.body?.includeUpstream,
      relatedDepthDownstream: req.body?.relatedDepthDownstream,
      relatedDepthUpstream: req.body?.relatedDepthUpstream,
    });
    if (limit === null || limit === undefined || !Number.isFinite(limit) || limit <= 0) {
      return res.status(400).json({ error: 'limit must be a positive number' });
    }
    if (workers === null || workers === undefined || !Number.isFinite(workers) || workers <= 0) {
      return res.status(400).json({ error: 'workers must be a positive number' });
    }
    if (enqueueDownload && (downloadBatchSize === null || downloadBatchSize === undefined || !Number.isFinite(downloadBatchSize) || downloadBatchSize <= 0)) {
      return res.status(400).json({ error: 'downloadBatchSize must be a positive number' });
    }

    try {
      const jobParams = {
        limit,
        workers,
        expansion,
        shouldFetchReferences: expansion.includeDownstream
      };

      const insertProcessMarkedJobs = authDb.transaction(() => {
        const enrichResult = enqueuePipelineJob({
          corpusId: req.corpusId,
          jobType: 'enrich',
          parameters: jobParams,
        });
        let downloadJobId = null;
        let downloadInserted = false;
        if (enqueueDownload) {
          const dlResult = enqueuePipelineJob({
            corpusId: req.corpusId,
            jobType: 'download',
            parameters: { batchSize: downloadBatchSize },
            dedupeStatuses: ['pending', 'running'],
          });
          downloadJobId = dlResult.jobId;
          downloadInserted = dlResult.inserted;
        }
        return {
          enrichJobId: enrichResult.jobId,
          downloadJobId,
          downloadInserted,
        };
      });
      const queued = insertProcessMarkedJobs();

      return res.json({
        success: true, 
        job_id: queued.enrichJobId,
        enrich_job_id: queued.enrichJobId,
        download_job_id: queued.downloadJobId,
        message: enqueueDownload
          ? (queued.downloadInserted
            ? 'Enrichment and download jobs queued successfully.'
            : 'Enrichment job queued; download queue already has pending or running work for this corpus.')
          : 'Enrichment job queued successfully.'
      });
    } catch (error) {
      console.error('[/api/ingest/process-marked] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to queue marked ingest entries for processing' });
    }
  });

  app.get('/api/ingest/runs', requireAuthMiddleware, async (req, res) => {
    const limit = coerceInt(req.query?.limit, 20);
    const dbPath = DB_PATH;
    const args = ['--db-path', dbPath, '--limit', String(limit)];
    try {
      const payload = await runPythonJson(INGEST_RUNS_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/ingest/runs] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to fetch ingest runs' });
    }
  });

  app.get('/api/ingest/stats', requireAuthMiddleware, async (req, res) => {
    const dbPath = DB_PATH;
    const args = ['--db-path', dbPath];
    try {
      const payload = await runPythonJson(INGEST_STATS_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/ingest/stats] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to fetch ingest stats' });
    }
  });

  app.get('/api/logs/tail', requireAuthMiddleware, (req, res) => {
    try {
      const type = String(req.query?.type || 'pipeline').trim().toLowerCase();
      const corpusIdRaw = req.query?.corpus_id ?? req.query?.corpusId;
      const parsedCorpusId =
        corpusIdRaw === undefined || corpusIdRaw === null || String(corpusIdRaw).trim() === ''
          ? null
          : coerceInt(corpusIdRaw, null);
      const lines = Math.min(2000, Math.max(1, coerceInt(req.query?.lines, 200) || 200));
      const cursor = Math.max(0, coerceInt(req.query?.cursor, 0) || 0);
      const includeRotatedRaw = String(req.query?.include_rotated ?? req.query?.includeRotated ?? '1').trim().toLowerCase();
      const includeRotated = !['0', 'false', 'no'].includes(includeRotatedRaw);
      const { logDir, maxFiles, appLogFile, pipelineLogFile } = getLogSettings();

      let sourceFile = null;
      if (type === 'pipeline') {
        sourceFile = pipelineLogFile;
      } else if (type === 'app') {
        sourceFile = appLogFile;
      } else {
        return res.status(400).json({ error: "Invalid log type. Use 'pipeline' or 'app'." });
      }

      if (!fs.existsSync(logDir)) {
        return res.json({
          type,
          entries: [],
          count: 0,
          lines,
          cursor,
          next_cursor: cursor,
          has_more: false,
          total_lines: 0,
          include_rotated: includeRotated,
          source_file: sourceFile,
          source_exists: false,
        });
      }

      const window = readLogWindow(sourceFile, {
        limit: lines,
        cursor,
        includeRotated,
        maxFiles,
        corpusId: type === 'pipeline' && Number.isFinite(parsedCorpusId) ? parsedCorpusId : null,
      });
      return res.json({
        type,
        entries: window.entries,
        count: window.entries.length,
        lines,
        cursor: window.cursor,
        next_cursor: window.nextCursor,
        has_more: window.hasMore,
        total_lines: window.totalLines,
        include_rotated: includeRotated,
        source_file: sourceFile,
        source_exists: fs.existsSync(sourceFile),
      });
    } catch (error) {
      console.error('[/api/logs/tail] Error:', error);
      return res.status(500).json({ error: error?.message || 'Failed to tail logs' });
    }
  });

  app.get('/api/pipeline/worker/status', requireAuthMiddleware, (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({
        running: false,
        in_flight: false,
        started_at: null,
        last_tick_at: null,
        last_result: null,
        last_error: null,
        resolved_download_workers: 1,
        config: { intervalSeconds: 15, promoteBatchSize: 25, promoteWorkers: 6, downloadBatchSize: 5, downloadWorkers: 0 },
      });
    }
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    const state = getOrInitPipelineWorkerState(req.corpusId);
    return res.json({
      running: state.running,
      in_flight: state.inFlight,
      started_at: state.startedAt,
      last_tick_at: state.lastTickAt,
      last_result: state.lastResult,
      last_error: state.lastError,
      resolved_download_workers: state.resolvedDownloadWorkers,
      config: state.config,
    });
  });

  app.post('/api/pipeline/worker/start', requireAuthMiddleware, (req, res) => {
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    
    const state = getOrInitPipelineWorkerState(req.corpusId);
    state.running = true;
    
    return res.json({
      running: true,
      in_flight: false,
      config: state.config,
      resolved_download_workers: state.config.downloadWorkers || 0,
    });
  });

  app.post('/api/pipeline/worker/pause', requireAuthMiddleware, (req, res) => {
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    
    const state = getOrInitPipelineWorkerState(req.corpusId);
    state.running = false;
    return res.json({ running: false });
  });

  app.get('/api/downloads/worker/status', requireAuthMiddleware, (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ running: false, in_flight: false, last_result: null, last_error: null, config: { intervalSeconds: 60, batchSize: 3 } });
    }
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    const state = getOrInitWorkerState(req.corpusId);
    let queuedJobs = 0;
    let runningJobs = 0;
    try {
      const pendingRow = authDb
        .prepare(
          `SELECT COUNT(*) AS count
           FROM pipeline_jobs
           WHERE job_type = 'download'
             AND status = 'pending'
             AND corpus_id IS ?`
        )
        .get(req.corpusId ?? null);
      queuedJobs = Number(pendingRow?.count || 0);
      const runningRow = authDb
        .prepare(
          `SELECT COUNT(*) AS count
           FROM pipeline_jobs
           WHERE job_type = 'download'
             AND status = 'running'
             AND corpus_id IS ?`
        )
        .get(req.corpusId ?? null);
      runningJobs = Number(runningRow?.count || 0);
    } catch (error) {
      // Best-effort; keep legacy in-memory state if query fails.
    }

    const queueActive = queuedJobs > 0 || runningJobs > 0;
    const legacyIntervalActive = Boolean(state.running && state.intervalId);
    const effectiveRunning = queueActive || legacyIntervalActive;
    return res.json({
      running: effectiveRunning,
      in_flight: state.inFlight || runningJobs > 0,
      started_at: state.startedAt,
      last_tick_at: state.lastTickAt,
      last_result: state.lastResult,
      last_error: state.lastError,
      config: state.config,
      queued_jobs: queuedJobs,
      running_jobs: runningJobs,
    });
  });

  app.post('/api/downloads/worker/start', requireAuthMiddleware, (req, res) => {
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    const state = getOrInitWorkerState(req.corpusId);
    const perJobMax = Math.max(1, Math.min(5000, coerceInt(process.env.RAG_FEEDER_DOWNLOAD_JOB_MAX, 150) || 150));
    const queueDepth = countQueuedDownloads(req.corpusId);
    const existingJobs = countDownloadPipelineJobs(req.corpusId);
    const existingOutstanding = Number(existingJobs.pending || 0) + Number(existingJobs.running || 0);
    const desiredJobs = queueDepth > 0 ? Math.ceil(queueDepth / perJobMax) : 0;
    const jobsToQueue = Math.max(0, desiredJobs - existingOutstanding);
    
    try {
      let queuedJobs = 0;
      if (jobsToQueue > 0) {
        const enqueueMany = authDb.transaction(() => {
          for (let index = 0; index < jobsToQueue; index += 1) {
            const remaining = Math.max(1, queueDepth - index * perJobMax);
            const batchSize = Math.max(1, Math.min(perJobMax, remaining));
            const queued = enqueuePipelineJob({
              corpusId: req.corpusId,
              jobType: 'download',
              parameters: { batchSize },
            });
            if (queued.inserted) {
              queuedJobs += 1;
            }
          }
        });
        enqueueMany();
      }

      // Queue-driven mode still reports a worker state in UI; keep it in sync.
      state.running = queueDepth > 0 || existingOutstanding > 0 || queuedJobs > 0;
      if (state.running) {
        state.startedAt = state.startedAt || new Date().toISOString();
      }
      state.lastTickAt = new Date().toISOString();
      state.lastError = null;
      state.config = { ...state.config, batchSize: perJobMax };

      return res.json({ 
        running: state.running,
        queued_jobs: queuedJobs,
        queued_download_items: queueDepth,
        outstanding_jobs_before: existingOutstanding,
        desired_jobs: desiredJobs,
        per_job_max: perJobMax,
        batch_size: perJobMax,
        mode: 'auto',
        message: queuedJobs > 0
          ? `Queued ${queuedJobs} download job(s) to drain ${queueDepth} queued item(s).`
          : 'Download backlog already covered by existing queued/running jobs.'
      });
    } catch (error) {
      console.error('[/api/downloads/worker/start] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to queue download job' });
    }
  });

  app.post('/api/downloads/worker/stop', requireAuthMiddleware, (req, res) => {
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    const state = getOrInitWorkerState(req.corpusId);
    state.running = false;
    if (state.intervalId) {
      clearInterval(state.intervalId);
      state.intervalId = null;
    }
    const cancelledJobs = cancelDownloadJobs(req.corpusId, {
      includeRunning: true,
      reason: 'download_worker_stop',
    });
    return res.json({ running: false, cancelled_jobs: cancelledJobs });
  });

  app.post('/api/downloads/worker/run-once', requireAuthMiddleware, async (req, res) => {
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    const batchSize = resolveAutoDownloadBatchSize(req.corpusId);
    
    try {
      const result = enqueuePipelineJob({
        corpusId: req.corpusId,
        jobType: 'download',
        parameters: { batchSize },
        dedupeStatuses: ['pending', 'running'],
      });

      return res.json({ 
        success: true, 
        job_id: result.jobId,
        batch_size: batchSize,
        mode: 'auto',
        message: result.inserted
          ? 'Download job queued successfully (auto throughput mode).'
          : 'Download queue already has pending or running work for this corpus.'
      });
    } catch (error) {
      console.error('[/api/downloads/worker/run-once] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to queue download job' });
    }
  });

  return app;
}
