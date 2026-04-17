import express from 'express';
import cors from 'cors';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import crypto from 'crypto';
import { spawn, spawnSync } from 'child_process';
import Database from 'better-sqlite3';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import nodemailer from 'nodemailer';
import {
  ensureSeedSchema,
  upsertSearchRunCorpus,
  listSeedSources,
  listSeedCandidates,
  hideSeedSource,
  dismissSeedCandidates,
} from './seed.js';

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
const FRONTEND_URL = process.env.RAG_FEEDER_FRONTEND_URL || 'https://genkia.de';
const SMTP_FROM = process.env.RAG_FEEDER_SMTP_FROM || process.env.SMTP_FROM || 'noreply@example.com';
const PYTHON_SCRIPTS_DIR = path.join(__dirname, '..', 'scripts');
const KEYWORD_SEARCH_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'keyword_search.py');
const CORPUS_LIST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'corpus_list.py');
const DOWNLOADS_LIST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'downloads_list.py');
const GRAPH_EXPORT_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'graph_export.py');
const INGEST_LATEST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_latest.py');
const INGEST_ENQUEUE_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_enqueue.py');
const INGEST_RUNS_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_runs.py');
const INGEST_STATS_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_stats.py');
const SEED_PROMOTE_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'seed_promote.py');
const EXPORT_BUNDLE_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'export_bundle.py');
const PYTHON_EXEC = process.env.RAG_FEEDER_PYTHON || 'python3';
const KANTROPOS_CORPORA_ROOT =
  process.env.RAG_FEEDER_KANTROPOS_CORPORA_ROOT || '/data/projects/kantropos/corpora';
const EMPTY_ZIP_BUFFER = Buffer.from([
  0x50, 0x4b, 0x05, 0x06,
  0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,
  0x00, 0x00,
]);

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

function slugifyKantroposCorpusName(name) {
  return String(name || '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function parseKantroposTargets() {
  const defaultNames = ['Anthropozän & Nachhaltiges Management'];
  const raw = String(process.env.RAG_FEEDER_KANTROPOS_CORPORA || '').trim();
  let values = defaultNames;
  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length > 0) {
        values = parsed;
      }
    } catch {
      values = raw
        .split(/\r?\n|,/)
        .map((value) => value.trim())
        .filter(Boolean);
    }
  }
  return values
    .map((entry) => {
      if (typeof entry === 'string') {
        const name = entry.trim();
        if (!name) return null;
        return {
          id: slugifyKantroposCorpusName(name),
          name,
          path: path.join(KANTROPOS_CORPORA_ROOT, name),
        };
      }
      if (!entry || typeof entry !== 'object') return null;
      const name = String(entry.name || '').trim();
      if (!name) return null;
      const id = String(entry.id || slugifyKantroposCorpusName(name)).trim();
      const targetPath = String(entry.path || path.join(KANTROPOS_CORPORA_ROOT, name)).trim();
      if (!id || !targetPath) return null;
      return { id, name, path: targetPath };
    })
    .filter(Boolean);
}

const KANTROPOS_TARGETS = parseKantroposTargets();
let smtpTransport = null;

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
      status: 'matched',
      doi: '10.1017/cbo9780511613807.002',
      openalex_id: 'W2175056322',
    },
    {
      id: 2,
      title: 'Markets and Hierarchies',
      year: 1975,
      source: 'Keyword search',
      status: 'queued_download',
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
      email TEXT,
      email_verified_at TIMESTAMP,
      account_status TEXT NOT NULL DEFAULT 'active',
      invited_by_user_id INTEGER,
      invited_at TIMESTAMP,
      password_set_at TIMESTAMP,
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
    CREATE TABLE IF NOT EXISTS corpus_kantropos_assignments (
      corpus_id INTEGER PRIMARY KEY,
      target_id TEXT NOT NULL,
      target_name TEXT NOT NULL,
      target_path TEXT NOT NULL,
      updated_by_user_id INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS auth_invite_tokens (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      email TEXT NOT NULL,
      token_hash TEXT NOT NULL UNIQUE,
      expires_at TIMESTAMP NOT NULL,
      used_at TIMESTAMP,
      created_by_user_id INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS password_reset_tokens (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      email TEXT NOT NULL,
      token_hash TEXT NOT NULL UNIQUE,
      expires_at TIMESTAMP NOT NULL,
      used_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS works (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      authors TEXT,
      editors TEXT,
      year INTEGER,
      doi TEXT,
      normalized_doi TEXT,
      openalex_id TEXT,
      semantic_scholar_id TEXT,
      source TEXT,
      publisher TEXT,
      volume TEXT,
      issue TEXT,
      pages TEXT,
      type TEXT,
      url TEXT,
      isbn TEXT,
      issn TEXT,
      abstract TEXT,
      keywords TEXT,
      normalized_title TEXT,
      normalized_authors TEXT,
      metadata_status TEXT NOT NULL DEFAULT 'pending',
      metadata_source TEXT,
      metadata_error TEXT,
      metadata_updated_at TIMESTAMP,
      download_status TEXT NOT NULL DEFAULT 'not_requested',
      download_source TEXT,
      download_error TEXT,
      downloaded_at TIMESTAMP,
      metadata_claimed_by TEXT,
      metadata_claimed_at INTEGER,
      metadata_lease_expires_at INTEGER,
      metadata_attempt_count INTEGER NOT NULL DEFAULT 0,
      metadata_last_attempt_at INTEGER,
      download_claimed_by TEXT,
      download_claimed_at INTEGER,
      download_lease_expires_at INTEGER,
      download_attempt_count INTEGER NOT NULL DEFAULT 0,
      download_last_attempt_at INTEGER,
      file_path TEXT,
      file_checksum TEXT,
      origin_type TEXT,
      origin_key TEXT,
      source_pdf TEXT,
      run_id INTEGER,
      source_work_id INTEGER,
      relationship_type TEXT,
      crossref_json TEXT,
      openalex_json TEXT,
      bibtex_entry_json TEXT,
      status_notes TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS corpus_works (
      corpus_id INTEGER NOT NULL,
      work_id INTEGER NOT NULL,
      added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (corpus_id, work_id)
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
    CREATE TABLE IF NOT EXISTS daemon_heartbeat (
      daemon_name TEXT PRIMARY KEY,
      worker_id TEXT,
      status TEXT,
      details_json TEXT,
      last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
  ensureColumn(db, 'users', 'email', 'TEXT');
  ensureColumn(db, 'users', 'email_verified_at', 'TIMESTAMP');
  ensureColumn(db, 'users', 'account_status', "TEXT NOT NULL DEFAULT 'active'");
  ensureColumn(db, 'users', 'invited_by_user_id', 'INTEGER');
  ensureColumn(db, 'users', 'invited_at', 'TIMESTAMP');
  ensureColumn(db, 'users', 'password_set_at', 'TIMESTAMP');
  db.exec(`CREATE INDEX IF NOT EXISTS idx_corpus_works_lookup ON corpus_works(corpus_id, work_id);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_corpus_kantropos_assignments_target ON corpus_kantropos_assignments(target_id);`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique ON users(email COLLATE NOCASE) WHERE email IS NOT NULL;`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_auth_invite_tokens_user ON auth_invite_tokens(user_id, used_at, expires_at);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_password_reset_tokens_user ON password_reset_tokens(user_id, used_at, expires_at);`);
  db.prepare("UPDATE users SET account_status = 'active' WHERE account_status IS NULL OR TRIM(account_status) = ''").run();
}

function bootstrapDefaultCorpus(db, authConfig = resolveAuthConfig()) {
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
      .run(defaultCorpusNameForUsername(authConfig.adminUsername), adminId);
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
  if (tableExists(db, 'ingest_entries')) {
    db.prepare('UPDATE ingest_entries SET corpus_id = ? WHERE corpus_id IS NULL').run(corpusId);
  }
}

function pruneStaleCorpusItems(db) {
  if (!tableExists(db, 'works') || !tableExists(db, 'corpus_works')) return;
  db.exec(
    `DELETE FROM corpus_works
      WHERE NOT EXISTS (
        SELECT 1 FROM works w
         WHERE w.id = corpus_works.work_id
      )`
  );
}

function defaultCorpusNameForUsername(username) {
  const base = String(username || '').trim() || 'User';
  const suffix = base.endsWith('s') ? "' Default Corpus" : "'s Default Corpus";
  return `${base}${suffix}`;
}

function listCorporaForUser(db, userId) {
  return db
    .prepare(
      `SELECT c.id, c.name, c.owner_user_id, owner.username AS owner_username, uc.role, c.created_at
       FROM corpora c
       JOIN user_corpora uc ON uc.corpus_id = c.id
       LEFT JOIN users owner ON owner.id = c.owner_user_id
       WHERE uc.user_id = ?
       ORDER BY c.created_at DESC, c.id DESC`
    )
    .all(userId);
}

function getCorpusRoleForUser(db, userId, corpusId) {
  if (!userId || !corpusId) return null;
  return db
    .prepare('SELECT role FROM user_corpora WHERE user_id = ? AND corpus_id = ?')
    .get(userId, corpusId)?.role;
}

function canConfigureCorpus(role) {
  return role === 'owner' || role === 'editor';
}

function getKantroposTargetById(targetId) {
  return KANTROPOS_TARGETS.find((target) => String(target.id) === String(targetId)) || null;
}

function getCorpusKantroposAssignment(db, corpusId) {
  if (!Number.isFinite(Number(corpusId)) || Number(corpusId) <= 0) return null;
  const row = db
    .prepare(
      `SELECT a.corpus_id, a.target_id, a.target_name, a.target_path, a.updated_at, u.username AS updated_by_username
       FROM corpus_kantropos_assignments a
       LEFT JOIN users u ON u.id = a.updated_by_user_id
       WHERE a.corpus_id = ?`
    )
    .get(Number(corpusId));
  if (!row) return null;
  const target = getKantroposTargetById(row.target_id);
  return {
    corpus_id: Number(row.corpus_id),
    target_id: row.target_id,
    target_name: row.target_name,
    target_path: row.target_path,
    updated_at: row.updated_at || '',
    updated_by_username: row.updated_by_username || '',
    is_known_target: Boolean(target),
  };
}

function getUserById(db, userId) {
  return db
    .prepare('SELECT id, username, email, account_status, last_corpus_id FROM users WHERE id = ?')
    .get(userId);
}

function canonicalEmail(value) {
  const email = String(value || '').trim().toLowerCase();
  return email || null;
}

function generateOpaqueToken() {
  return crypto.randomBytes(32).toString('hex');
}

function hashOpaqueToken(token) {
  return crypto.createHash('sha256').update(String(token || '')).digest('hex');
}

function timestampPlusHours(hours) {
  return new Date(Date.now() + Math.max(0, Number(hours) || 0) * 60 * 60 * 1000)
    .toISOString()
    .replace('T', ' ')
    .replace(/\.\d{3}Z$/, '');
}

function isNonDeliverableRecipient(to) {
  const recipients = Array.isArray(to) ? to : [to];
  return recipients.every((recipient) => {
    const email = canonicalEmail(recipient);
    if (!email) return true;
    const domain = email.split('@')[1] || '';
    return (
      domain === 'example.com' ||
      domain.endsWith('.example') ||
      domain.endsWith('.invalid') ||
      domain.endsWith('.test') ||
      domain === 'localhost'
    );
  });
}

function initSmtpTransport() {
  if (smtpTransport) return smtpTransport;
  const host = process.env.RAG_FEEDER_SMTP_HOST || process.env.SMTP_HOST;
  if (!host) {
    smtpTransport = {
      async sendMail(options) {
        const firstLink = String(options.html || '').match(/href="([^"]+)"/)?.[1] || 'N/A';
        console.log('[mail] Console delivery');
        console.log('  To:', options.to);
        console.log('  Subject:', options.subject);
        console.log('  Link:', firstLink);
        return { messageId: `console-${Date.now()}` };
      },
    };
    return smtpTransport;
  }

  const port = Number(process.env.RAG_FEEDER_SMTP_PORT || process.env.SMTP_PORT || 587);
  const secure = String(process.env.RAG_FEEDER_SMTP_SECURE || process.env.SMTP_SECURE || 'false') === 'true';
  const user = process.env.RAG_FEEDER_SMTP_USER || process.env.SMTP_USER || '';
  const pass = process.env.RAG_FEEDER_SMTP_PASS || process.env.SMTP_PASS || '';
  const ignoreTLS = String(process.env.RAG_FEEDER_SMTP_IGNORE_TLS || process.env.SMTP_IGNORE_TLS || 'false') === 'true';
  const rejectUnauthorized = String(process.env.RAG_FEEDER_SMTP_TLS_REJECT_UNAUTHORIZED || process.env.SMTP_TLS_REJECT_UNAUTHORIZED || 'true') !== 'false';
  const transportOptions = {
    host,
    port,
    secure,
    tls: { rejectUnauthorized },
  };
  if (ignoreTLS) {
    transportOptions.ignoreTLS = true;
  }
  if (user || pass) {
    transportOptions.auth = { user, pass };
  }
  smtpTransport = nodemailer.createTransport(transportOptions);
  return smtpTransport;
}

async function sendTransactionalMail({ to, subject, html, text }) {
  if (isNonDeliverableRecipient(to)) {
    return initSmtpTransport().sendMail({ to, subject, html, text, from: SMTP_FROM });
  }
  const transport = initSmtpTransport();
  return transport.sendMail({
    from: `"Korpus Builder" <${SMTP_FROM}>`,
    to,
    subject,
    html,
    text,
  });
}

function issueAuthToken(user, authConfig = resolveAuthConfig()) {
  return jwt.sign({ sub: user.id, username: user.username }, authConfig.jwtSecret, {
    expiresIn: '7d',
  });
}

function authUserPayload(user, lastCorpusId = null, authConfig = resolveAuthConfig()) {
  return {
    id: user.id,
    username: user.username,
    email: user.email || null,
    account_status: user.account_status || 'active',
    last_corpus_id: lastCorpusId,
    is_admin: isAdminUser(user, authConfig),
  };
}

function isAdminUser(user, authConfig = resolveAuthConfig()) {
  return String(user?.username || '') === authConfig.adminUsername;
}

function requireAuth(db, authConfig = resolveAuthConfig()) {
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
      const payload = jwt.verify(token, authConfig.jwtSecret);
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
      0
    ),
    relatedDepthUpstream: coercePositiveInt(
      coerceInt(body?.relatedDepthUpstream, explicitDepth ?? null),
      RECURSION_DEFAULTS.keyword.relatedDepthUpstream,
      0
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
    args.push('--related-depth', String(0));
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
      0
    ),
    relatedDepthDownstream: coercePositiveInt(
      coerceInt(body?.relatedDepthDownstream, explicitDepth ?? null),
      RECURSION_DEFAULTS.uploadedDocs.relatedDepthDownstream,
      0
    ),
    relatedDepthUpstream: coercePositiveInt(
      coerceInt(body?.relatedDepthUpstream, explicitDepth ?? null),
      RECURSION_DEFAULTS.uploadedDocs.relatedDepthUpstream,
      0
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
    args.push('--related-depth-downstream', String(0));
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
    relatedDepthDownstream: Math.max(0, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH, 0) || 0),
    relatedDepthUpstream: Math.max(0, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH_UPSTREAM, 0) || 0),
    maxRelated: Math.max(1, coerceInt(process.env.RAG_FEEDER_MAX_RELATED, 30) || 30),
    includeDownstream: coerceBoolEnv(process.env.RAG_FEEDER_INCLUDE_DOWNSTREAM, false),
    includeUpstream: coerceBoolEnv(process.env.RAG_FEEDER_INCLUDE_UPSTREAM, false),
  },
  uploadedDocs: {
    relatedDepth: Math.max(0, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH, 0) || 0),
    relatedDepthDownstream: Math.max(0, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH, 0) || 0),
    relatedDepthUpstream: Math.max(0, coerceInt(process.env.RAG_FEEDER_RELATED_DEPTH_UPSTREAM, 0) || 0),
    maxRelated: Math.max(1, coerceInt(process.env.RAG_FEEDER_MAX_RELATED, 30) || 30),
    includeDownstream: coerceBoolEnv(process.env.RAG_FEEDER_INCLUDE_DOWNSTREAM, false),
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
  ensureSeedSchema(authDb);
  const defaultCorpusId = bootstrapDefaultCorpus(authDb, authConfig);
  migrateExistingToCorpus(authDb, defaultCorpusId);
  pruneStaleCorpusItems(authDb);
  const requireAuthMiddleware = requireAuth(authDb, authConfig);
  const hasWorkerAccess = (req) => {
    if (isStubMode()) return true;
    const role = getCorpusRoleForUser(authDb, req.user?.id, req.corpusId);
    return role === 'owner' || role === 'editor';
  };
  const hasAdminWorkerAccess = (req) => {
    if (isStubMode()) return true;
    return isAdminUser(req.user, authConfig);
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

  function parseSqliteTimestampToMs(value) {
    const text = String(value || '').trim();
    if (!text) return null;
    const normalized = text.includes('T') ? text : text.replace(' ', 'T');
    const withZone = /[zZ]|[+-]\d{2}:\d{2}$/.test(normalized) ? normalized : `${normalized}Z`;
    const ms = Date.parse(withZone);
    return Number.isFinite(ms) ? ms : null;
  }

  function readPipelineDaemonStatus() {
    const staleAfterSeconds = Math.max(5, coerceInt(process.env.RAG_FEEDER_DAEMON_STALE_SECONDS, 15) || 15);
    if (!tableExists(authDb, 'daemon_heartbeat')) {
      return {
        online: false,
        worker_id: null,
        status: 'unknown',
        last_seen_at: null,
        stale_after_seconds: staleAfterSeconds,
      };
    }

    const rows = authDb
      .prepare(
        `SELECT daemon_name, worker_id, status, details_json, last_seen_at
         FROM daemon_heartbeat
         WHERE daemon_name IN ('pipeline', 'enrich', 'download')`
      )
      .all();

    if (!rows || rows.length === 0) {
      return {
        online: false,
        worker_id: null,
        status: 'offline',
        details: null,
        last_seen_at: null,
        stale_after_seconds: staleAfterSeconds,
      };
    }
    const nowMs = Date.now();
    const workers = {};
    let anyOnline = false;
    let latestLastSeen = null;
    let latestWorker = null;
    for (const row of rows) {
      const lastSeenMs = parseSqliteTimestampToMs(row.last_seen_at);
      const online = Number.isFinite(lastSeenMs) ? nowMs - lastSeenMs <= staleAfterSeconds * 1000 : false;
      let details = null;
      try {
        details = row.details_json ? JSON.parse(row.details_json) : null;
      } catch (error) {
        details = null;
      }
      workers[row.daemon_name] = {
        online,
        worker_id: row.worker_id || null,
        status: row.status || (online ? 'online' : 'offline'),
        details,
        last_seen_at: row.last_seen_at || null,
      };
      if (online) anyOnline = true;
      if (!latestLastSeen || String(row.last_seen_at || '') > String(latestLastSeen)) {
        latestLastSeen = row.last_seen_at || null;
        latestWorker = workers[row.daemon_name];
      }
    }
    return {
      online: anyOnline,
      worker_id: latestWorker?.worker_id || null,
      status: latestWorker?.status || (anyOnline ? 'online' : 'offline'),
      details: latestWorker?.details || null,
      last_seen_at: latestWorker?.last_seen_at || null,
      workers,
      stale_after_seconds: staleAfterSeconds,
    };
  }

  function readWorkerBacklogSummary(corpusId) {
    const scopedCorpusId = corpusId ?? null;
    const countScoped = (whereSql = '', params = []) => {
      if (!tableExists(authDb, 'works')) return 0;
      const normalizedWhere = String(whereSql || '').trim();
      try {
        if (scopedCorpusId === null) {
          const row = authDb
            .prepare(
              `SELECT COUNT(*) AS count
               FROM works
               ${normalizedWhere ? `WHERE ${normalizedWhere}` : ''}`
            )
            .get(...params);
          return Number(row?.count || 0);
        }
        const row = authDb
          .prepare(
            `SELECT COUNT(*) AS count
             FROM works t
             JOIN corpus_works cw ON cw.work_id = t.id
             WHERE cw.corpus_id = ?${normalizedWhere ? ` AND ${normalizedWhere}` : ''}`
          )
          .get(scopedCorpusId, ...params);
        return Number(row?.count || 0);
      } catch (error) {
        return 0;
      }
    };

    return {
      corpus_id: scopedCorpusId,
      raw_pending: countScoped("COALESCE(metadata_status, 'pending') = 'pending'"),
      enriching: countScoped("COALESCE(metadata_status, 'pending') = 'in_progress'"),
      matched: countScoped("metadata_status = 'matched' AND COALESCE(download_status, 'not_requested') = 'not_requested'"),
      queued_download: countScoped("COALESCE(download_status, 'not_requested') = 'queued'"),
      downloading: countScoped("COALESCE(download_status, 'not_requested') = 'in_progress'"),
      downloaded: countScoped("download_status = 'downloaded'"),
      failed_enrichment: countScoped("metadata_status = 'failed'"),
      failed_download: countScoped("download_status = 'failed'"),
    };
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
      enqueuePipelineJob(corpusId, 'download', { batchSize }, { dedupeStatuses: ['pending', 'running'] });
    } catch (error) {
      state.lastError = error?.message || String(error);
      console.error(`[download-worker ERROR] corpus=${corpusTag} ${state.lastError}`);
    } finally {
      state.inFlight = false;
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
      .get(String(jobType), corpusId ?? null, ...statuses);
  }

  function enqueuePipelineJob(corpusId, jobType, parameters = {}, { dedupeStatuses = [] } = {}) {
    const tx = authDb.transaction(() => {
      const existing = dedupeStatuses.length
        ? findExistingPipelineJob(jobType, corpusId, dedupeStatuses)
        : null;
      if (existing) {
        return Number(existing.id);
      }
      return Number(
        authDb
          .prepare(
            "INSERT INTO pipeline_jobs (corpus_id, job_type, status, parameters_json) VALUES (?, ?, 'pending', ?)"
          )
          .run(corpusId ?? null, String(jobType), JSON.stringify(parameters || {}))
          .lastInsertRowid
      );
    });
    return tx();
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

  function cancelAllDownloadJobs({ includeRunning = true, reason = 'stopped_by_user' } = {}) {
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
               AND status = 'pending'`
          )
          .run(resultPayload).changes || 0
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
                 AND status = 'running'`
            )
            .run(resultPayload).changes || 0
        );
      }
    } catch (error) {
      console.warn(`[download-worker] Failed to cancel all download jobs: ${error?.message || error}`);
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

  function countPipelineTickJobs(corpusId) {
    if (!tableExists(authDb, 'pipeline_jobs')) {
      return { pending: 0, running: 0 };
    }
    try {
      const pending = Number(
        authDb
          .prepare(
            `SELECT COUNT(*) AS count
             FROM pipeline_jobs
             WHERE job_type = 'pipeline_tick'
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
             WHERE job_type = 'pipeline_tick'
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

  function countQueuedDownloads(corpusId) {
    try {
      if (corpusId !== undefined && corpusId !== null) {
        const row = authDb
          .prepare(
            `SELECT COUNT(*) AS count
             FROM works w
             JOIN corpus_works cw ON cw.work_id = w.id
             WHERE cw.corpus_id = ?
               AND w.download_status IN ('queued', 'in_progress')`
          )
          .get(corpusId);
        return Number(row?.count || 0);
      }
      const row = authDb
        .prepare("SELECT COUNT(*) AS count FROM works WHERE download_status IN ('queued', 'in_progress')")
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
      const outstanding = countPipelineTickJobs(corpusId);
      if (Number(outstanding.pending || 0) > 0 || Number(outstanding.running || 0) > 0) {
        state.inFlight = false;
        return;
      }
      enqueuePipelineJob(corpusId, 'pipeline_tick', { config: state.config }, { dedupeStatuses: ['pending', 'running'] });
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

  function pauseAllPipelineWorkers({ force = false } = {}) {
    const result = {
      requestedForce: Boolean(force),
      stopped: 0,
      inFlight: 0,
      wasRunning: 0,
      states: {},
    };
    for (const [key, state] of pipelineWorkers.entries()) {
      const hadInFlight = Boolean(state.inFlight);
      const hadRunning = Boolean(state.running);
      state.running = false;
      if (state.intervalId) {
        clearInterval(state.intervalId);
        state.intervalId = null;
      }
      if (force && Array.isArray(state.children)) {
        state.children.forEach((child) => {
          try {
            child.kill('SIGTERM');
          } catch (error) {
            // ignore
          }
        });
      }
      if (force) {
        state.inFlight = false;
        state.children = [];
      }
      const keyLabel = key === 0 ? 'none' : String(key);
      result.states[keyLabel] = {
        running: state.running,
        inFlight: state.inFlight,
      };
      if (hadRunning) {
        result.wasRunning += 1;
      }
      if (hadInFlight) {
        result.inFlight += 1;
      }
      if (hadRunning || hadInFlight) {
        result.stopped += 1;
      }
    }
    return result;
  }

  function stopAllDownloadWorkers({ force = false } = {}) {
    const result = {
      requestedForce: Boolean(force),
      stopped: 0,
      inFlight: 0,
      wasRunning: 0,
      states: {},
    };
    for (const [key, state] of downloadWorkers.entries()) {
      const hadInFlight = Boolean(state.inFlight);
      const hadRunning = Boolean(state.running);
      state.running = false;
      if (state.intervalId) {
        clearInterval(state.intervalId);
        state.intervalId = null;
      }
      if (force && state.child) {
        try {
          state.child.kill('SIGTERM');
        } catch (error) {
          // ignore
        }
      }
      if (force) {
        state.inFlight = false;
        state.child = null;
      }
      const keyLabel = key === 0 ? 'none' : String(key);
      result.states[keyLabel] = {
        running: state.running,
        inFlight: state.inFlight,
      };
      if (hadRunning) {
        result.wasRunning += 1;
      }
      if (hadInFlight) {
        result.inFlight += 1;
      }
      if (hadRunning || hadInFlight) {
        result.stopped += 1;
      }
    }
    return result;
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

  function createUserWithDefaultCorpus({ username, passwordHash, email = null, accountStatus = 'active', invitedByUserId = null }) {
    const result = authDb
      .prepare(
        `INSERT INTO users (username, password_hash, email, account_status, invited_by_user_id, invited_at)
         VALUES (?, ?, ?, ?, ?, CASE WHEN ? IS NOT NULL THEN CURRENT_TIMESTAMP ELSE NULL END)`
      )
      .run(username, passwordHash, canonicalEmail(email), accountStatus, invitedByUserId, invitedByUserId);
    const userId = Number(result.lastInsertRowid);
    const corpus = authDb
      .prepare('INSERT INTO corpora (name, owner_user_id) VALUES (?, ?)')
      .run(defaultCorpusNameForUsername(username), userId);
    const corpusId = Number(corpus.lastInsertRowid);
    authDb
      .prepare("INSERT INTO user_corpora (user_id, corpus_id, role) VALUES (?, ?, 'owner')")
      .run(userId, corpusId);
    authDb.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(corpusId, userId);
    return { userId, corpusId };
  }

  async function sendInviteEmail({ userId, username, email, invitedByUserId = null }) {
    const token = generateOpaqueToken();
    const tokenHash = hashOpaqueToken(token);
    authDb.prepare('DELETE FROM auth_invite_tokens WHERE user_id = ? AND used_at IS NULL').run(userId);
    authDb
      .prepare(
        `INSERT INTO auth_invite_tokens (user_id, email, token_hash, expires_at, created_by_user_id)
         VALUES (?, ?, ?, ?, ?)`
      )
      .run(userId, canonicalEmail(email), tokenHash, timestampPlusHours(72), invitedByUserId);
    const inviteUrl = `${FRONTEND_URL}/#/accept-invite?token=${encodeURIComponent(token)}`;
    const html = `
      <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#22313d;line-height:1.5">
        <h2>Set your Korpus Builder password</h2>
        <p>An account has been created for you.</p>
        <p><strong>Username:</strong> ${username}</p>
        <p>Use the link below to set your password and activate the account.</p>
        <p><a href="${inviteUrl}" style="display:inline-block;padding:12px 18px;border-radius:999px;background:#0f8b8d;color:#fff;text-decoration:none;font-weight:600">Set password</a></p>
        <p>If the button does not work, open this link:</p>
        <p style="word-break:break-all">${inviteUrl}</p>
        <p>This link expires in 72 hours.</p>
      </div>
    `;
    const text = `An account has been created for you in Korpus Builder.\n\nUsername: ${username}\n\nSet your password here:\n${inviteUrl}\n\nThis link expires in 72 hours.`;
    const delivery = await sendTransactionalMail({
      to: canonicalEmail(email),
      subject: 'Set your Korpus Builder password',
      html,
      text,
    });
    return { token, inviteUrl, messageId: delivery?.messageId || null };
  }

  async function sendPasswordResetEmail({ userId, username, email }) {
    const token = generateOpaqueToken();
    const tokenHash = hashOpaqueToken(token);
    authDb.prepare('DELETE FROM password_reset_tokens WHERE user_id = ? AND used_at IS NULL').run(userId);
    authDb
      .prepare(
        `INSERT INTO password_reset_tokens (user_id, email, token_hash, expires_at)
         VALUES (?, ?, ?, ?)`
      )
      .run(userId, canonicalEmail(email), tokenHash, timestampPlusHours(2));
    const resetUrl = `${FRONTEND_URL}/#/reset-password?token=${encodeURIComponent(token)}`;
    const html = `
      <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#22313d;line-height:1.5">
        <h2>Reset your Korpus Builder password</h2>
        <p>Hello ${username},</p>
        <p>Use the link below to set a new password.</p>
        <p><a href="${resetUrl}" style="display:inline-block;padding:12px 18px;border-radius:999px;background:#0f8b8d;color:#fff;text-decoration:none;font-weight:600">Reset password</a></p>
        <p>If the button does not work, open this link:</p>
        <p style="word-break:break-all">${resetUrl}</p>
        <p>This link expires in 2 hours.</p>
      </div>
    `;
    const text = `Reset your Korpus Builder password:\n${resetUrl}\n\nThis link expires in 2 hours.`;
    const delivery = await sendTransactionalMail({
      to: canonicalEmail(email),
      subject: 'Reset your Korpus Builder password',
      html,
      text,
    });
    return { token, resetUrl, messageId: delivery?.messageId || null };
  }

  // --- Auth endpoints ---
  app.post('/api/auth/login', (req, res) => {
    const { username, password } = req.body || {};
    if (!username || !password) {
      return res.status(400).json({ error: 'Missing username or password' });
    }
    const user = authDb
      .prepare('SELECT id, username, email, password_hash, account_status, last_corpus_id FROM users WHERE username = ?')
      .get(username);
    if (user?.account_status === 'invited') {
      return res.status(403).json({ error: 'Invitation pending. Use the link from your email to set your password.' });
    }
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
    return res.json({
      token: issueAuthToken(user, authConfig),
      user: authUserPayload(user, lastCorpusId, authConfig),
    });
  });

  app.post('/api/auth/register', (req, res) => {
    if (!isStubMode()) {
      return res.status(404).json({ error: 'Not found' });
    }
    const { username, password, email } = req.body || {};
    if (!username || !password || !email) {
      return res.status(400).json({ error: 'Missing username, password, or email' });
    }
    const normalizedEmail = canonicalEmail(email);
    if (!normalizedEmail || !normalizedEmail.includes('@')) {
      return res.status(400).json({ error: 'Invalid email address' });
    }
    const existing = authDb.prepare('SELECT id FROM users WHERE username = ?').get(username);
    if (existing) {
      return res.status(409).json({ error: 'User already exists' });
    }
    const existingEmail = authDb.prepare('SELECT id FROM users WHERE email = ? COLLATE NOCASE').get(normalizedEmail);
    if (existingEmail) {
      return res.status(409).json({ error: 'Email already exists' });
    }
    const hash = bcrypt.hashSync(password, 10);
    const { userId } = createUserWithDefaultCorpus({
      username,
      passwordHash: hash,
      email: normalizedEmail,
      accountStatus: 'active',
    });
    authDb.prepare('UPDATE users SET email_verified_at = CURRENT_TIMESTAMP, password_set_at = CURRENT_TIMESTAMP WHERE id = ?').run(userId);
    return res.status(201).json({ id: userId, username, email: normalizedEmail });
  });

  app.post('/api/auth/invitations', requireAuthMiddleware, async (req, res) => {
    if (!isAdminUser(req.user, authConfig)) {
      return res.status(403).json({ error: 'Only admins can create invited users' });
    }
    const username = String(req.body?.username || '').trim();
    const email = canonicalEmail(req.body?.email);
    if (!username || !email || !email.includes('@')) {
      return res.status(400).json({ error: 'Missing or invalid username/email' });
    }
    if (authDb.prepare('SELECT id FROM users WHERE username = ?').get(username)) {
      return res.status(409).json({ error: 'User already exists' });
    }
    if (authDb.prepare('SELECT id FROM users WHERE email = ? COLLATE NOCASE').get(email)) {
      return res.status(409).json({ error: 'Email already exists' });
    }
    try {
      const placeholderHash = bcrypt.hashSync(generateOpaqueToken(), 10);
      const { userId } = createUserWithDefaultCorpus({
        username,
        passwordHash: placeholderHash,
        email,
        accountStatus: 'invited',
        invitedByUserId: req.user.id,
      });
      const delivery = await sendInviteEmail({ userId, username, email, invitedByUserId: req.user.id });
      return res.status(201).json({
        invited: true,
        user_id: userId,
        username,
        email,
        preview_url: delivery.inviteUrl,
      });
    } catch (error) {
      console.error('[auth invite] Failed:', error);
      return res.status(500).json({ error: error?.message || 'Failed to create invitation' });
    }
  });

  app.post('/api/auth/accept-invite', async (req, res) => {
    const token = String(req.body?.token || '').trim();
    const password = String(req.body?.password || '');
    if (!token || !password || password.length < 8) {
      return res.status(400).json({ error: 'Missing token or password too short' });
    }
    const tokenHash = hashOpaqueToken(token);
    const tokenRow = authDb
      .prepare(
        `SELECT it.id, it.user_id, it.email, u.username, u.last_corpus_id
         FROM auth_invite_tokens it
         JOIN users u ON u.id = it.user_id
         WHERE it.token_hash = ?
           AND it.used_at IS NULL
           AND datetime(it.expires_at) > datetime('now')`
      )
      .get(tokenHash);
    if (!tokenRow) {
      return res.status(400).json({ error: 'Invite link is invalid or expired' });
    }
    const passwordHash = bcrypt.hashSync(password, 10);
    authDb.transaction(() => {
      authDb
        .prepare(
          `UPDATE users
           SET password_hash = ?, account_status = 'active', password_set_at = CURRENT_TIMESTAMP,
               email_verified_at = COALESCE(email_verified_at, CURRENT_TIMESTAMP)
           WHERE id = ?`
        )
        .run(passwordHash, tokenRow.user_id);
      authDb.prepare('UPDATE auth_invite_tokens SET used_at = CURRENT_TIMESTAMP WHERE id = ?').run(tokenRow.id);
    })();
    const user = authDb
      .prepare('SELECT id, username, email, account_status, last_corpus_id FROM users WHERE id = ?')
      .get(tokenRow.user_id);
    let lastCorpusId = user.last_corpus_id;
    if (!lastCorpusId) {
      lastCorpusId = authDb
        .prepare('SELECT corpus_id FROM user_corpora WHERE user_id = ? ORDER BY created_at DESC LIMIT 1')
        .get(user.id)?.corpus_id || defaultCorpusId;
      authDb.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(lastCorpusId, user.id);
    }
    return res.json({
      accepted: true,
      token: issueAuthToken(user, authConfig),
      user: authUserPayload(user, lastCorpusId, authConfig),
    });
  });

  app.post('/api/auth/forgot-password', async (req, res) => {
    const email = canonicalEmail(req.body?.email);
    if (!email || !email.includes('@')) {
      return res.status(400).json({ error: 'Enter a valid email address' });
    }
    const user = authDb
      .prepare(
        `SELECT id, username, email, account_status
         FROM users
         WHERE email = ? COLLATE NOCASE`
      )
      .get(email);
    if (user && user.account_status === 'active') {
      try {
        await sendPasswordResetEmail({ userId: user.id, username: user.username, email: user.email });
      } catch (error) {
        console.error('[auth forgot-password] Failed:', error);
      }
    }
    return res.json({ sent: true });
  });

  app.post('/api/auth/reset-password', async (req, res) => {
    const token = String(req.body?.token || '').trim();
    const password = String(req.body?.password || '');
    if (!token || !password || password.length < 8) {
      return res.status(400).json({ error: 'Missing token or password too short' });
    }
    const tokenHash = hashOpaqueToken(token);
    const tokenRow = authDb
      .prepare(
        `SELECT prt.id, prt.user_id, u.username, u.email, u.last_corpus_id, u.account_status
         FROM password_reset_tokens prt
         JOIN users u ON u.id = prt.user_id
         WHERE prt.token_hash = ?
           AND prt.used_at IS NULL
           AND datetime(prt.expires_at) > datetime('now')`
      )
      .get(tokenHash);
    if (!tokenRow || tokenRow.account_status !== 'active') {
      return res.status(400).json({ error: 'Reset link is invalid or expired' });
    }
    const passwordHash = bcrypt.hashSync(password, 10);
    authDb.transaction(() => {
      authDb
        .prepare('UPDATE users SET password_hash = ?, password_set_at = CURRENT_TIMESTAMP WHERE id = ?')
        .run(passwordHash, tokenRow.user_id);
      authDb.prepare('UPDATE password_reset_tokens SET used_at = CURRENT_TIMESTAMP WHERE id = ?').run(tokenRow.id);
    })();
    const user = authDb
      .prepare('SELECT id, username, email, account_status, last_corpus_id FROM users WHERE id = ?')
      .get(tokenRow.user_id);
    let lastCorpusId = user.last_corpus_id;
    if (!lastCorpusId) {
      lastCorpusId = authDb
        .prepare('SELECT corpus_id FROM user_corpora WHERE user_id = ? ORDER BY created_at DESC LIMIT 1')
        .get(user.id)?.corpus_id || defaultCorpusId;
      authDb.prepare('UPDATE users SET last_corpus_id = ? WHERE id = ?').run(lastCorpusId, user.id);
    }
    return res.json({
      reset: true,
      token: issueAuthToken(user, authConfig),
      user: authUserPayload(user, lastCorpusId, authConfig),
    });
  });

  app.get('/api/auth/me', requireAuthMiddleware, (req, res) => {
    const corpora = listCorporaForUser(authDb, req.user.id);
    return res.json({
      user: authUserPayload(req.user, req.user.last_corpus_id, authConfig),
      corpora,
    });
  });

  // --- Corpus management ---
  app.get('/api/corpora', requireAuthMiddleware, (req, res) => {
    const corpora = listCorporaForUser(authDb, req.user.id);
    return res.json({ corpora });
  });

  app.get('/api/kantropos/corpora', requireAuthMiddleware, (_req, res) => {
    return res.json({
      root_path: KANTROPOS_CORPORA_ROOT,
      corpora: KANTROPOS_TARGETS,
    });
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

  app.get('/api/corpora/:id/kantropos-assignment', requireAuthMiddleware, (req, res) => {
    const corpusId = Number(req.params.id);
    const role = getCorpusRoleForUser(authDb, req.user.id, corpusId);
    if (!role) {
      return res.status(403).json({ error: 'No access to corpus' });
    }
    return res.json({
      assignment: getCorpusKantroposAssignment(authDb, corpusId),
      corpora: KANTROPOS_TARGETS,
      root_path: KANTROPOS_CORPORA_ROOT,
      can_edit: canConfigureCorpus(role) || isAdminUser(req.user, authConfig),
    });
  });

  app.put('/api/corpora/:id/kantropos-assignment', requireAuthMiddleware, (req, res) => {
    const corpusId = Number(req.params.id);
    const role = getCorpusRoleForUser(authDb, req.user.id, corpusId);
    if (!role) {
      return res.status(403).json({ error: 'No access to corpus' });
    }
    if (!(canConfigureCorpus(role) || isAdminUser(req.user, authConfig))) {
      return res.status(403).json({ error: 'Only corpus owners, editors, or the global admin can assign a Kantropos corpus' });
    }
    const nextTargetIdRaw = req.body?.targetId;
    const nextTargetId = nextTargetIdRaw === null || nextTargetIdRaw === undefined || nextTargetIdRaw === ''
      ? null
      : String(nextTargetIdRaw);

    if (!nextTargetId) {
      authDb.prepare('DELETE FROM corpus_kantropos_assignments WHERE corpus_id = ?').run(corpusId);
      return res.json({ saved: true, assignment: null });
    }

    const target = getKantroposTargetById(nextTargetId);
    if (!target) {
      return res.status(400).json({ error: 'Unknown Kantropos corpus target' });
    }

    authDb
      .prepare(
        `INSERT INTO corpus_kantropos_assignments (
           corpus_id, target_id, target_name, target_path, updated_by_user_id, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT(corpus_id) DO UPDATE SET
           target_id = excluded.target_id,
           target_name = excluded.target_name,
           target_path = excluded.target_path,
           updated_by_user_id = excluded.updated_by_user_id,
           updated_at = CURRENT_TIMESTAMP`
      )
      .run(corpusId, target.id, target.name, target.path, req.user.id);

    return res.json({
      saved: true,
      assignment: getCorpusKantroposAssignment(authDb, corpusId),
    });
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
        if (tableExists(authDb, 'corpus_kantropos_assignments')) {
          authDb.prepare('DELETE FROM corpus_kantropos_assignments WHERE corpus_id = ?').run(corpusId);
        }
        if (tableExists(authDb, 'corpus_works')) {
          authDb.prepare('DELETE FROM corpus_works WHERE corpus_id = ?').run(corpusId);
        }
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
              .run(defaultCorpusNameForUsername(authDb.prepare('SELECT username FROM users WHERE id = ?').get(userId)?.username), userId);
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
    const sendExtractionSignal = ({ status = 'success', mode = '', reason = '' } = {}) => {
      const parts = [
        '[extract-status]',
        `[corpus=${corpusTag}]`,
        `[base=${baseName}]`,
        `[file=${filename}]`,
        'done',
        `status=${status}`,
      ];
      if (mode) parts.push(`mode=${mode}`);
      if (reason) parts.push(`reason=${reason}`);
      send(parts.join(' '));
      sendEvent({
        kind: 'extraction',
        event: 'done',
        corpus_id: Number.isFinite(Number(corpusId)) ? Number(corpusId) : null,
        base_name: baseName,
        filename,
        status,
        mode: mode || null,
        reason: reason || null,
      });
    };
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
        inlineModelOverride = null,
        label = 'APIscraper',
      }) => {
        return new Promise((resolve) => {
          const dbPath = DB_PATH;
          const scrapeApiArgs = ['--db-path', dbPath, '--ingest-source', baseName, '--extract-mode', extractMode];
          if (inputDir) scrapeApiArgs.push('--input-dir', inputDir);
          if (inputPdf) scrapeApiArgs.push('--input-pdf', inputPdf);
          if (chunkPages && Number(chunkPages) > 0) scrapeApiArgs.push('--chunk-pages', String(chunkPages));
          if (uploadedFileName) scrapeApiArgs.push('--uploaded-file-name', uploadedFileName);
          scrapeApiArgs.push('--seed-only');
          if (corpusId) scrapeApiArgs.push('--corpus-id', String(corpusId));

          console.log(`[/api/extract-bibliography] Spawning ${label}: python ${API_SCRAPER_SCRIPT} ${scrapeApiArgs.join(' ')}`);
          send(`[/api/extract-bibliography][corpus=${corpusTag}] Starting ${label}...`);
          const child = spawn(PYTHON_EXEC, [API_SCRAPER_SCRIPT, ...scrapeApiArgs], {
            env: {
              ...process.env,
              ...(modelOverride ? { RAG_FEEDER_GEMINI_MODEL: modelOverride } : {}),
              ...(inlineModelOverride ? { RAG_FEEDER_API_INLINE_MODEL: inlineModelOverride } : {}),
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
        const chunkPages = inputPageCount && inputPageCount > 12 ? 10 : 0;
        const inlineUploadedFileName = chunkPages > 0 ? null : sharedUploadedFileName;
        const why = reason || 'reference page extraction returned no usable pages';
        console.warn(`[/api/extract-bibliography] Triggering inline citation fallback: ${why}`);
        send(
          `[/api/extract-bibliography][corpus=${corpusTag}] Triggering inline citation fallback (${why}, chunk_pages=${chunkPages || 0}).`
        );
        const ok = await runApiScraper({
          inputPdf: inputPdfPath,
          extractMode: 'inline',
          chunkPages,
          uploadedFileName: inlineUploadedFileName,
          inlineModelOverride: 'gemini-3.1-pro-preview',
          label: 'APIscraper inline fallback',
        });
        if (ok) {
          sendExtractionSignal({ status: 'success', mode: 'inline', reason: why });
        }
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
        })
          .then((ok) => {
            sendExtractionSignal({ status: ok ? 'success' : 'failed', mode: 'page_reuse' });
          })
          .finally(() => cleanupSharedUpload());
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
            const ok = await runInlineFallback(`get_bib_pages failed with code ${code}`);
            if (!ok) {
              sendExtractionSignal({ status: 'failed', mode: 'inline', reason: `get_bib_pages_exit_${code}` });
            }
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
            const ok = await runInlineFallback('no extracted bibliography pages found');
            if (!ok) {
              sendExtractionSignal({ status: 'failed', mode: 'inline', reason: 'no_extracted_pages' });
            }
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
            sendExtractionSignal({ status: 'success', mode: 'page' });
          } else {
            sendExtractionSignal({ status: 'failed', mode: 'page' });
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
    const author = req.body?.author?.trim() || '';
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
    if (query && author) args.push('--author', author);
    if (query && yearFrom) args.push('--year-from', String(yearFrom));
    if (query && yearTo) args.push('--year-to', String(yearTo));
    if (mailto) args.push('--mailto', String(mailto));
    if (enqueue) args.push('--enqueue');
    applyKeywordSearchExpansionArgs(args, expansion);

    try {
      const payload = await runPythonJson(KEYWORD_SEARCH_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      if (Number.isFinite(Number(payload?.runId)) && Number(payload.runId) > 0 && Number.isFinite(Number(req.corpusId))) {
        upsertSearchRunCorpus(authDb, {
          searchRunId: Number(payload.runId),
          corpusId: Number(req.corpusId),
        });
      }
      return res.json(payload);
    } catch (error) {
      console.error('[/api/keyword-search] Error:', error);
      return res.status(500).json({ error: error.message || 'Keyword search failed' });
    }
  });

  app.get('/api/seed/sources', requireAuthMiddleware, (req, res) => {
    try {
      const limit = Math.max(1, Math.min(500, coerceInt(req.query?.limit, 100) || 100));
      const sources = listSeedSources(authDb, req.corpusId, { limit });
      return res.json({ source: 'db', sources, total: sources.length });
    } catch (error) {
      console.error('[/api/seed/sources] Error:', error);
      return res.status(500).json({ error: error?.message || 'Failed to load seed sources' });
    }
  });

  app.get('/api/seed/sources/:sourceType/:sourceKey/candidates', requireAuthMiddleware, (req, res) => {
    try {
      const sourceType = String(req.params?.sourceType || '').trim().toLowerCase();
      const sourceKey = String(req.params?.sourceKey || '').trim();
      if (!['pdf', 'search'].includes(sourceType) || !sourceKey) {
        return res.status(400).json({ error: 'Invalid seed source' });
      }
      const candidates = listSeedCandidates(authDb, req.corpusId, sourceType, sourceKey);
      const sourceSummary = listSeedSources(authDb, req.corpusId, { limit: 500 }).find(
        (source) => source.source_type === sourceType && String(source.source_key) === sourceKey
      ) || null;
      return res.json({
        source: 'db',
        source_summary: sourceSummary,
        candidates,
        total: candidates.length,
      });
    } catch (error) {
      console.error('[/api/seed/sources/:sourceType/:sourceKey/candidates] Error:', error);
      return res.status(500).json({ error: error?.message || 'Failed to load seed candidates' });
    }
  });

  app.post('/api/seed/sources/:sourceType/:sourceKey/promote', requireAuthMiddleware, async (req, res) => {
    const sourceType = String(req.params?.sourceType || '').trim().toLowerCase();
    const sourceKey = String(req.params?.sourceKey || '').trim();
    if (!['pdf', 'search'].includes(sourceType) || !sourceKey) {
      return res.status(400).json({ error: 'Invalid seed source' });
    }

    const rawCandidateKeys = Array.isArray(req.body?.candidateKeys) ? req.body.candidateKeys : [];
    const requestedCandidateKeys = [...new Set(rawCandidateKeys.map((value) => String(value || '').trim()).filter(Boolean))];
    const workers = coerceInt(req.body?.workers, 6);
    const enqueueDownload = req.body?.enqueueDownload === undefined ? true : Boolean(req.body.enqueueDownload);
    const downloadBatchSize = coerceInt(req.body?.downloadBatchSize, 25);
    const expansion = buildUploadedDocsExpansion({
      relatedDepth: req.body?.relatedDepth,
      maxRelated: req.body?.maxRelated,
      includeDownstream: req.body?.includeDownstream,
      includeUpstream: req.body?.includeUpstream,
      relatedDepthDownstream: req.body?.relatedDepthDownstream,
      relatedDepthUpstream: req.body?.relatedDepthUpstream,
    });
    if (workers === null || workers === undefined || !Number.isFinite(workers) || workers <= 0) {
      return res.status(400).json({ error: 'workers must be a positive number' });
    }
    if (enqueueDownload && (downloadBatchSize === null || downloadBatchSize === undefined || !Number.isFinite(downloadBatchSize) || downloadBatchSize <= 0)) {
      return res.status(400).json({ error: 'downloadBatchSize must be a positive number' });
    }

    try {
      const availableCandidates = listSeedCandidates(authDb, req.corpusId, sourceType, sourceKey);
      const promotableCandidates = availableCandidates.filter((candidate) => {
        const state = String(candidate?.state || '').trim().toLowerCase();
        return !Boolean(candidate?.in_corpus) && state !== 'downloaded';
      });
      const selectedCandidates = requestedCandidateKeys.length > 0
        ? promotableCandidates.filter((candidate) => requestedCandidateKeys.includes(String(candidate.candidate_key || '')))
        : promotableCandidates;
      const promotionCandidates = selectedCandidates.map((candidate) => ({
        candidate_key: candidate?.candidate_key || null,
        source_type: sourceType,
        source_key: sourceKey,
        title: candidate?.title || null,
        authors: candidate?.authors || [],
        year: candidate?.year || null,
        doi: candidate?.doi || null,
        openalex_id: candidate?.openalex_id || null,
        source: candidate?.source || null,
        publisher: candidate?.publisher || null,
        url: candidate?.url || null,
        type: candidate?.type || null,
        abstract: candidate?.abstract || null,
        keywords: candidate?.keywords || null,
        source_pdf: candidate?.source_pdf || null,
        ingest_source: candidate?.ingest_source || null,
        run_id: candidate?.run_id || null,
      }));

      if (promotionCandidates.length === 0) {
        return res.status(400).json({ error: 'No selectable seed candidates found for promotion' });
      }

      const promotionArgs = [
        '--db-path',
        DB_PATH,
      ];
      let tempCandidatesPath = null;
      const promotionCandidatesJson = JSON.stringify(promotionCandidates);
      if (promotionCandidatesJson.length > 100_000) {
        tempCandidatesPath = path.join(
          os.tmpdir(),
          `seed-promote-${req.corpusId}-${Date.now()}-${Math.random().toString(36).slice(2)}.json`
        );
        fs.writeFileSync(tempCandidatesPath, promotionCandidatesJson, 'utf8');
        promotionArgs.push('--candidates-file', tempCandidatesPath);
      } else {
        promotionArgs.push('--candidates-json', promotionCandidatesJson);
      }
      if (enqueueDownload) {
        promotionArgs.push('--enqueue-download');
      }

      let promotion;
      try {
        promotion = await runPythonJson(
          SEED_PROMOTE_SCRIPT,
          promotionArgs,
          { dbPath: DB_PATH, corpusId: req.corpusId }
        );
      } finally {
        if (tempCandidatesPath) {
          try {
            fs.unlinkSync(tempCandidatesPath);
          } catch (cleanupError) {
            console.warn('[/api/seed/sources/:sourceType/:sourceKey/promote] Failed to remove temp candidates file:', cleanupError);
          }
        }
      }

      const pendingWorkIds = [...new Set((promotion?.pending_work_ids || []).map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0))];
      const shouldQueueEnrich = pendingWorkIds.length > 0;
      const shouldQueueDownload = Boolean(
        enqueueDownload && (
          shouldQueueEnrich ||
          Number(promotion?.queued_existing_download || 0) > 0 ||
          Number(promotion?.already_enriched || 0) > 0 ||
          Number(promotion?.already_queued_download || 0) > 0 ||
          Number(promotion?.failed_download_existing || 0) > 0
        )
      );

      const jobs = [];
      let enrichJobId = null;
      let downloadJobId = null;
      if (shouldQueueEnrich) {
        enrichJobId = enqueuePipelineJob(req.corpusId, 'enrich', {
          limit: pendingWorkIds.length,
          workers,
          expansion,
          pending_work_ids: pendingWorkIds,
        });
        jobs.push({ id: enrichJobId, type: 'enrich' });
      }
      if (shouldQueueDownload) {
        downloadJobId = enqueuePipelineJob(req.corpusId, 'download', {
          batchSize: downloadBatchSize,
        }, { dedupeStatuses: ['pending', 'running'] });
        jobs.push({ id: downloadJobId, type: 'download' });
      }

      return res.json({
        success: true,
        request_id: '',
        jobs,
        tracking: null,
        enrich_job_id: enrichJobId,
        download_job_id: downloadJobId,
        promotion,
        backlog: readWorkerBacklogSummary(req.corpusId),
        message: shouldQueueEnrich || shouldQueueDownload
          ? 'Selected candidates were added to the corpus pipeline. Background workers will continue from DB state.'
          : 'Selected candidates were already reflected in the corpus state.',
      });
    } catch (error) {
      console.error('[/api/seed/sources/:sourceType/:sourceKey/promote] Error:', error);
      return res.status(500).json({ error: error?.message || 'Failed to promote seed candidates' });
    }
  });

  app.post('/api/seed/candidates/dismiss', requireAuthMiddleware, (req, res) => {
    try {
      const sourceType = String(req.body?.sourceType || '').trim().toLowerCase();
      const sourceKey = String(req.body?.sourceKey || '').trim();
      const candidateKeys = Array.isArray(req.body?.candidateKeys) ? req.body.candidateKeys : [];
      if (!['pdf', 'search'].includes(sourceType) || !sourceKey) {
        return res.status(400).json({ error: 'Invalid seed source' });
      }
      if (candidateKeys.length === 0) {
        return res.status(400).json({ error: 'candidateKeys must be a non-empty array' });
      }
      const dismissed = dismissSeedCandidates(authDb, req.corpusId, sourceType, sourceKey, candidateKeys);
      return res.json({ success: true, dismissed });
    } catch (error) {
      console.error('[/api/seed/candidates/dismiss] Error:', error);
      return res.status(500).json({ error: error?.message || 'Failed to dismiss seed candidates' });
    }
  });

  app.delete('/api/seed/sources/:sourceType/:sourceKey', requireAuthMiddleware, (req, res) => {
    try {
      const sourceType = String(req.params?.sourceType || '').trim().toLowerCase();
      const sourceKey = String(req.params?.sourceKey || '').trim();
      if (!['pdf', 'search'].includes(sourceType) || !sourceKey) {
        return res.status(400).json({ error: 'Invalid seed source' });
      }
      const removed = hideSeedSource(authDb, req.corpusId, sourceType, sourceKey);
      return res.json({ success: true, removed });
    } catch (error) {
      console.error('[/api/seed/sources/:sourceType/:sourceKey] Error:', error);
      return res.status(500).json({ error: error?.message || 'Failed to remove seed source' });
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

  app.get('/api/corpus/downloaded/:id/file', requireAuthMiddleware, async (req, res) => {
    const itemId = coerceInt(req.params?.id, null);
    if (!Number.isFinite(itemId) || itemId <= 0) {
      return res.status(400).json({ error: 'Invalid downloaded item id' });
    }

    try {
      const db = new Database(DB_PATH, { readonly: true });
      const row = db.prepare(
        `SELECT w.id, w.title, w.file_path
           FROM works w
           JOIN corpus_works cw ON cw.work_id = w.id
          WHERE cw.corpus_id = ?
            AND w.id = ?
            AND w.download_status = 'downloaded'
          LIMIT 1`
      ).get(req.corpusId, itemId);
      db.close();

      if (!row) {
        return res.status(404).json({ error: 'Downloaded file not found in this corpus' });
      }

      const filePath = path.resolve(String(row.file_path || ''));
      if (!filePath || !fs.existsSync(filePath)) {
        return res.status(404).json({ error: 'Downloaded file is missing on disk' });
      }

      const downloadName = path.basename(filePath);
      return res.download(filePath, downloadName);
    } catch (error) {
      console.error('[/api/corpus/downloaded/:id/file] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to download corpus file' });
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

    if (process.env.RAG_FEEDER_STUB === '1') {
      if (selectedFormat === 'json') {
        const payload = JSON.stringify({
          generated_at: new Date().toISOString(),
          total: STUB_RESULTS.corpus.length,
          items: STUB_RESULTS.corpus,
        }, null, 2);
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.setHeader('Content-Disposition', 'attachment; filename="export_stub.json"');
        return res.status(200).send(payload);
      }
      if (selectedFormat === 'bibtex') {
        const payload = '@article{stub_export,\n  title = {Stub Export}\n}\n';
        res.setHeader('Content-Type', 'application/x-bibtex; charset=utf-8');
        res.setHeader('Content-Disposition', 'attachment; filename="export_stub.bib"');
        return res.status(200).send(payload);
      }
      res.setHeader('Content-Type', 'application/zip');
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="${selectedFormat === 'bundle_zip' ? 'export_stub_bundle.zip' : 'export_stub_pdfs.zip'}"`
      );
      return res.status(200).send(EMPTY_ZIP_BUFFER);
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
      const markedRow = authDb
        .prepare(
          `SELECT COUNT(*) AS count
           FROM works w
           JOIN corpus_works cw ON cw.work_id = w.id
           WHERE cw.corpus_id = ?
             AND COALESCE(w.metadata_status, 'pending') IN ('pending', 'in_progress')
             AND COALESCE(w.download_status, 'not_requested') = 'not_requested'`
        )
        .get(req.corpusId);
      const marked = Number(markedRow?.count || 0);

      const jobs = [];
      let enrichJobId = null;
      let downloadJobId = null;
      if (marked > 0) {
        enrichJobId = enqueuePipelineJob(req.corpusId, 'enrich', {
          limit,
          workers,
          expansion,
        });
        jobs.push({ id: enrichJobId, type: 'enrich' });
      }
      if (enqueueDownload) {
        downloadJobId = enqueuePipelineJob(req.corpusId, 'download', {
          batchSize: downloadBatchSize,
        }, { dedupeStatuses: ['pending', 'running'] });
        jobs.push({ id: downloadJobId, type: 'download' });
      }

      return res.json({
        success: true,
        request_id: '',
        job_id: enrichJobId,
        enrich_job_id: enrichJobId,
        download_job_id: downloadJobId,
        jobs,
        tracking: null,
        marked,
        backlog: readWorkerBacklogSummary(req.corpusId),
        message: enqueueDownload
          ? 'Marked entries remain queued in the database. Background workers will enrich and download them automatically.'
          : 'Marked entries remain queued in the database. Background workers will enrich them automatically.'
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
    const globalScope = String(req.query?.global || '').trim() === '1';
    if (globalScope && !isAdminUser(req.user, authConfig)) {
      return res.status(403).json({ error: 'Admin access required for global ingest stats' });
    }
    const args = ['--db-path', dbPath];
    const corpusId = globalScope ? null : req.corpusId;
    if (corpusId !== null && corpusId !== undefined) {
      args.push('--corpus-id', String(corpusId));
    }
    try {
      const payload = await runPythonJson(INGEST_STATS_SCRIPT, args, { dbPath, corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/ingest/stats] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to fetch ingest stats' });
    }
  });

  app.get('/api/pipeline/daemon/status', requireAuthMiddleware, (req, res) => {
    try {
      const globalScope = String(req.query?.global || '').trim() === '1';
      if (globalScope && !isAdminUser(req.user, authConfig)) {
        return res.status(403).json({ error: 'Admin access required for global daemon status' });
      }
      return res.json({
        source: 'db',
        daemon: readPipelineDaemonStatus(),
        backlog: readWorkerBacklogSummary(globalScope ? null : req.corpusId),
      });
    } catch (error) {
      console.error('[/api/pipeline/daemon/status] Error:', error);
      return res.status(500).json({ error: error?.message || 'Failed to fetch daemon status' });
    }
  });

  app.get('/api/pipeline/jobs/summary', requireAuthMiddleware, (req, res) => {
    try {
      if (!tableExists(authDb, 'pipeline_jobs')) {
        return res.json({
          source: 'db',
          counts: {
            pending: 0,
            running: 0,
            completed: 0,
            failed: 0,
            cancelled: 0,
            other: 0,
            total: 0,
          },
          jobs: [],
          daemon: readPipelineDaemonStatus(),
        });
      }

      const rawJobIds = String(req.query?.job_ids ?? req.query?.jobIds ?? '').trim();
      const requestedJobIds = rawJobIds
        ? [...new Set(rawJobIds.split(',').map((part) => Number(part.trim())).filter((v) => Number.isFinite(v) && v > 0))]
        : [];
      const rawTypes = String(req.query?.types || '').trim().toLowerCase();
      const allowedTypes = new Set(['enrich', 'download', 'pipeline_tick']);
      const requestedTypes = rawTypes
        ? [...new Set(rawTypes.split(',').map((part) => part.trim()).filter((value) => allowedTypes.has(value)))]
        : [];
      const recentLimit = Math.max(1, Math.min(500, coerceInt(req.query?.recent_limit ?? req.query?.recentLimit, 50) || 50));
      const limit = Math.max(recentLimit, requestedJobIds.length || 0);

      const whereClauses = ['corpus_id IS ?'];
      const params = [req.corpusId ?? null];
      if (requestedJobIds.length > 0) {
        whereClauses.push(`id IN (${requestedJobIds.map(() => '?').join(', ')})`);
        params.push(...requestedJobIds);
      }
      if (requestedTypes.length > 0) {
        whereClauses.push(`job_type IN (${requestedTypes.map(() => '?').join(', ')})`);
        params.push(...requestedTypes);
      }

      const rows = authDb
        .prepare(
          `SELECT id, corpus_id, job_type, status, parameters_json, result_json, created_at, started_at, finished_at
           FROM pipeline_jobs
           WHERE ${whereClauses.join(' AND ')}
           ORDER BY created_at DESC, id DESC
           LIMIT ?`
        )
        .all(...params, limit);

      const counts = {
        pending: 0,
        running: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
        other: 0,
        total: rows.length,
      };

      const jobs = rows.map((row) => {
        const status = String(row.status || '').trim().toLowerCase();
        if (Object.prototype.hasOwnProperty.call(counts, status)) {
          counts[status] += 1;
        } else {
          counts.other += 1;
        }

        let paramsJson = null;
        let resultJson = null;
        try {
          paramsJson = row.parameters_json ? JSON.parse(row.parameters_json) : null;
        } catch (error) {
          paramsJson = null;
        }
        try {
          resultJson = row.result_json ? JSON.parse(row.result_json) : null;
        } catch (error) {
          resultJson = row.result_json || null;
        }
        const errorMessage =
          typeof resultJson === 'object' && resultJson !== null
            ? resultJson.error || null
            : typeof resultJson === 'string'
              ? resultJson
              : null;

        return {
          id: Number(row.id),
          corpus_id: row.corpus_id,
          job_type: row.job_type,
          status: row.status,
          request_id: paramsJson?.request_id || null,
          parameters: paramsJson,
          created_at: row.created_at,
          started_at: row.started_at,
          finished_at: row.finished_at,
          error: errorMessage,
          result: resultJson,
        };
      });

      return res.json({
        source: 'db',
        counts,
        filters: {
          corpus_id: req.corpusId ?? null,
          job_ids: requestedJobIds,
          types: requestedTypes,
          limit,
        },
        jobs,
        daemon: readPipelineDaemonStatus(),
      });
    } catch (error) {
      console.error('[/api/pipeline/jobs/summary] Error:', error);
      return res.status(500).json({ error: error?.message || 'Failed to fetch job summary' });
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
    if (!hasAdminWorkerAccess(req)) {
      return res.status(403).json({ error: 'Admin access required for diagnostics worker status' });
    }
    const daemon = readPipelineDaemonStatus();
    const enrich = daemon?.workers?.enrich || {};
    const backlog = readWorkerBacklogSummary(req.corpusId);
    return res.json({
      running: Boolean(enrich.online),
      in_flight: String(enrich.status || '').toLowerCase() === 'running',
      started_at: enrich.last_seen_at || null,
      last_tick_at: enrich.last_seen_at || null,
      last_result: enrich.details || null,
      last_error: enrich.status === 'error' ? JSON.stringify(enrich.details || {}) : null,
      resolved_download_workers: Boolean((daemon?.workers?.download || {}).online) ? 1 : 0,
      config: { mode: 'db-driven' },
      backlog,
    });
  });

  app.post('/api/pipeline/worker/start', requireAuthMiddleware, (req, res) => {
    if (!hasAdminWorkerAccess(req)) {
      return res.status(403).json({ error: 'Admin access required to manage pipeline workers' });
    }

    return res.json({
      running: true,
      in_flight: false,
      config: { mode: 'db-driven' },
      message: 'DB-driven enrich/download workers run continuously. Manual pipeline start is no longer used.',
    });
  });

  app.post('/api/pipeline/worker/pause', requireAuthMiddleware, (req, res) => {
    if (!hasAdminWorkerAccess(req)) {
      return res.status(403).json({ error: 'Admin access required to manage pipeline workers' });
    }

    return res.json({
      running: true,
      in_flight: false,
      force: Boolean(req.body?.force),
      message: 'DB-driven workers are managed outside the app. Manual pipeline pause is no longer used.',
    });
  });

  app.post('/api/pipeline/worker/pause-all', requireAuthMiddleware, (req, res) => {
    return res.status(404).json({ error: 'Not found' });
  });

  app.get('/api/downloads/worker/status', requireAuthMiddleware, (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ running: false, in_flight: false, last_result: null, last_error: null, config: { intervalSeconds: 60, batchSize: 3 } });
    }
    if (!hasAdminWorkerAccess(req)) {
      return res.status(403).json({ error: 'Admin access required for diagnostics worker status' });
    }
    const daemon = readPipelineDaemonStatus();
    const download = daemon?.workers?.download || {};
    const backlog = readWorkerBacklogSummary(req.corpusId);
    return res.json({
      running: Boolean(download.online),
      in_flight: String(download.status || '').toLowerCase() === 'running',
      started_at: download.last_seen_at || null,
      last_tick_at: download.last_seen_at || null,
      last_result: download.details || null,
      last_error: download.status === 'error' ? JSON.stringify(download.details || {}) : null,
      config: { mode: 'db-driven' },
      queued_jobs: Number(backlog.queued_download || 0),
      running_jobs: Number(backlog.downloading || 0),
      queued_download_items: Number(backlog.queued_download || 0),
    });
  });

  app.post('/api/downloads/worker/start', requireAuthMiddleware, (req, res) => {
    if (!hasAdminWorkerAccess(req)) {
      return res.status(403).json({ error: 'Admin access required to manage download workers' });
    }
    const backlog = readWorkerBacklogSummary(req.corpusId);
    return res.json({
      running: true,
      queued_jobs: 0,
      queued_download_items: Number(backlog.queued_download || 0),
      outstanding_jobs_before: 0,
      desired_jobs: 0,
      per_job_max: 0,
      batch_size: 0,
      mode: 'db-driven',
      message: 'DB-driven download worker runs continuously. Manual start is no longer used.',
    });
  });

  app.post('/api/downloads/worker/stop', requireAuthMiddleware, (req, res) => {
    if (!hasAdminWorkerAccess(req)) {
      return res.status(403).json({ error: 'Admin access required to manage download workers' });
    }
    return res.json({
      running: true,
      cancelled_jobs: { pending: 0, running: 0 },
      message: 'DB-driven download worker is managed outside the app. Manual stop is no longer used.',
    });
  });

  app.post('/api/downloads/worker/run-once', requireAuthMiddleware, async (req, res) => {
    if (!hasAdminWorkerAccess(req)) {
      return res.status(403).json({ error: 'Admin access required to manage download workers' });
    }
    const backlog = readWorkerBacklogSummary(req.corpusId);
    return res.json({
      success: true,
      job_id: null,
      batch_size: 0,
      mode: 'db-driven',
      queued_download_items: Number(backlog.queued_download || 0),
      message: 'DB-driven download worker runs continuously. Run once is no longer used.',
    });
  });

  if (process.env.NODE_ENV !== 'test') {
    console.log('[pipeline-worker] DB-driven enrich/download workers expected; no pipeline_jobs scheduler started.');
  }

  return app;
}
