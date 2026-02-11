import express from 'express';
import cors from 'cors';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import crypto from 'crypto';
import { spawn } from 'child_process';
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

// --- Define Container Paths ---
const UPLOADS_DIR = '/usr/src/app/uploads';
const DL_LIT_PROJECT_DIR =
  findUpwards(__dirname, 'dl_lit_project') || path.join(__dirname, '..', '..', 'dl_lit_project');
const DL_LIT_CODE_DIR = path.join(DL_LIT_PROJECT_DIR, 'dl_lit');
const ARTIFACTS_DIR = process.env.RAG_FEEDER_ARTIFACTS_DIR || path.join(DL_LIT_PROJECT_DIR, 'artifacts');
const BIB_OUTPUT_DIR = path.join(ARTIFACTS_DIR, 'bibliographies');
const GET_BIB_PAGES_SCRIPT = path.join(DL_LIT_CODE_DIR, 'get_bib_pages.py');
const API_SCRAPER_SCRIPT = path.join(DL_LIT_CODE_DIR, 'APIscraper_v2.py');
const REPO_ROOT = path.dirname(DL_LIT_PROJECT_DIR);
const DEFAULT_DB_PATH = path.join(DL_LIT_PROJECT_DIR, 'data', 'literature.db');
const DB_PATH = process.env.RAG_FEEDER_DB_PATH || DEFAULT_DB_PATH;
const JWT_SECRET = process.env.RAG_FEEDER_JWT_SECRET || crypto.randomUUID();
const ADMIN_USERNAME = process.env.RAG_ADMIN_USER || 'admin';
const ADMIN_PASSWORD = process.env.RAG_ADMIN_PASSWORD || 'admin';
const PYTHON_SCRIPTS_DIR = path.join(__dirname, '..', 'scripts');
const KEYWORD_SEARCH_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'keyword_search.py');
const CORPUS_LIST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'corpus_list.py');
const DOWNLOADS_LIST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'downloads_list.py');
const GRAPH_EXPORT_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'graph_export.py');
const INGEST_LATEST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_latest.py');
const INGEST_ENQUEUE_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_enqueue.py');
const INGEST_PROCESS_MARKED_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_process_marked.py');
const INGEST_RUNS_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_runs.py');
const INGEST_STATS_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'ingest_stats.py');
const DOWNLOAD_WORKER_ONCE_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'download_worker_once.py');
const PYTHON_EXEC = process.env.RAG_FEEDER_PYTHON || 'python3';

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
  `);
  if (tableExists(db, 'ingest_entries')) {
    ensureColumn(db, 'ingest_entries', 'corpus_id', 'INTEGER');
  }
  db.exec(`CREATE INDEX IF NOT EXISTS idx_corpus_items_lookup ON corpus_items(corpus_id, table_name, row_id);`);
}

function bootstrapDefaultCorpus(db) {
  const admin = db.prepare('SELECT id, last_corpus_id FROM users WHERE username = ?').get(ADMIN_USERNAME);
  let adminId = admin?.id;
  if (!adminId) {
    const hash = bcrypt.hashSync(ADMIN_PASSWORD, 10);
    const result = db
      .prepare('INSERT OR IGNORE INTO users (username, password_hash) VALUES (?, ?)')
      .run(ADMIN_USERNAME, hash);
    if (result.changes === 0) {
      adminId = db.prepare('SELECT id FROM users WHERE username = ?').get(ADMIN_USERNAME)?.id;
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

function requireAuth(db) {
  return (req, res, next) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
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
      const payload = jwt.verify(token, JWT_SECRET);
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
  const lines = output.trim().split(/\r?\n/).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i].trim();
    if (!line) continue;
    if (line.startsWith('{') || line.startsWith('[')) {
      return JSON.parse(line);
    }
  }
  throw new Error(`No JSON output from python. Output was: ${output}`);
}

function runPythonJson(scriptPath, args, { dbPath, corpusId } = {}) {
  return new Promise((resolve, reject) => {
    const env = {
      ...process.env,
      PYTHONPATH: [DL_LIT_PROJECT_DIR, REPO_ROOT, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
      RAG_FEEDER_DB_PATH: dbPath || DB_PATH,
    };

    const finalArgs = [...args];
    if (corpusId !== undefined && corpusId !== null) {
      finalArgs.push('--corpus-id', String(corpusId));
    }
    const child = spawn(PYTHON_EXEC, [scriptPath, ...finalArgs], { env });
    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    child.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    child.on('close', (code) => {
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
}

function coerceInt(value, fallback = null) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function createApp({ broadcast } = {}) {
  const app = express();
  const send = typeof broadcast === 'function' ? broadcast : () => {};
  const downloadWorkers = new Map(); // corpusId -> state

  // Ensure temporary and output directories exist
  fs.mkdirSync(ARTIFACTS_DIR, { recursive: true });
  fs.mkdirSync(BIB_OUTPUT_DIR, { recursive: true });

  fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
  const authDb = new Database(DB_PATH);
  ensureAuthSchema(authDb);
  const defaultCorpusId = bootstrapDefaultCorpus(authDb);
  migrateExistingToCorpus(authDb, defaultCorpusId);
  if (!process.env.RAG_FEEDER_JWT_SECRET) {
    console.warn('[auth] RAG_FEEDER_JWT_SECRET not set; tokens will reset on restart.');
  }
  const requireAuthMiddleware = requireAuth(authDb);
  const hasWorkerAccess = (req) => {
    if (process.env.RAG_FEEDER_STUB === '1') return true;
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

  function spawnDownloadOnce({ corpusId, batchSize, downloadDir }) {
    const dbPath = DB_PATH;
    const args = ['--db-path', dbPath, '--limit', String(batchSize)];
    if (downloadDir) {
      args.push('--download-dir', String(downloadDir));
    }

    const env = {
      ...process.env,
      PYTHONPATH: [DL_LIT_PROJECT_DIR, REPO_ROOT, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
      RAG_FEEDER_DB_PATH: dbPath,
    };
    if (corpusId !== undefined && corpusId !== null) {
      args.push('--corpus-id', String(corpusId));
    }

    const child = spawn(PYTHON_EXEC, [DOWNLOAD_WORKER_ONCE_SCRIPT, ...args], { env });
    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (data) => {
      const message = data.toString();
      stdout += message;
      message
        .split(/\r?\n/)
        .filter(Boolean)
        .forEach((line) => send(`[download-worker] ${line}`));
    });

    child.stderr.on('data', (data) => {
      const message = data.toString();
      stderr += message;
      message
        .split(/\r?\n/)
        .filter(Boolean)
        .forEach((line) => send(`[download-worker ERROR] ${line}`));
    });

    const done = new Promise((resolve, reject) => {
      child.on('close', (code) => {
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

  async function tickDownloadWorker(corpusId) {
    const state = getOrInitWorkerState(corpusId);
    if (!state.running || state.inFlight) return;
    state.inFlight = true;
    state.lastTickAt = new Date().toISOString();
    state.lastError = null;
    const { batchSize } = state.config;
    send(`[download-worker] tick corpus=${corpusId || 'none'} batch=${batchSize}`);
    try {
      const { child, done } = spawnDownloadOnce({ corpusId, batchSize });
      state.child = child;
      state.lastResult = await done;
      send(
        `[download-worker] done processed=${state.lastResult.processed} downloaded=${state.lastResult.downloaded} failed=${state.lastResult.failed} skipped=${state.lastResult.skipped}`
      );
    } catch (error) {
      state.lastError = error?.message || String(error);
      send(`[download-worker ERROR] ${state.lastError}`);
    } finally {
      state.child = null;
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
    if (force && state.child) {
      try {
        state.child.kill('SIGTERM');
      } catch (error) {
        // ignore
      }
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
    const token = jwt.sign({ sub: user.id, username: user.username }, JWT_SECRET, {
      expiresIn: '7d',
    });
    return res.json({ token, user: { id: user.id, username: user.username, last_corpus_id: lastCorpusId } });
  });

  app.post('/api/auth/register', (req, res) => {
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
  app.post('/api/extract-bibliography/:filename', requireAuthMiddleware, (req, res) => {
    const { filename } = req.params;
    const corpusId = req.corpusId;
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

    // --- Run Scripts in Background ---
    try {
      // Fast-path: if the upload is missing but we already have extracted page PDFs, run the scraper only.
      if (!inputExists && pagesExist) {
        console.log(`[/api/extract-bibliography] Upload missing; reusing extracted pages in ${existingPagesDir}`);
        send(`[/api/extract-bibliography] Upload missing; reusing extracted pages in ${existingPagesDir}`);
        const dbPath = DB_PATH;
        const scrapeApiArgs = ['--input-dir', existingPagesDir, '--db-path', dbPath, '--ingest-source', baseName];
        if (corpusId) {
          scrapeApiArgs.push('--corpus-id', String(corpusId));
        }
        const apiScraperProcess = spawn(PYTHON_EXEC, [API_SCRAPER_SCRIPT, ...scrapeApiArgs]);

        apiScraperProcess.stdout.on('data', (data) => {
          const message = data.toString();
          console.log(`[APIscraper stdout]: ${message}`);
          send(`[APIscraper] ${message}`);
        });

        apiScraperProcess.stderr.on('data', (data) => {
          const message = data.toString();
          console.error(`[APIscraper stderr]: ${message}`);
          send(`[APIscraper ERROR] ${message}`);
        });

        apiScraperProcess.on('close', (code) => {
          if (code !== 0) {
            console.error(`[/api/extract-bibliography] ${API_SCRAPER_SCRIPT} exited with code ${code}`);
            return;
          }
          console.log(`[/api/extract-bibliography] Finished ${API_SCRAPER_SCRIPT} (reused pages).`);
        });
        return;
      }

      // 3. Run get_bib_pages.py in background
      const getPagesArgs = [
        '--input-pdf', inputPdfPath,
        '--output-dir', BIB_OUTPUT_DIR // Keep this, it seems to place output in BIB_OUTPUT_DIR/baseName/
      ];

      console.log(`[/api/extract-bibliography] Spawning: python ${GET_BIB_PAGES_SCRIPT} ${getPagesArgs.join(' ')}`);
      const getPagesProcess = spawn(PYTHON_EXEC, [GET_BIB_PAGES_SCRIPT, ...getPagesArgs]);

      getPagesProcess.stdout.on('data', (data) => {
        const message = data.toString();
        console.log(`[get_bib_pages stdout]: ${message}`);
        send(`[get_bib_pages] ${message}`);
      });

      getPagesProcess.stderr.on('data', (data) => {
        const message = data.toString();
        console.error(`[get_bib_pages stderr]: ${message}`);
        send(`[get_bib_pages ERROR] ${message}`);
      });

      getPagesProcess.on('close', (code) => {
        console.log(`[/api/extract-bibliography] ${GET_BIB_PAGES_SCRIPT} exited with code ${code}`);
        if (code !== 0) {
          const errorMessage = `[/api/extract-bibliography] Error during bibliography page extraction (get_bib_pages.py exited with code ${code}).`;
          send(errorMessage);
          console.error(errorMessage);
          // Send error response only if headers not already sent
          if (!res.headersSent) {
            return res.status(500).json({ error: 'Error during bibliography page extraction.', scriptCode: code });
          }
          return; // Stop further processing
        }

        // **Proceed to Step 2: Call API Scraper only if Step 1 succeeded**
        // Construct the expected output dir based on the *output* structure of get_bib_pages
        const jsonSubDir = existingPagesDir; // Subdirectory named after the base PDF name
        const pageFiles = fs.existsSync(jsonSubDir)
          ? fs.readdirSync(jsonSubDir).filter((file) => file.endsWith('.pdf'))
          : [];

        if (pageFiles.length === 0) {
          const errorMessage = `[/api/extract-bibliography] No extracted page PDFs found in: ${jsonSubDir}`;
          console.error(errorMessage);
          send(errorMessage);
          if (!res.headersSent) {
            return res.status(500).json({ error: 'No extracted page PDFs found after page extraction.' });
          }
          return;
        }

        console.log(`[/api/extract-bibliography] Spawning: python ${API_SCRAPER_SCRIPT} ...`);
        // Correct arguments based on APIscraper_v2.py's argparse
        const dbPath = DB_PATH;
        const scrapeApiArgs = [
          '--input-dir', jsonSubDir,
          '--db-path', dbPath,
          '--ingest-source', baseName
        ];
        if (corpusId) {
          scrapeApiArgs.push('--corpus-id', String(corpusId));
        }
        const apiScraperProcess = spawn(PYTHON_EXEC, [API_SCRAPER_SCRIPT, ...scrapeApiArgs]);

        // Stream output via WebSocket (Already correctly implemented in previous step)
        apiScraperProcess.stdout.on('data', (data) => {
          const message = data.toString();
          console.log(`[APIscraper stdout]: ${message}`);
          send(`[APIscraper] ${message}`); // Broadcast stdout
        });

        apiScraperProcess.stderr.on('data', (data) => {
          const message = data.toString();
          console.error(`[APIscraper stderr]: ${message}`);
          send(`[APIscraper ERROR] ${message}`); // Broadcast stderr
        });

        apiScraperProcess.on('close', (code) => {
          if (code !== 0) {
            console.error(`[/api/extract-bibliography] ${API_SCRAPER_SCRIPT} exited with code ${code}`);
            // Stop processing
            return;
          }
          console.log(`[/api/extract-bibliography] Finished ${API_SCRAPER_SCRIPT}.`);
          console.log(`[/api/extract-bibliography] Bibliography extraction complete for ${filename}. Output in ${BIB_OUTPUT_DIR}.`);
        }); // End APIscraper callback
      }); // End get_bib_pages callback

    } catch (error) {
      // This catch block likely only catches synchronous errors like fs.mkdirSync failure
      console.error(`[/api/extract-bibliography] Synchronous error setting up processing for ${filename}:`, error);
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
    const relatedDepth = coerceInt(req.body?.relatedDepth, 1);
    const maxRelated = coerceInt(req.body?.maxRelated, 30);
    const dbPath = DB_PATH;

    const args = ['--db-path', dbPath, '--max-results', String(maxResults), '--field', String(field)];
    if (query) args.push('--query', query);
    if (!query && seedJson) {
      const seedPayload = typeof seedJson === 'string' ? seedJson : JSON.stringify(seedJson);
      args.push('--seed-json', seedPayload);
    }
    if (query && yearFrom) args.push('--year-from', String(yearFrom));
    if (query && yearTo) args.push('--year-to', String(yearTo));
    if (mailto) args.push('--mailto', String(mailto));
    if (enqueue) args.push('--enqueue');
    if (relatedDepth) args.push('--related-depth', String(relatedDepth));
    if (maxRelated) args.push('--max-related', String(maxRelated));

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
    if (limit === null || limit === undefined || !Number.isFinite(limit) || limit <= 0) {
      return res.status(400).json({ error: 'limit must be a positive number' });
    }

    const dbPath = DB_PATH;
    const args = ['--db-path', dbPath, '--limit', String(limit)];
    try {
      const payload = await runPythonJson(INGEST_PROCESS_MARKED_SCRIPT, args, { dbPath, corpusId: req.corpusId });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/ingest/process-marked] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to process marked ingest entries' });
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

  app.get('/api/downloads/worker/status', requireAuthMiddleware, (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ running: false, in_flight: false, last_result: null, last_error: null, config: { intervalSeconds: 60, batchSize: 3 } });
    }
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    const state = getOrInitWorkerState(req.corpusId);
    return res.json({
      running: state.running,
      in_flight: state.inFlight,
      started_at: state.startedAt,
      last_tick_at: state.lastTickAt,
      last_result: state.lastResult,
      last_error: state.lastError,
      config: state.config,
    });
  });

  app.post('/api/downloads/worker/start', requireAuthMiddleware, (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ running: true, stub: true });
    }
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    const intervalSeconds = coerceInt(req.body?.intervalSeconds, 60);
    const batchSize = coerceInt(req.body?.batchSize, 3);
    const state = startDownloadWorker(req.corpusId, { intervalSeconds, batchSize });
    return res.json({ running: state.running, config: state.config });
  });

  app.post('/api/downloads/worker/stop', requireAuthMiddleware, (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ running: false, stub: true });
    }
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    const force = Boolean(req.body?.force);
    const state = stopDownloadWorker(req.corpusId, { force });
    return res.json({ running: state.running });
  });

  app.post('/api/downloads/worker/run-once', requireAuthMiddleware, async (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ processed: 0, downloaded: 0, failed: 0, skipped: 0, source: 'stub' });
    }
    if (!hasWorkerAccess(req)) {
      return res.status(403).json({ error: 'Insufficient corpus role to manage workers' });
    }
    const batchSize = coerceInt(req.body?.batchSize, 3);
    const state = getOrInitWorkerState(req.corpusId);
    if (state.inFlight) {
      return res.status(409).json({ error: 'Worker is already processing a batch' });
    }
    state.inFlight = true;
    state.lastTickAt = new Date().toISOString();
    state.lastError = null;
    send(`[download-worker] run-once corpus=${req.corpusId || 'none'} batch=${batchSize}`);
    try {
      const { child, done } = spawnDownloadOnce({ corpusId: req.corpusId, batchSize });
      state.child = child;
      const payload = await done;
      state.lastResult = payload;
      send(
        `[download-worker] done processed=${payload.processed} downloaded=${payload.downloaded} failed=${payload.failed} skipped=${payload.skipped}`
      );
      return res.json(payload);
    } catch (error) {
      state.lastError = error?.message || String(error);
      send(`[download-worker ERROR] ${state.lastError}`);
      return res.status(500).json({ error: state.lastError });
    } finally {
      state.child = null;
      state.inFlight = false;
    }
  });

  return app;
}
