import express from 'express';
import cors from 'cors';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import crypto from 'crypto';
import { spawn } from 'child_process';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// --- Define Container Paths ---
const UPLOADS_DIR = '/usr/src/app/uploads'; // Reverted path
const DL_LIT_DIR = '/usr/src/app/dl_lit'; // Mounted from ../dl_lit
const TEMP_PAGES_BASE_DIR = path.join(DL_LIT_DIR, 'temp_pages');
const BIB_OUTPUT_DIR = path.join(DL_LIT_DIR, 'bibliographies');
const GET_BIB_PAGES_SCRIPT = path.join(DL_LIT_DIR, 'get_bib_pages.py');
const API_SCRAPER_SCRIPT = path.join(DL_LIT_DIR, 'APIscraper_v2.py'); // Updated script name

const DL_LIT_PROJECT_DIR = path.join(__dirname, '..', '..', 'dl_lit_project');
const DEFAULT_DB_PATH = path.join(DL_LIT_PROJECT_DIR, 'data', 'literature.db');
const PYTHON_SCRIPTS_DIR = path.join(__dirname, '..', 'scripts');
const KEYWORD_SEARCH_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'keyword_search.py');
const CORPUS_LIST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'corpus_list.py');
const DOWNLOADS_LIST_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'downloads_list.py');
const GRAPH_EXPORT_SCRIPT = path.join(PYTHON_SCRIPTS_DIR, 'graph_export.py');
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

function runPythonJson(scriptPath, args, { dbPath } = {}) {
  return new Promise((resolve, reject) => {
    const env = {
      ...process.env,
      PYTHONPATH: DL_LIT_PROJECT_DIR,
      RAG_FEEDER_DB_PATH: dbPath || process.env.RAG_FEEDER_DB_PATH || DEFAULT_DB_PATH,
    };

    const child = spawn(PYTHON_EXEC, [scriptPath, ...args], { env });
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

  // Ensure temporary and output directories exist
  fs.mkdirSync(TEMP_PAGES_BASE_DIR, { recursive: true });
  fs.mkdirSync(BIB_OUTPUT_DIR, { recursive: true });

  // Ensure uploads directory exists (already present, keeping for clarity)
  const uploadDir = UPLOADS_DIR;
  if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir, { recursive: true });
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

  // Dynamic route to fetch bibliography JSON by basename
  app.get('/api/bibliographies/:baseName/data', (req, res) => {
    const base = req.params.baseName;
    function findFile(dir) {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const ent of entries) {
        if (ent.isFile() && ent.name === `${base}_bibliography.json`) {
          return path.join(dir, ent.name);
        }
        if (ent.isDirectory()) {
          const f = findFile(path.join(dir, ent.name));
          if (f) return f;
        }
      }
      return null;
    }
    const filePath = findFile(BIB_OUTPUT_DIR);
    if (!filePath) {
      return res.status(404).json({ error: 'Bibliography not found yet' });
    }
    res.sendFile(filePath);
  });

  app.use('/api/bibliographies', express.static(BIB_OUTPUT_DIR));

  // GET combined bibliography entries for a given baseName
  app.get('/api/bibliographies/:baseName/all', (req, res) => {
    const base = req.params.baseName;
    const combined = [];
    function walk(dir) {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const ent of entries) {
        const abs = path.join(dir, ent.name);
        if (ent.isDirectory()) {
          walk(abs);
        } else if (ent.isFile() && ent.name.startsWith(base) && ent.name.endsWith('_bibliography.json')) {
          try {
            const json = JSON.parse(fs.readFileSync(abs, 'utf8'));
            if (Array.isArray(json)) combined.push(...json);
          } catch (e) {
            console.error(`Error reading/parsing ${abs}:`, e);
          }
        }
      }
    }
    walk(BIB_OUTPUT_DIR);
    res.json(combined);
  });

  // --- NEW: GET ALL current bibliography entries ---
  app.get('/api/bibliographies/all-current', (req, res) => {
    console.log('[/api/bibliographies/all-current] Received request for file list.');
    const fileList = [];
    function walk(dir, relativePath = '') {
      try {
        // Check if directory exists before reading
        if (!fs.existsSync(dir)) {
          console.warn(`[/api/bibliographies/all-current] Directory not found, skipping: ${dir}`);
          return;
        }
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const ent of entries) {
          const abs = path.join(dir, ent.name);
          const rel = path.join(relativePath, ent.name);
          if (ent.isDirectory()) {
            walk(abs, rel);
          } else if (ent.isFile() && ent.name.endsWith('_bibliography.json')) {
            console.log(`[/api/bibliographies/all-current] Found file: ${rel}`);
            fileList.push(rel); // Add relative path to the list
          }
        }
      } catch (readDirErr) {
        console.error(`[/api/bibliographies/all-current] Error reading directory ${dir}:`, readDirErr);
      }
    }

    // Start walking from the base bibliography output directory
    walk(BIB_OUTPUT_DIR);

    console.log(`[/api/bibliographies/all-current] Returning ${fileList.length} bibliography filenames.`);
    // Set content type and send JSON response
    res.setHeader('Content-Type', 'application/json');
    res.status(200).json(fileList); // Return the list of filenames
  });

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
  app.post('/api/process_pdf', upload.single('file'), (req, res) => { // Made non-async
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
  app.post('/api/extract-bibliography/:filename', (req, res) => {
    const { filename } = req.params;
    const inputPdfPath = path.join(UPLOADS_DIR, filename);
    const uniqueSuffix = crypto.randomBytes(8).toString('hex');
    const tempPagesDir = path.join(TEMP_PAGES_BASE_DIR, `${Date.now()}-${uniqueSuffix}`);

    console.log(`[/api/extract-bibliography] Received request for: ${filename}`);
    console.log(`[/api/extract-bibliography] Input PDF path: ${inputPdfPath}`);
    console.log(`[/api/extract-bibliography] Temp pages dir: ${tempPagesDir}`);
    console.log(`[/api/extract-bibliography] Final bib output dir: ${BIB_OUTPUT_DIR}`);

    // 1. Check if input PDF exists
    if (!fs.existsSync(inputPdfPath)) {
      console.error(`[/api/extract-bibliography] Input PDF not found: ${inputPdfPath}`);
      // Return error immediately
      return res.status(404).json({ error: 'Uploaded PDF not found.' });
    }

    // --- Send Immediate Response ---
    res.status(202).json({ message: 'Bibliography extraction process started.' });
    // Status 202 Accepted indicates the request is accepted for processing, but is not complete.
    // --- End Immediate Response ---

    // --- Run Scripts in Background ---
    try {
      // 2. Create unique temporary directory for pages
      fs.mkdirSync(tempPagesDir, { recursive: true });
      console.log(`[/api/extract-bibliography] Created temp directory: ${tempPagesDir}`);

      // 3. Run get_bib_pages.py in background
      const getPagesArgs = [
        '--input-pdf', inputPdfPath,
        '--output-dir', BIB_OUTPUT_DIR // Keep this, it seems to place output in BIB_OUTPUT_DIR/baseName/
      ];

      console.log(`[/api/extract-bibliography] Spawning: python ${GET_BIB_PAGES_SCRIPT} ${getPagesArgs.join(' ')}`);
      const getPagesProcess = spawn('python', [GET_BIB_PAGES_SCRIPT, ...getPagesArgs]);

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
          // Clean up temporary directory on error
          fs.rm(tempPagesDir, { recursive: true, force: true }, (rmErr) => {
            if (rmErr) console.error(`[/api/extract-bibliography] Error cleaning up temp dir ${tempPagesDir}:`, rmErr);
          });
          // Send error response only if headers not already sent
          if (!res.headersSent) {
            return res.status(500).json({ error: 'Error during bibliography page extraction.', scriptCode: code });
          }
          return; // Stop further processing
        }

        // **Proceed to Step 2: Call API Scraper only if Step 1 succeeded**
        const baseName = path.basename(filename, path.extname(filename)); // Extract base name from original filename
        // Construct the expected JSON path based on the *output* structure of get_bib_pages
        // It should be in BIB_OUTPUT_DIR/<baseName>/<baseName>_bibliography_pages.json
        const jsonSubDir = path.join(BIB_OUTPUT_DIR, baseName); // Subdirectory named after the base PDF name
        const jsonInputPath = path.join(jsonSubDir, `${baseName}_bibliography_pages.json`);

        console.log(`[/api/extract-bibliography] Looking for input JSON for API scraper: ${jsonInputPath}`);

        // Check if the expected JSON file exists
        if (!fs.existsSync(jsonInputPath)) {
          const errorMessage = `[/api/extract-bibliography] Input JSON for API scraper not found: ${jsonInputPath}`;
          console.error(errorMessage);
          send(errorMessage); // Broadcast error
          // Clean up temporary directory as processing cannot continue
          fs.rm(tempPagesDir, { recursive: true, force: true }, (rmErr) => {
            if (rmErr) console.error(`[/api/extract-bibliography] Error cleaning up temp dir ${tempPagesDir}:`, rmErr);
          });
          if (!res.headersSent) {
            return res.status(500).json({ error: 'Intermediate JSON file not found after page extraction.' });
          }
          return;
        }

        console.log(`[/api/extract-bibliography] Spawning: python ${API_SCRAPER_SCRIPT} ...`);
        // Correct arguments based on APIscraper_v2.py's argparse
        const scrapeApiArgs = [
          '--input-dir', jsonSubDir,       // Directory containing the JSON from get_bib_pages.py
          '--base-output-dir', BIB_OUTPUT_DIR // The main output directory
        ];
        const apiScraperProcess = spawn('python', [API_SCRAPER_SCRIPT, ...scrapeApiArgs]);

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

          // Consider cleanup here *after* everything is done
          if (fs.existsSync(tempPagesDir)) {
            fs.rm(tempPagesDir, { recursive: true, force: true }, (err) => {
              if (err) {
                console.error(`[/api/extract-bibliography] Error deleting temp directory ${tempPagesDir} after completion:`, err);
              } else {
                console.log(`[/api/extract-bibliography] Deleted temp directory ${tempPagesDir} after completion.`);
              }
            });
          }
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

  // --- NEW: POST endpoint to trigger consolidation ---
  app.post('/api/bibliographies/consolidate', (req, res) => {
    console.log('[/api/bibliographies/consolidate] Received request.');

    const pythonScriptPath = path.join(__dirname, '..', 'dl_lit', 'consolidate_bibs.py');
    const inputDir = BIB_OUTPUT_DIR;
    const outputFile = path.join(inputDir, 'master_bibliography.json');

    console.log(`[/api/bibliographies/consolidate] Attempting to execute python3 with script: ${pythonScriptPath}`);

    // Execute python3 - PATH in Dockerfile ensures /opt/venv/bin/python3 is used
    const pythonProcess = spawn('python3', [
      pythonScriptPath,
      inputDir,
      outputFile
    ], { stdio: 'pipe' });

    let stdoutData = '';
    let stderrData = '';

    pythonProcess.stdout.on('data', (data) => {
      stdoutData += data.toString();
      console.log(`[/api/bibliographies/consolidate] Script stdout: ${data}`);
      send(`[/api/bibliographies/consolidate] ${data}`); // Broadcast stdout
    });

    pythonProcess.stderr.on('data', (data) => {
      stderrData += data.toString();
      console.error(`[/api/bibliographies/consolidate] Script stderr: ${data}`);
      send(`[/api/bibliographies/consolidate ERROR] ${data}`); // Broadcast stderr
    });

    pythonProcess.on('close', (code) => {
      console.log(`[/api/bibliographies/consolidate] Script exited with code ${code}`);
      if (code === 0) {
        res.status(200).json({ message: 'Consolidation successful.', output: stdoutData });
      } else {
        res.status(500).json({
          message: 'Consolidation failed.',
          error: stderrData || 'Unknown error from script.',
          stdout: stdoutData
        });
      }
    });

    pythonProcess.on('error', (err) => {
      console.error('[/api/bibliographies/consolidate] Failed to start subprocess.', err);
      res.status(500).json({ message: 'Failed to start consolidation process.', error: err.message });
    });
  });

  app.post('/api/keyword-search', async (req, res) => {
    const query = req.body?.query?.trim();
    if (!query) {
      return res.status(400).json({ error: 'query is required' });
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
    const dbPath = req.body?.dbPath || process.env.RAG_FEEDER_DB_PATH || DEFAULT_DB_PATH;

    const args = ['--query', query, '--db-path', dbPath, '--max-results', String(maxResults), '--field', String(field)];
    if (yearFrom) args.push('--year-from', String(yearFrom));
    if (yearTo) args.push('--year-to', String(yearTo));
    if (mailto) args.push('--mailto', String(mailto));
    if (enqueue) args.push('--enqueue');

    try {
      const payload = await runPythonJson(KEYWORD_SEARCH_SCRIPT, args, { dbPath });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/keyword-search] Error:', error);
      return res.status(500).json({ error: error.message || 'Keyword search failed' });
    }
  });

  app.get('/api/corpus', async (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ items: STUB_RESULTS.corpus, total: STUB_RESULTS.corpus.length, source: 'stub' });
    }
    const limit = coerceInt(req.query?.limit, 200);
    const offset = coerceInt(req.query?.offset, 0);
    const dbPath = req.query?.dbPath || process.env.RAG_FEEDER_DB_PATH || DEFAULT_DB_PATH;

    const args = ['--db-path', dbPath, '--limit', String(limit), '--offset', String(offset)];
    try {
      const payload = await runPythonJson(CORPUS_LIST_SCRIPT, args, { dbPath });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/corpus] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to fetch corpus' });
    }
  });

  app.get('/api/downloads', async (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ items: STUB_RESULTS.downloads, total: STUB_RESULTS.downloads.length, source: 'stub' });
    }
    const limit = coerceInt(req.query?.limit, 200);
    const offset = coerceInt(req.query?.offset, 0);
    const dbPath = req.query?.dbPath || process.env.RAG_FEEDER_DB_PATH || DEFAULT_DB_PATH;

    const args = ['--db-path', dbPath, '--limit', String(limit), '--offset', String(offset)];
    try {
      const payload = await runPythonJson(DOWNLOADS_LIST_SCRIPT, args, { dbPath });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/downloads] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to fetch download queue' });
    }
  });

  app.get('/api/graph', async (req, res) => {
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ ...STUB_RESULTS.graph, source: 'stub' });
    }
    const maxNodes = coerceInt(req.query?.max_nodes || req.query?.maxNodes, 200);
    const relationship = req.query?.relationship || 'both';
    const status = req.query?.status || 'all';
    const yearFrom = coerceInt(req.query?.year_from || req.query?.yearFrom, null);
    const yearTo = coerceInt(req.query?.year_to || req.query?.yearTo, null);
    const hideIsolates = req.query?.hide_isolates !== '0' && req.query?.hideIsolates !== '0';
    const dbPath = req.query?.dbPath || process.env.RAG_FEEDER_DB_PATH || DEFAULT_DB_PATH;

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
      const payload = await runPythonJson(GRAPH_EXPORT_SCRIPT, args, { dbPath });
      return res.json(payload);
    } catch (error) {
      console.error('[/api/graph] Error:', error);
      return res.status(500).json({ error: error.message || 'Failed to build graph' });
    }
  });

  return app;
}
