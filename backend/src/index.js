import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import util from 'node:util';
import { createApp } from './app.js';
import { attachLogSocket } from './logSocket.js';

const port = process.env.PORT || 4000;
const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const logDir = process.env.RAG_FEEDER_LOG_DIR || path.join(repoRoot, 'logs');
const appLogFile = path.join(logDir, 'backend-app.log');
const pipelineLogFile = path.join(logDir, 'backend-pipeline.log');
const maxLogBytes = Number(process.env.RAG_FEEDER_LOG_MAX_BYTES || 10 * 1024 * 1024);
const maxLogBackups = Number(process.env.RAG_FEEDER_LOG_MAX_FILES || 5);

fs.mkdirSync(logDir, { recursive: true });

function stringifyArg(arg) {
  if (arg instanceof Error) return arg.stack || arg.message;
  if (typeof arg === 'string') return arg;
  return util.inspect(arg, { depth: 4, breakLength: 120, colors: false });
}

function rotateIfNeeded(logFile) {
  try {
    if (!fs.existsSync(logFile)) return;
    const { size } = fs.statSync(logFile);
    if (!Number.isFinite(size) || size < maxLogBytes) return;

    for (let i = maxLogBackups - 1; i >= 1; i -= 1) {
      const src = `${logFile}.${i}`;
      const dest = `${logFile}.${i + 1}`;
      if (fs.existsSync(src)) {
        fs.renameSync(src, dest);
      }
    }
    fs.renameSync(logFile, `${logFile}.1`);
  } catch (error) {
    process.stderr.write(`[log-rotate-error] ${String(error)}\n`);
  }
}

function appendLogLine(logFile, line) {
  try {
    rotateIfNeeded(logFile);
    fs.appendFileSync(logFile, `${line}\n`, 'utf8');
  } catch (error) {
    process.stderr.write(`[log-write-error] ${String(error)}\n`);
  }
}

function logWithTimestamp(logFile, level, message) {
  const ts = new Date().toISOString();
  appendLogLine(logFile, `[${ts}] [${level}] ${message}`);
}

const baseConsole = {
  log: console.log.bind(console),
  info: console.info.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
  debug: console.debug ? console.debug.bind(console) : console.log.bind(console),
};

function patchConsoleForFileLogs() {
  const bind = (consoleFn, level) => (...args) => {
    consoleFn(...args);
    const message = args.map(stringifyArg).join(' ');
    logWithTimestamp(appLogFile, level, message);
  };
  console.log = bind(baseConsole.log, 'INFO');
  console.info = bind(baseConsole.info, 'INFO');
  console.warn = bind(baseConsole.warn, 'WARN');
  console.error = bind(baseConsole.error, 'ERROR');
  console.debug = bind(baseConsole.debug, 'DEBUG');
}

patchConsoleForFileLogs();
logWithTimestamp(appLogFile, 'INFO', `Persistent logging enabled: ${appLogFile}`);
logWithTimestamp(pipelineLogFile, 'INFO', `Pipeline logging enabled: ${pipelineLogFile}`);

// The log socket needs the HTTP server and app.authDb, both of which exist
// only after createApp(); these forwarders are handed to createApp first and
// delegate once attachLogSocket has run (right after listen(), before any
// request can arrive). A line broadcast before that is still written to file.
let logSocket = null;

function broadcast(message) {
  if (logSocket) {
    logSocket.broadcast(message);
    return;
  }
  appendLogLine(pipelineLogFile, `[${new Date().toISOString()}] ${message}`);
}

function broadcastEvent(payload) {
  logSocket?.broadcastEvent(payload);
}

const app = createApp({ broadcast, broadcastEvent });
let seedWarmupTimer;
const server = app.listen(port, () => {
  console.log(`HTTP server listening on port ${port}`);
  seedWarmupTimer = setTimeout(() => app.warmSeedState?.(), 250);
  // Pre-build the default 3D graph snapshot so the first Graph-tab open is
  // instant instead of waiting for a cold ~30s build.
  setTimeout(() => {
    app.warmGraph3dSnapshot?.().catch(() => {});
  }, 3000);
});
server.on('close', () => clearTimeout(seedWarmupTimer));

logSocket = attachLogSocket(server, {
  authDb: app.authDb,
  authConfig: app.authConfig,
  appendLogLine: (line) => appendLogLine(pipelineLogFile, line),
});
