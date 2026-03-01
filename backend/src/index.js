import { WebSocketServer } from 'ws';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import util from 'node:util';
import { createApp } from './app.js';

const port = process.env.PORT || 4000;
const clients = new Set();
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

function broadcast(message) {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  appendLogLine(pipelineLogFile, formattedMessage);
  clients.forEach((client) => {
    if (client.readyState === client.OPEN) {
      client.send(formattedMessage);
    }
  });
}

function broadcastEvent(payload) {
  if (!payload || typeof payload !== 'object') return;
  const serialized = JSON.stringify(payload);
  clients.forEach((client) => {
    if (client.readyState === client.OPEN) {
      client.send(serialized);
    }
  });
}

const app = createApp({ broadcast, broadcastEvent });
const server = app.listen(port, () => {
  console.log(`HTTP server listening on port ${port}`);
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  console.log('Client connected via WebSocket');
  clients.add(ws);

  ws.on('message', (message) => {
    console.log('Received message from client:', message);
  });

  ws.on('close', () => {
    console.log('Client disconnected');
    clients.delete(ws);
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
    clients.delete(ws);
  });

});
