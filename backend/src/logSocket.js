import { WebSocketServer } from 'ws';
import jwt from 'jsonwebtoken';
import { getUserById, isAdminUser } from './app.js';

// Same tag the log tail filter reads (extractLogCorpusTag in app.js and the
// frontend's isLogLineForCurrentCorpus). Only numeric ids name a corpus;
// `corpus=none` or any other value is admin-only.
const CORPUS_TAG_RE = /\bcorpus=([a-zA-Z0-9_-]+)/;

function lineCorpusId(line) {
  const match = String(line || '').match(CORPUS_TAG_RE);
  return match && /^\d+$/.test(match[1]) ? Number(match[1]) : null;
}

function eventCorpusId(payload) {
  const id = payload?.corpus_id;
  return typeof id === 'number' && Number.isFinite(id) ? id : null;
}

function tokenFromRequest(req) {
  return String(req.headers['sec-websocket-protocol'] || '')
    .split(',')
    .map((entry) => entry.trim())
    .find((entry) => entry && entry !== 'bearer') || null;
}

export function attachLogSocket(
  server,
  {
    authDb,
    authConfig,
    path = '/api/ws',
    isStub = () => process.env.RAG_FEEDER_STUB === '1',
    appendLogLine = () => {},
    membershipTtlMs = 5000,
  } = {}
) {
  const clients = new Set();
  const corpusStmt = authDb
    ? authDb.prepare('SELECT corpus_id FROM user_corpora WHERE user_id = ?')
    : null;

  // Re-read the user's admin flag and memberships. A user that no longer
  // exists (or any lookup error) leaves the record with nothing visible.
  function loadAccess(record) {
    record.loadedAt = Date.now();
    try {
      const user = getUserById(authDb, record.userId);
      record.isAdmin = Boolean(user) && isAdminUser(user, authConfig);
      record.corpusIds = user
        ? new Set(corpusStmt.all(record.userId).map((row) => Number(row.corpus_id)))
        : new Set();
      return Boolean(user);
    } catch (error) {
      console.warn('[log-socket] Access lookup failed:', error?.message || error);
      record.isAdmin = false;
      record.corpusIds = new Set();
      return false;
    }
  }

  function canReceive(record, corpusId) {
    if (record.stub) return true;
    if (Date.now() - record.loadedAt > membershipTtlMs) loadAccess(record);
    if (record.isAdmin) return true;
    return corpusId !== null && record.corpusIds.has(corpusId);
  }

  function deliver(text, corpusId) {
    clients.forEach((record) => {
      if (record.ws.readyState !== record.ws.OPEN) return;
      if (canReceive(record, corpusId)) record.ws.send(text);
    });
  }

  const wss = new WebSocketServer({
    server,
    path,
    // Always complete the upgrade (the deploy probe expects a bare 101);
    // auth failures are reported with close code 4401 afterwards.
    handleProtocols: (protocols) => (protocols.has('bearer') ? 'bearer' : false),
  });

  wss.on('connection', (ws, req) => {
    let record;
    if (isStub()) {
      record = { ws, userId: 0, isAdmin: true, corpusIds: new Set(), loadedAt: Date.now(), stub: true };
    } else {
      const token = tokenFromRequest(req);
      let userId = null;
      try {
        userId = token ? jwt.verify(token, authConfig.jwtSecret)?.sub : null;
      } catch {
        userId = null;
      }
      record = { ws, userId, isAdmin: false, corpusIds: new Set(), loadedAt: 0 };
      if (userId === null || userId === undefined || !loadAccess(record)) {
        ws.close(4401, 'unauthorized');
        return;
      }
    }
    clients.add(record);
    ws.on('close', () => clients.delete(record));
    ws.on('error', (error) => {
      console.error('WebSocket error:', error);
      clients.delete(record);
    });
  });

  return {
    clients,
    broadcast(message) {
      const formatted = `[${new Date().toISOString()}] ${message}`;
      appendLogLine(formatted);
      deliver(formatted, lineCorpusId(message));
    },
    broadcastEvent(payload) {
      if (!payload || typeof payload !== 'object') return;
      deliver(JSON.stringify(payload), eventCorpusId(payload));
    },
    close() {
      clients.forEach((record) => record.ws.terminate());
      clients.clear();
      wss.close();
    },
  };
}
