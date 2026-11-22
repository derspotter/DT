import os from 'os';
import path from 'path';
import { spawn } from 'child_process';

// One place that builds Python child environments, spawns scripts, forwards
// their output line by line and parses their JSON results, so every child sees
// the same PYTHONPATH and the Admin-tab settings.

// D12: the fallback's parse input is at most this many times the text.
const FALLBACK_PARSE_BUDGET_FACTOR = 4;

// One structural scan over `text`: opener index -> matching close index, and
// the openers that opened with an empty stack (outer blocks). Inside an open
// block JSON strings are skipped (honouring \" and \\); a string ends at the
// next newline at the latest. Outside any block only openers count, so a quote
// in diagnostic text cannot start a string. A close that does not match the
// top of the stack clears the stack. An opener that never closes has no entry.
function bracketCloses(text, stats = null) {
  const closes = new Map();
  const outer = new Set();
  const stack = [];
  let inString = false;
  let escaped = false;
  let visited = 0;
  for (let i = 0; i < text.length; i += 1) {
    visited += 1;
    const ch = text[i];
    if (ch === '\n') {
      inString = false;
      escaped = false;
      continue;
    }
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '[' || ch === '{') {
      if (stack.length === 0) outer.add(i);
      stack.push(i);
    } else if (ch === ']' || ch === '}') {
      const top = stack.length ? stack[stack.length - 1] : -1;
      if (top >= 0 && text[top] === (ch === ']' ? '[' : '{')) {
        stack.pop();
        closes.set(top, i);
      } else {
        stack.length = 0;
      }
    } else if (ch === '"' && stack.length > 0) {
      inString = true;
    }
  }
  if (stats) stats.visited += visited;
  return { closes, outer };
}

export function parseJsonFromOutput(output, { stats } = {}) {
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

  // Fallback: recover pretty-printed/multiline JSON blocks. One scan pairs the
  // brackets; then, from the last line to the first, a line's first opener
  // whose block closes on a later line is tried once, provided the close ends
  // its line or the block is outer. The parsed input is capped by a budget.
  const text = lines.join('\n');
  const { closes, outer } = bracketCloses(text, stats);
  const lineStarts = new Array(lines.length);
  for (let i = 0, offset = 0; i < lines.length; i += 1) {
    lineStarts[i] = offset;
    offset += lines[i].length + 1;
  }
  let budget = FALLBACK_PARSE_BUDGET_FACTOR * text.length;
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const bracketIndex = lines[i].search(/[\[{]/);
    if (bracketIndex < 0) continue;
    const start = lineStarts[i] + bracketIndex;
    const close = closes.get(start);
    const lineEnd = lineStarts[i] + lines[i].length;
    if (close === undefined || close < lineEnd) continue; // unclosed or single-line
    const closeLineEnd = text.indexOf('\n', close);
    const endsLine = close + 1 === (closeLineEnd < 0 ? text.length : closeLineEnd);
    if (!endsLine && !outer.has(start)) continue;
    budget -= close + 1 - start;
    if (budget < 0) break;
    if (stats) {
      stats.parses += 1;
      stats.parsedChars += close + 1 - start;
    }
    const parsedBlock = tryParse(text.slice(start, close + 1));
    if (parsedBlock !== null) return parsedBlock;
  }

  throw new Error(`No valid JSON output from python. Output was: ${output}`);
}

// Split buffered output into trimmed, non-empty complete lines for `handler`
// and return the unfinished tail; with `flushPartial` the tail is forwarded too.
function flushLines(buffer, handler, flushPartial = false) {
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
}

export function createPythonRunner({ pythonExec, dlLitProjectDir, repoRoot, dbPath: defaultDbPath, appSettingsEnv }) {
  const settingsEnv = typeof appSettingsEnv === 'function' ? appSettingsEnv : () => ({});

  // Admin-managed settings win over the process environment, so changing a key
  // or model in the UI takes effect on the next script run (spec 13/14);
  // `extra` wins over both (e.g. per-call model overrides).
  function pythonEnv(extra = {}) {
    return {
      ...process.env,
      ...settingsEnv(),
      PYTHONPATH: [dlLitProjectDir, repoRoot, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
      RAG_FEEDER_DL_LIT_PROJECT_DIR: dlLitProjectDir,
      RAG_FEEDER_DB_PATH: defaultDbPath,
      ...extra,
    };
  }

  function spawnPython(scriptPath, args = [], {
    env,
    dbPath,
    corpusId,
    onStdoutLine,
    onStderrLine,
    lowPriority,
    label,
  } = {}) {
    const childEnv = pythonEnv({
      ...(dbPath ? { RAG_FEEDER_DB_PATH: dbPath } : {}),
      ...(env || {}),
    });

    const finalArgs = [...args];
    // A caller that already passes --corpus-id keeps its own; never add a second.
    if (corpusId !== undefined && corpusId !== null && !finalArgs.includes('--corpus-id')) {
      finalArgs.push('--corpus-id', String(corpusId));
    }

    const child = spawn(pythonExec, [scriptPath, ...finalArgs], { env: childEnv });
    if (lowPriority && child.pid) {
      // Background jobs (e.g. the ~60s graph layout rebuild) shouldn't starve
      // the API: drop their CPU priority so interactive requests stay fast
      // while they run. Best-effort — ignore if the platform disallows it.
      try { os.setPriority(child.pid, 15); } catch { /* noop */ }
    }
    let stdout = '';
    let stderr = '';
    let stdoutBuf = '';
    let stderrBuf = '';

    // Decode with a stream decoder so a multi-byte character split across two
    // chunks is not turned into U+FFFD.
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');

    child.stdout.on('data', (text) => {
      stdout += text;
      stdoutBuf += text;
      stdoutBuf = flushLines(stdoutBuf, onStdoutLine);
    });

    child.stderr.on('data', (text) => {
      stderr += text;
      stderrBuf += text;
      stderrBuf = flushLines(stderrBuf, onStderrLine);
    });

    const done = new Promise((resolve, reject) => {
      let settled = false;
      const fail = (error) => {
        if (settled) return;
        settled = true;
        reject(error);
      };
      child.on('error', (error) => {
        // e.g. ENOENT for a missing interpreter; 'close' may not follow.
        fail(error);
      });
      child.on('close', (code) => {
        stdoutBuf = flushLines(stdoutBuf, onStdoutLine, true);
        stderrBuf = flushLines(stderrBuf, onStderrLine, true);
        if (code !== 0) {
          const error = new Error(`Python script failed (${code}): ${stderr || stdout}`);
          error.exitCode = code;
          if (label) error.label = label;
          error.stdout = stdout;
          error.stderr = stderr;
          fail(error);
          return;
        }
        if (settled) return;
        settled = true;
        resolve({ stdout, stderr, code });
      });
    });

    return { child, done };
  }

  async function runPythonJson(scriptPath, args, { dbPath, corpusId, lowPriority } = {}) {
    const { stdout } = await spawnPython(scriptPath, args, { dbPath, corpusId, lowPriority }).done;
    return parseJsonFromOutput(stdout);
  }

  return { pythonEnv, spawnPython, runPythonJson, parseJsonFromOutput };
}
