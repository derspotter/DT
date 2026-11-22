import os from 'os';
import path from 'path';
import { spawn } from 'child_process';

// One place that builds Python child environments, spawns scripts, forwards
// their output line by line and parses their JSON results, so every child sees
// the same PYTHONPATH and the Admin-tab settings.

// D12: the multiline fallback is the original search (every line's first
// opener, every later line as an end), capped by a cumulative work budget so
// a child printing thousands of tagged lines and no JSON cannot pin a core.
const FALLBACK_WORK_FACTOR = 4;
const FALLBACK_WORK_SLACK = 64 * 1024;

// `stats` (tests only): the fallback adds every character it scans or joins to
// `stats.visited`, its parse attempts to `stats.parses`, the characters it
// hands to JSON.parse to `stats.parsedChars`, and its cap to `stats.cap`.
// visited + parsedChars never exceeds the cap.
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

  // Fallback: recover pretty-printed/multiline JSON blocks from stdout, in the
  // original order and with the original results. Each candidate's length is
  // known from prefix sums before it is built, so the budget is checked first:
  // building and parsing a candidate both count, and so does every scan.
  const noJson = () => new Error(`No valid JSON output from python. Output was: ${output}`);
  const cap = FALLBACK_WORK_FACTOR * String(output || '').length + FALLBACK_WORK_SLACK;
  let visited = 0;
  let parses = 0;
  let parsedChars = 0;
  const report = () => {
    if (!stats) return;
    stats.visited += visited;
    stats.parses += parses;
    stats.parsedChars += parsedChars;
    stats.cap = cap;
  };
  // prefix[k]: characters of lines[0..k-1] joined with '\n' plus a trailing one.
  const prefix = new Array(lines.length + 1);
  prefix[0] = 0;
  for (let k = 0; k < lines.length; k += 1) prefix[k + 1] = prefix[k] + lines[k].length + 1;
  visited += lines.length;
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i];
    const bracketIndex = line.search(/[\[{]/);
    visited += bracketIndex < 0 ? line.length : bracketIndex + 1;
    if (bracketIndex < 0) continue;
    for (let j = lines.length; j > i; j -= 1) {
      const length = prefix[j] - prefix[i] - 1 - bracketIndex;
      if (visited + parsedChars + 2 * length > cap) {
        report();
        throw noJson();
      }
      const segment = lines.slice(i, j);
      segment[0] = segment[0].slice(bracketIndex);
      const candidate = segment.join('\n');
      visited += length;
      parses += 1;
      parsedChars += length;
      const parsedSegment = tryParse(candidate);
      if (parsedSegment !== null) {
        report();
        return parsedSegment;
      }
    }
  }

  report();
  throw noJson();
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
